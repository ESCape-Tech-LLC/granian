use futures::{SinkExt, StreamExt};
use std::{
    hash::{BuildHasher, RandomState},
    sync::{Arc, Mutex, atomic::Ordering},
    time::Duration,
};
use tokio::{
    sync::{Mutex as AsyncMutex, Notify, OwnedSemaphorePermit, Semaphore, mpsc, oneshot, watch},
    time::{self, Instant, MissedTickBehavior},
};
use tokio_tungstenite::tungstenite::{
    Error, Message,
    protocol::{
        WebSocketConfig,
        frame::{CloseFrame, coding::CloseCode},
    },
};

use crate::{
    metrics::ArcWorkerMetrics,
    runtime::{Runtime, RuntimeRef},
    ws::{WSRxStream, WSStream, WSTxStream},
};

const INCOMING_MESSAGES: usize = 64;
const INCOMING_BYTES: usize = 8 * 1024 * 1024;
const OUTGOING_MESSAGES: usize = 16;
const CONTROL_MESSAGES: usize = 8;

#[derive(Clone, Copy)]
pub(super) struct HeartbeatConfig {
    interval: Duration,
    pong_timeout: Duration,
    close_timeout: Duration,
}

impl HeartbeatConfig {
    pub(super) fn new(interval: f64, pong_timeout: f64, close_timeout: f64) -> Option<Self> {
        fn duration(seconds: f64) -> Option<Duration> {
            let value = Duration::try_from_secs_f64(seconds).ok()?;
            if value.is_zero() {
                return None;
            }
            Instant::now().checked_add(value)?;
            Some(value)
        }

        Some(Self {
            interval: duration(interval)?,
            pong_timeout: duration(pong_timeout)?,
            close_timeout: duration(close_timeout)?,
        })
    }

    pub(super) fn websocket_config(self) -> WebSocketConfig {
        WebSocketConfig::default()
            .max_message_size(Some(INCOMING_BYTES))
            .max_frame_size(Some(INCOMING_BYTES))
    }
}

#[derive(Clone)]
enum CloseRequest {
    Send(Option<CloseFrame>),
    Peer,
}

impl CloseRequest {
    fn with_reason(code: CloseCode, reason: &'static str) -> Self {
        Self::Send(Some(CloseFrame {
            code,
            reason: reason.into(),
        }))
    }
}

fn request_close(closing: &watch::Sender<Option<CloseRequest>>, request: CloseRequest) {
    closing.send_if_modified(|current| {
        if current.is_some() {
            return false;
        }
        *current = Some(request);
        true
    });
}

async fn wait_close(closing: &mut watch::Receiver<Option<CloseRequest>>) -> CloseRequest {
    closing
        .wait_for(Option::is_some)
        .await
        .ok()
        .and_then(|request| request.clone())
        .unwrap_or(CloseRequest::Send(None))
}

struct IncomingMessage {
    message: Message,
    _budget: OwnedSemaphorePermit,
}

struct OutgoingMessage {
    message: Message,
    sent: oneshot::Sender<()>,
}

struct PendingPing {
    payload: [u8; 16],
    deadline: Instant,
    received: oneshot::Sender<()>,
}

#[derive(Default)]
struct PongMatcher {
    pending: Mutex<Option<PendingPing>>,
    matched: Notify,
}

impl PongMatcher {
    fn awaiting_pong(&self) -> bool {
        self.pending.lock().unwrap().is_some()
    }

    fn match_pong(&self, payload: &[u8]) {
        let mut pending = self.pending.lock().unwrap();
        if pending
            .as_ref()
            .is_some_and(|ping| payload == ping.payload.as_slice() && Instant::now() <= ping.deadline)
        {
            let _ = pending.take().unwrap().received.send(());
            self.matched.notify_one();
        }
    }
}

enum WriteControl {
    Flush,
    HeartbeatPing {
        ping: PendingPing,
        sent: oneshot::Sender<()>,
    },
}

pub(super) struct HeartbeatTransport {
    incoming: AsyncMutex<mpsc::Receiver<IncomingMessage>>,
    outgoing: mpsc::Sender<OutgoingMessage>,
    closing: watch::Sender<Option<CloseRequest>>,
    disconnect: watch::Receiver<Option<Message>>,
}

impl HeartbeatTransport {
    pub(super) fn spawn(
        runtime: &RuntimeRef,
        stream: WSStream,
        config: HeartbeatConfig,
        worker_shutdown: Arc<Notify>,
    ) -> Self {
        let (incoming_tx, incoming_rx) = mpsc::channel(INCOMING_MESSAGES);
        let (outgoing_tx, outgoing_rx) = mpsc::channel(OUTGOING_MESSAGES);
        let (closing_tx, closing_rx) = watch::channel(None);
        let (disconnect_tx, disconnect_rx) = watch::channel(None);

        runtime.spawn(run_transport(
            stream,
            config,
            incoming_tx,
            outgoing_rx,
            closing_tx.clone(),
            closing_rx,
            disconnect_tx,
            worker_shutdown,
            runtime.metrics(),
        ));

        Self {
            incoming: AsyncMutex::new(incoming_rx),
            outgoing: outgoing_tx,
            closing: closing_tx,
            disconnect: disconnect_rx,
        }
    }

    async fn disconnected(&self) -> Message {
        let mut disconnect = self.disconnect.clone();
        match disconnect.wait_for(Option::is_some).await {
            Ok(frame) => frame.clone().unwrap_or(Message::Close(None)),
            Err(_) => Message::Close(None),
        }
    }

    pub(super) async fn receive(&self) -> Message {
        let mut incoming = self.incoming.lock().await;
        let mut closing = self.closing.subscribe();

        tokio::select! {
            biased;
            _ = wait_close(&mut closing) => self.disconnected().await,
            message = incoming.recv() => match message {
                Some(message) => message.message,
                None => self.disconnected().await,
            }
        }
    }

    pub(super) async fn send(&self, message: Message) {
        let (sent, completed) = oneshot::channel();
        let mut closing = self.closing.subscribe();

        tokio::select! {
            biased;
            _ = wait_close(&mut closing) => {}
            () = async {
                if self.outgoing.send(OutgoingMessage { message, sent }).await.is_ok() {
                    let _ = completed.await;
                }
            } => {}
        }
    }

    pub(super) fn request_close(&self, frame: Option<CloseFrame>) {
        request_close(&self.closing, CloseRequest::Send(frame));
    }

    pub(super) async fn close(&self, frame: Option<CloseFrame>) {
        self.request_close(frame);
        let _ = self.disconnected().await;
    }
}

impl Drop for HeartbeatTransport {
    fn drop(&mut self) {
        self.request_close(None);
    }
}

struct ActiveHeartbeat(ArcWorkerMetrics);

impl ActiveHeartbeat {
    fn new(metrics: ArcWorkerMetrics) -> Self {
        metrics.ws_heartbeat_active.fetch_add(1, Ordering::Relaxed);
        Self(metrics)
    }
}

impl Drop for ActiveHeartbeat {
    fn drop(&mut self) {
        self.0.ws_heartbeat_active.fetch_sub(1, Ordering::Relaxed);
    }
}

async fn run_transport(
    stream: WSStream,
    config: HeartbeatConfig,
    incoming: mpsc::Sender<IncomingMessage>,
    outgoing: mpsc::Receiver<OutgoingMessage>,
    closing_tx: watch::Sender<Option<CloseRequest>>,
    closing_rx: watch::Receiver<Option<CloseRequest>>,
    disconnect: watch::Sender<Option<Message>>,
    worker_shutdown: Arc<Notify>,
    metrics: Option<ArcWorkerMetrics>,
) {
    let active = metrics.map(ActiveHeartbeat::new);
    let peer_close = Mutex::new(None);
    {
        let (tx, rx) = stream.split();
        let (control_tx, control_rx) = mpsc::channel(CONTROL_MESSAGES);
        let matcher = PongMatcher::default();
        let reader = read_frames(rx, incoming, control_tx.clone(), &matcher, &peer_close, &closing_rx);
        let writer = write_frames(tx, outgoing, control_rx, &matcher, closing_rx.clone());
        let watchdog = watch_pong(config, control_tx);
        let mut close_requested = closing_rx.clone();

        tokio::pin!(reader, writer, watchdog);
        let mut reader_finished = false;
        let mut writer_finished = false;
        let request = tokio::select! {
            request = wait_close(&mut close_requested) => request,
            request = &mut reader => {
                reader_finished = true;
                request
            }
            request = &mut writer => {
                writer_finished = true;
                request
            }
            request = &mut watchdog => request,
            () = worker_shutdown.notified() => CloseRequest::with_reason(CloseCode::Away, "Worker shutdown"),
        };

        request_close(&closing_tx, request);
        let _ = time::timeout(config.close_timeout, async {
            tokio::join!(
                async {
                    if !reader_finished {
                        let _ = reader.await;
                    }
                },
                async {
                    if !writer_finished {
                        let _ = writer.await;
                    }
                }
            );
        })
        .await;
        // Both halves are owned by these futures. Dropping them at the
        // deadline releases the socket even if a write still can't finish.
    }
    drop(active);
    disconnect.send_replace(Some(Message::Close(peer_close.into_inner().unwrap())));
}

async fn read_frames(
    mut rx: WSRxStream,
    incoming: mpsc::Sender<IncomingMessage>,
    control: mpsc::Sender<WriteControl>,
    matcher: &PongMatcher,
    peer_close: &Mutex<Option<CloseFrame>>,
    closing: &watch::Receiver<Option<CloseRequest>>,
) -> CloseRequest {
    let budget = Arc::new(Semaphore::new(INCOMING_BYTES));
    loop {
        let message = match rx.next().await {
            Some(Ok(message)) => message,
            Some(Err(Error::Capacity(_))) => {
                return CloseRequest::with_reason(CloseCode::Size, "WebSocket message exceeds the receive limit");
            }
            _ => return CloseRequest::Send(None),
        };
        match message {
            Message::Close(frame) => {
                *peer_close.lock().unwrap() = frame;
                return CloseRequest::Peer;
            }
            Message::Ping(_) => {
                // Tungstenite queues the Pong with the peer's payload.
                if !closing.borrow().is_some() && control.try_send(WriteControl::Flush).is_err() {
                    return CloseRequest::with_reason(CloseCode::Again, "WebSocket control queue full");
                }
            }
            Message::Pong(payload) => matcher.match_pong(&payload),
            message @ (Message::Text(_) | Message::Binary(_)) if closing.borrow().is_none() => {
                let size = message.len();
                let Ok(permit) = budget.clone().try_acquire_many_owned(size as u32) else {
                    return CloseRequest::with_reason(CloseCode::Again, "WebSocket receive queue full");
                };
                if incoming
                    .try_send(IncomingMessage {
                        message,
                        _budget: permit,
                    })
                    .is_err()
                {
                    return CloseRequest::with_reason(CloseCode::Again, "WebSocket receive queue full");
                }
            }
            Message::Text(_) | Message::Binary(_) | Message::Frame(_) => {}
        }
        tokio::task::consume_budget().await;
    }
}

async fn write_frames(
    mut tx: WSTxStream,
    mut outgoing: mpsc::Receiver<OutgoingMessage>,
    mut control: mpsc::Receiver<WriteControl>,
    matcher: &PongMatcher,
    mut closing: watch::Receiver<Option<CloseRequest>>,
) -> CloseRequest {
    let mut deferred_ping = None;
    loop {
        let deferred_ready = deferred_ping.is_some() && !matcher.awaiting_pong();
        let outgoing = tokio::select! {
            biased;
            request = wait_close(&mut closing) => {
                if let CloseRequest::Send(frame) = &request {
                    let _ = tx.send(Message::Close(frame.clone())).await;
                }
                // For peer close Tungstenite already queued a response.
                let _ = tx.close().await;
                return request;
            }
            () = std::future::ready(()), if deferred_ready => deferred_ping.take().unwrap(),
            Some(command) = control.recv() => {
                match command {
                    WriteControl::Flush => {
                        if tx.flush().await.is_err() {
                            return CloseRequest::Send(None);
                        }
                    }
                    WriteControl::HeartbeatPing { ping, sent } => {
                        if Instant::now() >= ping.deadline {
                            return pong_timeout();
                        }
                        let payload = bytes::Bytes::copy_from_slice(&ping.payload);
                        *matcher.pending.lock().unwrap() = Some(ping);
                        if tx.send(Message::Ping(payload)).await.is_err() {
                            return CloseRequest::Send(None);
                        }
                        let _ = sent.send(());
                    }
                }
                continue;
            }
            () = matcher.matched.notified(), if deferred_ping.is_some() => continue,
            Some(message) = outgoing.recv(), if deferred_ping.is_none() => message,
        };
        // A client may answer only the newest Ping. Let the native Ping
        // finish before sending a manual Ping that could replace its Pong.
        if matches!(outgoing.message, Message::Ping(_)) && matcher.awaiting_pong() {
            deferred_ping = Some(outgoing);
            continue;
        }
        if tx.send(outgoing.message).await.is_err() {
            return CloseRequest::Send(None);
        }
        let _ = outgoing.sent.send(());
    }
}

fn pong_timeout() -> CloseRequest {
    CloseRequest::with_reason(CloseCode::Error, "WebSocket Pong timeout")
}

async fn watch_pong(config: HeartbeatConfig, control: mpsc::Sender<WriteControl>) -> CloseRequest {
    let mut ticks = time::interval_at(Instant::now() + config.interval, config.interval);
    ticks.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let random = RandomState::new();
    let mut sequence = 0u64;

    loop {
        ticks.tick().await;
        sequence = sequence.wrapping_add(1);
        let mut payload = [0u8; 16];
        payload[..8].copy_from_slice(&sequence.to_be_bytes());
        payload[8..].copy_from_slice(&random.hash_one(sequence).to_be_bytes());

        let (sent, written) = oneshot::channel();
        let (received, pong) = oneshot::channel();
        let deadline = Instant::now() + config.pong_timeout;
        let ping = PendingPing {
            payload,
            deadline,
            received,
        };
        let result = time::timeout_at(deadline, async {
            control
                .send(WriteControl::HeartbeatPing { ping, sent })
                .await
                .map_err(|_| ())?;
            written.await.map_err(|_| ())?;
            pong.await.map_err(|_| ())
        })
        .await;

        match result {
            Ok(Ok(())) => {}
            Ok(Err(())) => return CloseRequest::Send(None),
            Err(_) => return pong_timeout(),
        }
    }
}
