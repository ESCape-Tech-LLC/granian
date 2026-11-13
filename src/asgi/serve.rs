use pyo3::{exceptions::PyValueError, prelude::*};
use std::sync::Arc;

use super::{
    heartbeat::HeartbeatConfig,
    http::{handle, handle_ws},
};

use crate::callbacks::CallbackScheduler;
use crate::conversion::{worker_http1_config_from_py, worker_http2_config_from_py};
use crate::net::{ListenerSpec, SocketHolder};
use crate::serve::gen_serve_match;
use crate::workers::{WorkerConfig, WorkerSignal};

macro_rules! websocket_handler {
    ($worker:expr) => {{
        let config = $worker.websocket_heartbeat;
        let paths = $worker.websocket_heartbeat_paths.clone();
        move |rt, callback, server_addr, client_addr, req, scheme| {
            handle_ws(rt, callback, server_addr, client_addr, req, scheme, config, paths.clone())
        }
    }};
}

#[pyclass(frozen, module = "granian._granian")]
pub struct ASGIWorker {
    config: WorkerConfig,
    websocket_heartbeat: Option<HeartbeatConfig>,
    websocket_heartbeat_paths: Option<Arc<[String]>>,
}

#[pymethods]
impl ASGIWorker {
    #[new]
    #[pyo3(
        signature = (
            worker_id,
            sock,
            ipc,
            threads=1,
            blocking_threads=512,
            py_threads=1,
            py_threads_idle_timeout=30,
            backpressure=256,
            http_mode="1",
            http1_opts=None,
            http2_opts=None,
            websockets_enabled=false,
            static_files=None,
            ssl_enabled=false,
            ssl_cert=None,
            ssl_key=None,
            ssl_key_password=None,
            ssl_protocol_min="1.3",
            ssl_ca=None,
            ssl_crl=vec![],
            ssl_client_verify=false,
            metrics=(None, None),
            websocket_heartbeat=None,
            websocket_heartbeat_paths=vec![],
        )
    )]
    fn new(
        py: Python,
        worker_id: i32,
        sock: (Option<Py<ListenerSpec>>, Option<Py<SocketHolder>>),
        ipc: Option<Py<crate::ipc::IPCSenderHandle>>,
        threads: usize,
        blocking_threads: usize,
        py_threads: usize,
        py_threads_idle_timeout: u64,
        backpressure: usize,
        http_mode: &str,
        http1_opts: Option<Py<PyAny>>,
        http2_opts: Option<Py<PyAny>>,
        websockets_enabled: bool,
        static_files: Option<(Vec<(String, String)>, Option<String>, Option<String>)>,
        ssl_enabled: bool,
        ssl_cert: Option<String>,
        ssl_key: Option<String>,
        ssl_key_password: Option<String>,
        ssl_protocol_min: &str,
        ssl_ca: Option<String>,
        ssl_crl: Vec<String>,
        ssl_client_verify: bool,
        metrics: (Option<u64>, Option<Py<crate::metrics::MetricsAggregator>>),
        websocket_heartbeat: Option<(f64, f64, f64)>,
        websocket_heartbeat_paths: Vec<String>,
    ) -> PyResult<Self> {
        if !websocket_heartbeat_paths.is_empty() && websocket_heartbeat.is_none() {
            return Err(PyValueError::new_err("WebSocket heartbeat paths require a Ping interval"));
        }
        if websocket_heartbeat_paths
            .iter()
            .any(|path| !path.starts_with('/') || path.contains(['?', '#']))
        {
            return Err(PyValueError::new_err(
                "WebSocket heartbeat paths must be absolute URI paths without a query or fragment",
            ));
        }
        let websocket_heartbeat = websocket_heartbeat
            .map(|(interval, pong_timeout, close_timeout)| {
                if !websockets_enabled || http_mode == "2" {
                    return Err(PyValueError::new_err("WebSocket heartbeat requires HTTP/1 WebSockets"));
                }
                HeartbeatConfig::new(interval, pong_timeout, close_timeout)
                    .ok_or_else(|| PyValueError::new_err("WebSocket heartbeat durations must be positive and finite"))
            })
            .transpose()?;
        Ok(Self {
            websocket_heartbeat,
            websocket_heartbeat_paths: (!websocket_heartbeat_paths.is_empty())
                .then(|| websocket_heartbeat_paths.into()),
            config: WorkerConfig::new(
                worker_id,
                sock,
                ipc,
                threads,
                blocking_threads,
                py_threads,
                py_threads_idle_timeout,
                backpressure,
                http_mode,
                worker_http1_config_from_py(py, http1_opts)?,
                worker_http2_config_from_py(py, http2_opts)?,
                websockets_enabled,
                static_files,
                ssl_enabled,
                ssl_cert,
                ssl_key,
                ssl_key_password,
                ssl_protocol_min,
                ssl_ca,
                ssl_crl,
                ssl_client_verify,
                metrics,
            ),
        })
    }

    fn serve_mtr(
        &self,
        py: Python,
        callback: Py<CallbackScheduler>,
        event_loop: &Bound<PyAny>,
        signal: Py<WorkerSignal>,
    ) {
        gen_serve_match!(
            crate::serve::serve_mt,
            WorkerAcceptorTcpPlain,
            WorkerAcceptorTcpTls,
            self,
            py,
            callback,
            event_loop,
            signal,
            handle,
            websocket_handler!(self)
        );
    }

    fn serve_str(
        &self,
        py: Python,
        callback: Py<CallbackScheduler>,
        event_loop: &Bound<PyAny>,
        signal: Py<WorkerSignal>,
    ) {
        gen_serve_match!(
            crate::serve::serve_st,
            WorkerAcceptorTcpPlain,
            WorkerAcceptorTcpTls,
            self,
            py,
            callback,
            event_loop,
            signal,
            handle,
            websocket_handler!(self)
        );
    }

    fn serve_async(
        &self,
        py: Python,
        callback: Py<CallbackScheduler>,
        event_loop: &Bound<PyAny>,
        signal: Py<WorkerSignal>,
    ) {
        gen_serve_match!(
            crate::serve::serve_fut,
            WorkerAcceptorTcpPlain,
            WorkerAcceptorTcpTls,
            self,
            py,
            callback,
            event_loop,
            signal,
            handle,
            websocket_handler!(self)
        );
    }

    #[cfg(unix)]
    fn serve_mtr_uds(
        &self,
        py: Python,
        callback: Py<CallbackScheduler>,
        event_loop: &Bound<PyAny>,
        signal: Py<WorkerSignal>,
    ) {
        gen_serve_match!(
            crate::serve::serve_mt_uds,
            WorkerAcceptorUdsPlain,
            WorkerAcceptorUdsTls,
            self,
            py,
            callback,
            event_loop,
            signal,
            handle,
            websocket_handler!(self)
        );
    }

    #[cfg(unix)]
    fn serve_str_uds(
        &self,
        py: Python,
        callback: Py<CallbackScheduler>,
        event_loop: &Bound<PyAny>,
        signal: Py<WorkerSignal>,
    ) {
        gen_serve_match!(
            crate::serve::serve_st_uds,
            WorkerAcceptorUdsPlain,
            WorkerAcceptorUdsTls,
            self,
            py,
            callback,
            event_loop,
            signal,
            handle,
            websocket_handler!(self)
        );
    }

    #[cfg(unix)]
    fn serve_async_uds(
        &self,
        py: Python,
        callback: Py<CallbackScheduler>,
        event_loop: &Bound<PyAny>,
        signal: Py<WorkerSignal>,
    ) {
        gen_serve_match!(
            crate::serve::serve_fut_uds,
            WorkerAcceptorUdsPlain,
            WorkerAcceptorUdsTls,
            self,
            py,
            callback,
            event_loop,
            signal,
            handle,
            websocket_handler!(self)
        );
    }
}
