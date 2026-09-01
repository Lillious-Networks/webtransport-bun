use napi::Result;
use napi_derive::napi;
use std::sync::atomic::Ordering;

use crate::client_stream::{ClientBidiStreamHandle, ClientUniRecvHandle, ClientUniSendHandle};
use crate::panic_guard;
use crate::session_registry;
use crate::RUNTIME;
use tokio::time::{Duration, Instant};

#[napi]
pub struct SessionHandle {
    id: String,
    peer_ip: String,
    peer_port: u32,
}

#[napi]
impl SessionHandle {
    async fn wait_capacity_with_timeout(
        id: String,
        timeout_ms: u32,
        kind: &'static str,
    ) -> Result<()> {
        let deadline = Instant::now() + Duration::from_millis(timeout_ms as u64);
        loop {
            let Some((_, _, metrics, _, _, _, _)) = session_registry::get(&id) else {
                return Err(napi::Error::from_reason("E_SESSION_CLOSED"));
            };
            let Some(sm) = session_registry::get_session_metrics(&id) else {
                return Err(napi::Error::from_reason("E_SESSION_CLOSED"));
            };
            let Some(limits) = session_registry::get_limits(&id) else {
                return Err(napi::Error::from_reason("E_SESSION_CLOSED"));
            };
            let global_ok =
                metrics.streams_active.load(Ordering::Relaxed) < limits.max_streams_global;
            let kind_ok = match kind {
                "bidi" => {
                    sm.streams_bidi_active.load(Ordering::Relaxed)
                        < limits.max_streams_per_session_bidi
                }
                "uni" => {
                    sm.streams_uni_active.load(Ordering::Relaxed)
                        < limits.max_streams_per_session_uni
                }
                _ => false,
            };
            if global_ok && kind_ok {
                return Ok(());
            }
            let Some(notify) = session_registry::get_stream_capacity_notify(&id) else {
                return Err(napi::Error::from_reason("E_SESSION_CLOSED"));
            };
            let now = Instant::now();
            if now >= deadline {
                return Err(napi::Error::from_reason("E_BACKPRESSURE_TIMEOUT"));
            }
            let remain = deadline.saturating_duration_since(now);
            tokio::time::timeout(remain, notify.notified())
                .await
                .map_err(|_| napi::Error::from_reason("E_BACKPRESSURE_TIMEOUT"))?;
        }
    }

    #[napi(constructor)]
    pub fn new(id: String, peer_ip: String, peer_port: u32) -> Self {
        Self {
            id,
            peer_ip,
            peer_port,
        }
    }

    #[napi(getter)]
    pub fn id(&self) -> String {
        self.id.clone()
    }

    #[napi(getter)]
    pub fn peer_ip(&self) -> String {
        self.peer_ip.clone()
    }

    #[napi(getter)]
    pub fn peer_port(&self) -> u32 {
        self.peer_port
    }

    #[napi]
    pub fn close(&self, code: Option<u32>, reason: Option<String>) -> Result<()> {
        let c = code.unwrap_or(0);
        let r = reason.unwrap_or_default();
        session_registry::close_session(&self.id, c, r.as_bytes());
        Ok(())
    }

    #[napi]
    pub async fn send_datagram(&self, data: napi::bindgen_prelude::Buffer) -> Result<()> {
        // quinn's `send_datagram` is a synchronous enqueue - it does not block
        // and returns immediately. The old implementation still wrapped it in
        // `RUNTIME.spawn(...)` + `tokio::time::timeout(...).await`, i.e. a task
        // allocation, a timer, and a join per datagram. At a few thousand
        // sessions each pushing movement datagrams that is tens of thousands of
        // spawns/sec and the tokio scheduler becomes the bottleneck (the
        // "backpressure" wall). Datagrams are lossy by contract, so there is
        // nothing to wait on: reserve, send inline, release, done.
        let id = self.id.clone();
        let bytes = data.as_ref();
        let Some((conn, _, metrics, _, _, _, _)) = session_registry::get(&id) else {
            return Err(napi::Error::from_reason("E_SESSION_CLOSED"));
        };
        let sm = session_registry::get_session_metrics(&id);
        let Some(limits) = session_registry::get_limits(&id) else {
            return Err(napi::Error::from_reason("E_SESSION_CLOSED"));
        };
        let sz = bytes.len();
        if sz > limits.max_datagram_size {
            return Err(napi::Error::from_reason("E_QUEUE_FULL"));
        }
        let sz_u64 = sz as u64;

        // Budget reservation is bookkeeping only for a synchronous send: the
        // bytes leave for quinn's own datagram queue right away, so reserve and
        // release around the call rather than holding the reservation.
        if let Some(ref sm) = sm {
            if !metrics.try_reserve_queued_bytes_with_session(
                &sm.queued_bytes,
                sz_u64,
                limits.max_queued_bytes_global,
                limits.max_queued_bytes_per_session,
            ) {
                return Err(napi::Error::from_reason("E_QUEUE_FULL"));
            }
        }

        let start = std::time::Instant::now();
        let result = conn
            .send_datagram(bytes)
            .map_err(|_| napi::Error::from_reason("E_SESSION_CLOSED"));

        if let Some(ref sm) = sm {
            crate::server_metrics::ServerMetrics::release_session_queued_bytes(
                &sm.queued_bytes,
                metrics.as_ref(),
                sz_u64,
            );
        }

        result?;
        metrics.datagram_enqueue_histogram.observe(start.elapsed());
        metrics.datagrams_out.fetch_add(1, Ordering::Relaxed);
        if let Some(ref sm) = sm {
            sm.datagrams_out.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Synchronous, fire-and-forget datagram send. quinn's `send_datagram` is a
    /// non-blocking enqueue, so there is nothing to await - the old async
    /// signature cost a Promise + a tokio task hop per call. At high player
    /// counts the server pushes tens of thousands of movement datagrams per
    /// second; this removes that per-call overhead from the JS event loop.
    /// Returns false if the session is gone or the datagram was rejected
    /// (oversized / queue full); callers treat datagrams as lossy and ignore it.
    #[napi]
    pub fn send_datagram_sync(&self, data: napi::bindgen_prelude::Buffer) -> bool {
        self.send_one_datagram(data.as_ref())
    }

    /// Batched synchronous send: hand the whole per-session datagram set for one
    /// flush across the napi boundary in a single call instead of one call per
    /// datagram. Returns the number successfully enqueued.
    #[napi]
    pub fn send_datagrams_sync(&self, datagrams: Vec<napi::bindgen_prelude::Buffer>) -> u32 {
        let Some((conn, _, metrics, _, _, _, _)) = session_registry::get(&self.id) else {
            return 0;
        };
        let sm = session_registry::get_session_metrics(&self.id);
        let Some(limits) = session_registry::get_limits(&self.id) else {
            return 0;
        };
        let mut sent: u32 = 0;
        for buf in &datagrams {
            let bytes = buf.as_ref();
            let sz = bytes.len();
            if sz == 0 || sz > limits.max_datagram_size {
                continue;
            }
            let sz_u64 = sz as u64;
            if let Some(ref sm) = sm {
                if !metrics.try_reserve_queued_bytes_with_session(
                    &sm.queued_bytes,
                    sz_u64,
                    limits.max_queued_bytes_global,
                    limits.max_queued_bytes_per_session,
                ) {
                    continue;
                }
            }
            let ok = conn.send_datagram(bytes).is_ok();
            if let Some(ref sm) = sm {
                crate::server_metrics::ServerMetrics::release_session_queued_bytes(
                    &sm.queued_bytes,
                    metrics.as_ref(),
                    sz_u64,
                );
            }
            if ok {
                sent += 1;
                metrics.datagrams_out.fetch_add(1, Ordering::Relaxed);
                if let Some(ref sm) = sm {
                    sm.datagrams_out.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
        sent
    }

    fn send_one_datagram(&self, bytes: &[u8]) -> bool {
        let Some((conn, _, metrics, _, _, _, _)) = session_registry::get(&self.id) else {
            return false;
        };
        let sm = session_registry::get_session_metrics(&self.id);
        let Some(limits) = session_registry::get_limits(&self.id) else {
            return false;
        };
        let sz = bytes.len();
        if sz == 0 || sz > limits.max_datagram_size {
            return false;
        }
        let sz_u64 = sz as u64;
        if let Some(ref sm) = sm {
            if !metrics.try_reserve_queued_bytes_with_session(
                &sm.queued_bytes,
                sz_u64,
                limits.max_queued_bytes_global,
                limits.max_queued_bytes_per_session,
            ) {
                return false;
            }
        }
        let ok = conn.send_datagram(bytes).is_ok();
        if let Some(ref sm) = sm {
            crate::server_metrics::ServerMetrics::release_session_queued_bytes(
                &sm.queued_bytes,
                metrics.as_ref(),
                sz_u64,
            );
        }
        if ok {
            metrics.datagrams_out.fetch_add(1, Ordering::Relaxed);
            if let Some(ref sm) = sm {
                sm.datagrams_out.fetch_add(1, Ordering::Relaxed);
            }
        }
        ok
    }

    #[napi]
    pub async fn read_datagram(&self) -> Result<Option<napi::bindgen_prelude::Buffer>> {
        let id = self.id.clone();
        let Some((_, dgram_rx, metrics, _, _, _, _)) = session_registry::get(&id) else {
            return Ok(None);
        };
        let sm = session_registry::get_session_metrics(&id);
        let mut rx = dgram_rx.lock().await;
        match rx.recv().await {
            Some(v) => {
                if let Some(ref sm) = sm {
                    crate::server_metrics::ServerMetrics::release_session_queued_bytes(
                        &sm.queued_bytes,
                        &metrics,
                        v.len() as u64,
                    );
                }
                Ok(Some(v.into()))
            }
            None => Ok(None),
        }
    }

    // Streams (P0-1: wired to wtransport via session registry)

    #[napi]
    pub async fn create_bidi_stream(&self) -> Result<ClientBidiStreamHandle> {
        let id = self.id.clone();
        RUNTIME
            .spawn(async move {
                let Some((_, _, metrics, _, _, create_bi_tx, _)) = session_registry::get(&id)
                else {
                    return Err(napi::Error::from_reason("E_SESSION_CLOSED"));
                };
                let start = std::time::Instant::now();
                let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                create_bi_tx
                    .send(resp_tx)
                    .await
                    .map_err(|_| napi::Error::from_reason("E_SESSION_CLOSED"))?;
                let result = resp_rx
                    .await
                    .map_err(|_| napi::Error::from_reason("E_SESSION_CLOSED"))?
                    .map_err(napi::Error::from_reason);
                if result.is_ok() {
                    metrics.stream_open_histogram.observe(start.elapsed());
                }
                result
            })
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?
    }

    #[napi]
    pub async fn wait_bidi_capacity(&self, timeout_ms: u32) -> Result<()> {
        let id = self.id.clone();
        RUNTIME
            .spawn(async move { Self::wait_capacity_with_timeout(id, timeout_ms, "bidi").await })
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?
    }

    #[napi]
    pub async fn accept_bidi_stream(&self) -> Result<Option<ClientBidiStreamHandle>> {
        let id = self.id.clone();
        let Some((_, _, _, bidi_rx, _, _, _)) = session_registry::get(&id) else {
            return Ok(None);
        };
        let mut rx = bidi_rx.lock().await;
        Ok(rx.recv().await)
    }

    #[napi]
    pub async fn create_uni_stream(&self) -> Result<ClientUniSendHandle> {
        let id = self.id.clone();
        RUNTIME
            .spawn(async move {
                let Some((_, _, metrics, _, _, _, create_uni_tx)) = session_registry::get(&id)
                else {
                    return Err(napi::Error::from_reason("E_SESSION_CLOSED"));
                };
                let start = std::time::Instant::now();
                let (resp_tx, resp_rx) = tokio::sync::oneshot::channel();
                create_uni_tx
                    .send(resp_tx)
                    .await
                    .map_err(|_| napi::Error::from_reason("E_SESSION_CLOSED"))?;
                let result = resp_rx
                    .await
                    .map_err(|_| napi::Error::from_reason("E_SESSION_CLOSED"))?
                    .map_err(napi::Error::from_reason);
                if result.is_ok() {
                    metrics.stream_open_histogram.observe(start.elapsed());
                }
                result
            })
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?
    }

    #[napi]
    pub async fn wait_uni_capacity(&self, timeout_ms: u32) -> Result<()> {
        let id = self.id.clone();
        RUNTIME
            .spawn(async move { Self::wait_capacity_with_timeout(id, timeout_ms, "uni").await })
            .await
            .map_err(|e| napi::Error::from_reason(e.to_string()))?
    }

    #[napi]
    pub async fn accept_uni_stream(&self) -> Result<Option<ClientUniRecvHandle>> {
        let id = self.id.clone();
        let Some((_, _, _, _, uni_rx, _, _)) = session_registry::get(&id) else {
            return Ok(None);
        };
        let mut rx = uni_rx.lock().await;
        Ok(rx.recv().await)
    }

    #[napi]
    pub fn metrics_snapshot(&self) -> Result<crate::metrics::SessionMetricsSnapshot> {
        panic_guard::catch_panic(|| {
            if let Some(sm) = session_registry::get_session_metrics(&self.id) {
                Ok(crate::metrics::SessionMetricsSnapshot {
                    datagrams_in: sm.datagrams_in.load(Ordering::Relaxed) as u32,
                    datagrams_out: sm.datagrams_out.load(Ordering::Relaxed) as u32,
                    streams_active: sm.streams_active() as u32,
                    queued_bytes: sm.queued_bytes.load(Ordering::Relaxed) as u32,
                })
            } else {
                Ok(crate::metrics::SessionMetricsSnapshot {
                    datagrams_in: 0,
                    datagrams_out: 0,
                    streams_active: 0,
                    queued_bytes: 0,
                })
            }
        })
    }
}
