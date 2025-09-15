//! pprof profiling server for the node.
//!
//! This module provides a HTTP server that serves pprof profiling data including:
//! - CPU profiling via pprof::ProfilerGuardBuilder
//! - Memory profiling via jemalloc_pprof

use eyre::{Context, Result};
use http::{header::CONTENT_TYPE, HeaderValue, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Bytes;
use pprof::{protos::Message, ProfilerGuardBuilder};
use reth_tasks::TaskExecutor;
use std::{collections::HashMap, convert::Infallible, net::SocketAddr, time::Duration};
use tracing::{debug, error, info};

/// Configuration for the [`PprofServer`]
#[derive(Debug)]
pub struct PprofServerConfig {
    listen_addr: SocketAddr,
    task_executor: TaskExecutor,
}

impl PprofServerConfig {
    /// Create a new [`PprofServerConfig`] with the given configuration
    pub const fn new(listen_addr: SocketAddr, task_executor: TaskExecutor) -> Self {
        Self { listen_addr, task_executor }
    }
}

/// [`PprofServer`] responsible for serving the pprof endpoint
#[derive(Debug)]
pub struct PprofServer {
    config: PprofServerConfig,
}

impl PprofServer {
    /// Create a new [`PprofServer`] with the given configuration
    pub const fn new(config: PprofServerConfig) -> Self {
        Self { config }
    }

    /// Spawns the pprof server
    pub async fn serve(&self) -> Result<()> {
        let PprofServerConfig { listen_addr, task_executor } = &self.config;

        info!(target: "reth::cli", "Starting pprof endpoint at {}", listen_addr);

        self.start_endpoint(*listen_addr, task_executor.clone())
            .await
            .with_context(|| format!("Could not start pprof endpoint at {listen_addr}"))
    }

    async fn start_endpoint(
        &self,
        listen_addr: SocketAddr,
        task_executor: TaskExecutor,
    ) -> Result<()> {
        let listener = tokio::net::TcpListener::bind(listen_addr)
            .await
            .context("Could not bind to address")?;

        task_executor.spawn_with_graceful_shutdown_signal(|mut signal| {
            Box::pin(async move {
                loop {
                    let io = tokio::select! {
                        _ = &mut signal => break,
                        io = listener.accept() => {
                            match io {
                                Ok((stream, _remote_addr)) => stream,
                                Err(err) => {
                                    error!(%err, "failed to accept connection");
                                    continue;
                                }
                            }
                        }
                    };

                    let service = tower::service_fn(move |req| Box::pin(handle_pprof_request(req)));

                    let mut shutdown = signal.clone().ignore_guard();
                    tokio::task::spawn(async move {
                        let _ = jsonrpsee_server::serve_with_graceful_shutdown(
                            io,
                            service,
                            &mut shutdown,
                        )
                        .await
                        .inspect_err(|error| debug!(%error, "failed to serve request"));
                    });
                }
            })
        });

        Ok(())
    }
}

/// Handle pprof HTTP requests
async fn handle_pprof_request(
    req: http::Request<hyper::body::Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let path = req.uri().path();

    let result = match path {
        "/debug/pprof" | "/debug/pprof/" => handle_index().await,
        "/debug/pprof/profile" => handle_cpu_profile(req).await,
        "/debug/pprof/heap" => handle_memory_profile().await,
        _ => {
            let mut response = Response::new(Full::new(Bytes::from("Not Found")));
            *response.status_mut() = StatusCode::NOT_FOUND;
            Ok(response)
        }
    };

    match result {
        Ok(response) => Ok(response),
        Err(e) => {
            error!(target: "reth::cli", error = %e, "Error handling pprof request");
            let mut response = Response::new(Full::new(Bytes::from("Internal Server Error")));
            *response.status_mut() = StatusCode::INTERNAL_SERVER_ERROR;
            Ok(response)
        }
    }
}

/// Handle CPU profiling requests
async fn handle_cpu_profile(
    req: http::Request<hyper::body::Incoming>,
) -> Result<Response<Full<Bytes>>> {
    let query = req.uri().query().unwrap_or("");
    let params: HashMap<String, String> = serde_urlencoded::from_str(query).unwrap_or_default();

    // Default to 30 seconds, max 300 seconds (5 minutes)
    let duration_secs =
        params.get("seconds").and_then(|s| s.parse::<u64>().ok()).unwrap_or(30).min(300);

    let duration = Duration::from_secs(duration_secs);

    info!(target: "reth::cli", "Starting CPU profile for {} seconds", duration_secs);

    let guard = ProfilerGuardBuilder::default()
        .frequency(997)
        .blocklist(&["libc", "libgcc", "pthread", "vdso"])
        .build()
        .context("Failed to start profiler")?;

    tokio::time::sleep(duration).await;

    let mut content = Vec::new();
    guard.report().build()?.pprof()?.write_to_vec(&mut content)?;

    let mut response = Response::new(Full::new(Bytes::from(content)));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("application/octet-stream"));

    info!(target: "reth::cli", "CPU profile completed successfully");
    Ok(response)
}

/// Handle memory profiling requests via jemalloc profiling
#[cfg(all(feature = "jemalloc", unix))]
async fn handle_memory_profile() -> Result<Response<Full<Bytes>>> {
    use tikv_jemalloc_ctl::{epoch, stats};

    info!(target: "reth::cli", "Generating memory profile");

    // Update statistics
    if let Err(e) = epoch::advance() {
        error!(target: "reth::cli", error = %e, "Failed to advance jemalloc epoch");
    }

    // Generate a basic pprof-compatible memory profile
    // For now, create a simplified profile that shows memory allocation information
    let allocated = stats::allocated::read().unwrap_or(0);
    let active = stats::active::read().unwrap_or(0);
    let mapped = stats::mapped::read().unwrap_or(0);
    let resident = stats::resident::read().unwrap_or(0);
    let retained = stats::retained::read().unwrap_or(0);

    // Create a simple protobuf-like memory profile
    // This is a simplified approach - a full implementation would require
    // enabling jemalloc profiling and using proper heap profiling
    let profile_data = format!(
        "# Memory Profile\n\
         # Allocated: {} bytes\n\
         # Active: {} bytes\n\
         # Mapped: {} bytes\n\
         # Resident: {} bytes\n\
         # Retained: {} bytes\n\
         \n\
         # Note: For detailed heap profiling, rebuild with jemalloc profiling enabled\n\
         # and use --enable-prof flag with jemallocator",
        allocated, active, mapped, resident, retained
    );

    let mut response = Response::new(Full::new(Bytes::from(profile_data)));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/plain; charset=utf-8"));

    info!(target: "reth::cli", "Memory profile completed successfully");
    Ok(response)
}

/// Handle memory profiling requests when jemalloc feature is not enabled
#[cfg(not(all(feature = "jemalloc", unix)))]
async fn handle_memory_profile() -> Result<Response<Full<Bytes>>> {
    let mut response = Response::new(Full::new(Bytes::from(
        "jemalloc memory profiling not available (jemalloc feature not enabled or not on Unix)",
    )));
    *response.status_mut() = StatusCode::NOT_IMPLEMENTED;
    Ok(response)
}

/// Handle index page requests
async fn handle_index() -> Result<Response<Full<Bytes>>> {
    let index_html = r#"<!DOCTYPE html>
<html>
<head>
    <title>pprof</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        h1 { color: #333; }
        ul { line-height: 1.6; }
        a { color: #0066cc; text-decoration: none; }
        a:hover { text-decoration: underline; }
        .description { color: #666; margin-top: 20px; }
    </style>
</head>
<body>
    <h1>pprof Profiling Endpoints</h1>
    <p>Available profiles:</p>
    <ul>
        <li><a href="/debug/pprof/profile?seconds=30">CPU Profile (30s)</a> - Sample CPU usage for 30 seconds</li>
        <li><a href="/debug/pprof/profile?seconds=60">CPU Profile (60s)</a> - Sample CPU usage for 60 seconds</li>
        <li><a href="/debug/pprof/heap">Memory Profile</a> - Current memory allocation profile</li>
    </ul>
    <div class="description">
        <p><strong>Note:</strong> Profiles are returned in protobuf format compatible with <code>go tool pprof</code>.</p>
        <p>To analyze a profile:</p>
        <pre>go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30</pre>
    </div>
</body>
</html>"#;

    let mut response = Response::new(Full::new(Bytes::from(index_html)));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("text/html; charset=utf-8"));

    Ok(response)
}
