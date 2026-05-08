# smb-proxy-service-wb

HTTP proxy router with upstream failover by port range and credential channels.

## Performance tuning

New flags for connection reuse and lower latency:

- `-keepalive-idle-timeout` (default: `90s`) idle timeout for upstream GET keep-alive pool.
- `-keepalive-max-idle-conns` (default: `1000`) max idle upstream GET connections globally.
- `-keepalive-max-idle-per-host` (default: `100`) max idle upstream GET connections per endpoint.
- `-sticky-port-ttl` (default: `45s`) sticky upstream port per credential channel; `0` disables sticky.
- `-bad-proxy-penalty-base` (default: `15s`) base cooldown for a failing upstream port.
- `-bad-proxy-penalty-max` (default: `5m`) max cooldown for repeatedly failing upstream ports.
- `-bad-proxy-pick-samples` (default: `8`) how many candidate ports to sample before choosing the least penalized.

Notes:

- GET now uses a pooled `http.Client`/`Transport` keyed by upstream endpoint and credentials.
- CONNECT remains one dedicated tunnel per client connection (no multiplexing of active tunnels).
- Failing ports are temporarily deprioritized (soft invalidation) and automatically return to rotation after cooldown.