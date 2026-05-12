# ADR-004: SSE over WebSockets for dashboard alert streaming

**Status:** Accepted
**Date:** 2026-05-12

---

## Context

The governance dashboard needs to receive new drift alerts in real time
without the user refreshing the page. Two standard options: Server-Sent
Events (SSE) or WebSockets.

---

## Decision

Server-Sent Events via FastAPI StreamingResponse.

---

## Rationale

The data flow here is strictly unidirectional: the server pushes alerts
to the browser. The browser never sends data back over the stream channel
— acknowledgements go through normal POST requests.

SSE is the right tool for unidirectional server-to-client streaming.
It runs over plain HTTP/1.1, requires no protocol upgrade, has native
browser reconnect on connection drop, and is trivially implemented in
FastAPI with a StreamingResponse and an async generator.

WebSockets add bidirectional capability that this use case doesn't need.
That means a stateful connection pool on the server, a more complex
client implementation, and an upgrade handshake on every connection.
The operational complexity is real and the benefit is zero for this
specific pattern.

SSE also has a practical advantage in this deployment context: it works
through standard HTTP proxies and load balancers without configuration
changes. WebSockets require proxy-level support for the Upgrade header,
which is not always available in constrained deployment environments.

---

## Tradeoffs

SSE connections are limited to 6 per browser domain under HTTP/1.1.
For a single-user governance dashboard this is irrelevant. For a
multi-user deployment with many concurrent dashboard sessions, HTTP/2
multiplexing resolves this limit entirely.

SSE does not support binary frames. All messages are UTF-8 text. For
this use case (JSON alert payloads) that is not a constraint.

If the dashboard ever needs to send control commands back to the server
over the same channel (for example, a live filter to subscribe to alerts
from a specific model only), SSE would need to be replaced with
WebSockets. The current architecture routes all writes through REST
endpoints, so this is not a near-term concern.

---

## Alternatives considered

| Option | Reason not chosen |
|--------|------------------|
| WebSockets | Bidirectional complexity not needed; stateful pool overhead |
| Long polling | Higher latency, more server load, worse developer experience |
| Client-side polling (setInterval fetch) | Implemented as fallback only; SSE preferred for live updates |
