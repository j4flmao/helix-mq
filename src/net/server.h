/*
 * Single-threaded epoll TCP server (level-triggered, non-blocking).
 *
 * Properties:
 *   - Pipelining: many requests per connection, responses in request order.
 *   - Backpressure: a connection whose unsent output exceeds max_out_pending
 *     stops being read (and its already-buffered requests stop being
 *     processed) until the peer drains it, so memory per connection is bounded.
 *   - Frame size is validated before any allocation; oversized or malformed
 *     frames close the connection.
 *
 * This is the epoll backend of ADR-006; the broker core is I/O-free, so an
 * io_uring backend or a thread-per-core layout (ADR-003) can reuse it as-is.
 */
#ifndef HELIX_SERVER_H
#define HELIX_SERVER_H

#include <signal.h>
#include <stddef.h>
#include <stdint.h>

#include "broker/broker.h"

/*
 * Optional extension point (cluster mode). handle() sees every request before the broker does:
 * 0 = not mine, give it to the broker; 1 = answered (response appended to out); 2 = parked, the
 * answer comes later: no more frames of that connection are processed until resolve() returns 1;
 * -1 = close the connection. poll() runs every loop iteration (at least every tick_ms); -1 stops
 * the server (fail-stop). All callbacks run on the event-loop thread.
 */
typedef struct {
    void *ctx;
    int (*handle)(void *ctx, uint64_t conn, const uint8_t *req, size_t len, hx_buf *out);
    int (*resolve)(void *ctx, uint64_t conn, hx_buf *out);
    void (*drop)(void *ctx, uint64_t conn);
    int (*poll)(void *ctx, int64_t now_ms);
    int tick_ms;
} hx_server_hooks;

typedef struct {
    const hx_server_hooks *hooks; /* NULL = plain broker */
    const char *bind_addr;        /* IPv4 dotted quad, e.g. "127.0.0.1"          */
    uint16_t port;                /* 0 = pick an ephemeral port                  */
    uint32_t max_frame_bytes;     /* max request frame (after the length prefix) */
    uint32_t max_conns;           /* connections beyond this are refused         */
    size_t max_out_pending;       /* per-connection unsent-output high-water mark */
    const char *port_file;        /* if non-NULL, actual port written here       */
    int listen_backlog;           /* listen(2) backlog, default 1024             */
    uint8_t tcp_nodelay;          /* 1 = disable Nagle per connection (default)  */
    uint32_t recv_buffer_bytes;   /* SO_RCVBUF cap, 0 leaves the OS default      */
    uint32_t send_buffer_bytes;   /* SO_SNDBUF cap, 0 leaves the OS default      */
} hx_server_cfg;

void hx_server_cfg_default(hx_server_cfg *cfg);

/*
 * Run the event loop until *stop becomes non-zero (set from a signal handler)
 * or a fatal error occurs. Returns 0 on clean shutdown, -1 on fatal error.
 */
int hx_server_run(const hx_server_cfg *cfg, hx_broker *broker, const volatile sig_atomic_t *stop);

#endif /* HELIX_SERVER_H */
