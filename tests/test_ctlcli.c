/*
 * The broker side of the controller channel against a hand-written fake controller: what a broker
 * adopts and what it passes on to the replication layer.
 */
#define _XOPEN_SOURCE 700
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

#include "ctrl/ctrl_net.h"

static int failures;

#define REQ(cond)                                                                                  \
    do {                                                                                           \
        if (!(cond)) {                                                                             \
            (void)fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);                  \
            failures++;                                                                            \
            return false;                                                                          \
        }                                                                                          \
    } while (0)

static int64_t now_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

typedef struct {
    int lfd;
    int cfd;
    uint16_t port;
    uint8_t rx[65536];
    size_t rxn;
} fake;

static bool fake_open(fake *f)
{
    memset(f, 0, sizeof *f);
    f->lfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in a;
    memset(&a, 0, sizeof a);
    a.sin_family = AF_INET;
    (void)inet_pton(AF_INET, "127.0.0.1", &a.sin_addr);
    socklen_t al = sizeof a;
    REQ(bind(f->lfd, (struct sockaddr *)&a, sizeof a) == 0 && listen(f->lfd, 4) == 0);
    REQ(getsockname(f->lfd, (struct sockaddr *)&a, &al) == 0);
    f->port = ntohs(a.sin_port);
    f->cfd = -1;
    (void)fcntl(f->lfd, F_SETFL, O_NONBLOCK);
    return true;
}

static void wr(int fd, const void *b, size_t n)
{
    if (write(fd, b, n) != (ssize_t)n) {
        failures++;
    }
}

static void put32(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)(v >> 24);
    p[1] = (uint8_t)(v >> 16);
    p[2] = (uint8_t)(v >> 8);
    p[3] = (uint8_t)v;
}

static void put64(uint8_t *p, uint64_t v)
{
    put32(p, (uint32_t)(v >> 32));
    put32(p + 4, (uint32_t)v);
}

typedef struct {
    int states;
    uint32_t last_part;
    hx_ctrl_part last;
} sink;

static void on_state(void *ctx, uint32_t part, const hx_ctrl_part *st)
{
    sink *s = ctx;
    s->states++;
    s->last_part = part;
    s->last = *st;
}

static void pump(hx_ctlcli *c, fake *f, int ms)
{
    const int64_t end = now_ms() + ms;
    while (now_ms() < end) {
        hx_ctlcli_poll(c, 5, now_ms());
        if (f->cfd < 0) {
            f->cfd = accept(f->lfd, NULL, NULL);
            if (f->cfd >= 0) {
                (void)fcntl(f->cfd, F_SETFL, O_NONBLOCK);
            }
        }
        if (f->cfd >= 0) {
            const ssize_t r = recv(f->cfd, f->rx + f->rxn, sizeof f->rx - f->rxn, 0);
            if (r > 0) {
                f->rxn += (size_t)r;
            } else if (r == 0) {
                (void)close(f->cfd);
                f->cfd = -1;
                f->rxn = 0;
            }
        }
    }
}

static void send_session(fake *f, uint64_t nonce, uint64_t session)
{
    uint8_t b[4 + 1 + 16];
    put32(b, 17);
    b[4] = 3;
    put64(b + 5, nonce);
    put64(b + 13, session);
    wr(f->cfd, b, sizeof b);
}

static void send_state(fake *f, uint32_t part, uint64_t session, uint32_t epoch, uint32_t leader,
                       uint32_t isr, uint32_t ver)
{
    uint8_t b[4 + 1 + 4 + 8 + 16];
    put32(b, sizeof b - 4U);
    b[4] = 4;
    put32(b + 5, part);
    put64(b + 9, session);
    put32(b + 17, epoch);
    put32(b + 21, leader);
    put32(b + 25, isr);
    put32(b + 29, ver);
    wr(f->cfd, b, sizeof b);
}

static bool scenario(void)
{
    fake f;
    REQ(fake_open(&f));
    sink s;
    memset(&s, 0, sizeof s);
    hx_ctlcli_cfg cfg;
    memset(&cfg, 0, sizeof cfg);
    cfg.broker = 2;
    cfg.nonce = 0xABCD;
    cfg.ncontrollers = 1;
    cfg.addrs[0] = "127.0.0.1";
    cfg.ports[0] = f.port;
    cfg.hello_ms = 20;
    cfg.on_state = on_state;
    cfg.ctx = &s;
    hx_ctlcli *c = hx_ctlcli_create(&cfg);
    REQ(c != NULL);
    pump(c, &f, 300);
    REQ(f.cfd >= 0);
    /* The first thing a broker says is HELLO(broker 2, nonce). */
    REQ(f.rxn >= 17U && f.rx[4] == 1);
    REQ(f.rx[8] == 2 && f.rx[9 + 6] == 0xAB && f.rx[9 + 7] == 0xCD);

    /* State before any session is ignored. */
    send_state(&f, 0, 77, 1, 2, 7, 1);
    send_state(&f, 0, 0, 1, 2, 7, 1);
    pump(c, &f, 100);
    REQ(s.states == 0 && hx_ctlcli_session(c) == 0);
    /* A session granted to somebody else's nonce (a previous life of this broker) is not adopted.
     */
    send_session(&f, 0x9999, 50);
    pump(c, &f, 100);
    REQ(hx_ctlcli_session(c) == 0);
    send_state(&f, 0, 50, 1, 2, 7, 1);
    pump(c, &f, 100);
    REQ(s.states == 0);
    /* Our own nonce: adopted. */
    send_session(&f, 0xABCD, 60);
    pump(c, &f, 100);
    REQ(hx_ctlcli_session(c) == 60);
    /* Only state carrying the adopted session is passed on. */
    send_state(&f, 3, 50, 1, 2, 7, 1);
    send_state(&f, 3, 61, 1, 2, 7, 1);
    pump(c, &f, 100);
    REQ(s.states == 0);
    send_state(&f, 3, 60, 4, 2, 6, 9);
    pump(c, &f, 100);
    REQ(s.states == 1 && s.last_part == 3 && s.last.epoch == 4 && s.last.leader == 2 &&
        s.last.isr == 6 && s.last.isr_version == 9);
    /* An older session number never replaces a newer one. */
    send_session(&f, 0xABCD, 55);
    pump(c, &f, 100);
    REQ(hx_ctlcli_session(c) == 60);
    /* A newer one for the same life does (the controller declared us down and registered us again).
     */
    send_session(&f, 0xABCD, 70);
    pump(c, &f, 100);
    REQ(hx_ctlcli_session(c) == 70);

    /* ALTER carries the adopted session and our identity. */
    f.rxn = 0;
    REQ(hx_ctlcli_alter(c, 5, 4, 0x6, 9) == 1);
    pump(c, &f, 100);
    size_t at = 0;
    bool found = false;
    while (at + 5U <= f.rxn) {
        const uint32_t len = ((uint32_t)f.rx[at] << 24) | ((uint32_t)f.rx[at + 1] << 16) |
                             ((uint32_t)f.rx[at + 2] << 8) | f.rx[at + 3];
        if (f.rx[at + 4] == 2 && len == 29U && at + 4U + len <= f.rxn) {
            REQ(f.rx[at + 8] == 5 && f.rx[at + 12] == 2 && f.rx[at + 20] == 70);
            found = true;
            break;
        }
        at += 4U + len;
    }
    REQ(found);

    /* Garbage from the controller drops the connection; the client reconnects and says hello again.
     */
    const uint8_t junk[] = {0, 0, 0, 3, 77, 1, 2};
    wr(f.cfd, junk, sizeof junk);
    pump(c, &f, 100);
    REQ(f.cfd < 0 || f.rxn == 0);
    f.rxn = 0;
    pump(c, &f, 1500);
    REQ(f.cfd >= 0 && f.rxn >= 17U && f.rx[4] == 1);
    REQ(hx_ctlcli_session(c) == 70); /* the session is the broker's, not the connection's */
    hx_ctlcli_destroy(c);
    if (f.cfd >= 0) {
        (void)close(f.cfd);
    }
    (void)close(f.lfd);
    return true;
}

int main(void)
{
    const bool ok = scenario();
    if (!ok || failures != 0) {
        (void)fprintf(stderr, "controller client test failed\n");
        return 1;
    }
    (void)puts("all controller client tests passed");
    return 0;
}
