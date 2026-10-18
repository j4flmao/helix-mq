#include <string.h>

#include "proto/codec.h"
#include "repl/repl.h"

/*
 * Wire layout (big-endian):  u8 type | u32 from | u32 to | u32 epoch | body
 *   FETCH       : u64 fetch_offset | u32 last_fetched_epoch
 *   FETCH_RESP  : u8 status | u64 fetch_offset | u64 hw | u32 div_epoch | u64 div_end |
 *                 u32 n | n x (u32 epoch | u32 len | data)
 *   ALTER_ISR   : u32 isr | u32 isr_version
 */

int hx_repl_msg_encode(const hx_repl_msg *m, hx_buf *out)
{
    int bad = hx_buf_put_u8(out, (uint8_t)m->type) != 0 || hx_buf_put_u32(out, m->from) != 0 ||
              hx_buf_put_u32(out, m->to) != 0 || hx_buf_put_u32(out, m->epoch) != 0;
    switch (m->type) {
    case HX_REPL_MSG_FETCH:
        bad = bad || hx_buf_put_u64(out, m->fetch_offset) != 0 ||
              hx_buf_put_u32(out, m->last_fetched_epoch) != 0;
        break;
    case HX_REPL_MSG_FETCH_RESP:
        bad = bad || hx_buf_put_u8(out, (uint8_t)m->status) != 0 ||
              hx_buf_put_u64(out, m->fetch_offset) != 0 || hx_buf_put_u64(out, m->hw) != 0 ||
              hx_buf_put_u32(out, m->div_epoch) != 0 || hx_buf_put_u64(out, m->div_end) != 0 ||
              hx_buf_put_u32(out, m->nrecs) != 0;
        for (uint32_t i = 0; i < m->nrecs && !bad; i++) {
            bad = hx_buf_put_u32(out, m->recs[i].epoch) != 0 ||
                  hx_buf_put_u32(out, m->recs[i].len) != 0 ||
                  hx_buf_put(out, m->recs[i].data, m->recs[i].len) != 0;
        }
        break;
    case HX_REPL_MSG_ALTER_ISR:
        bad = bad || hx_buf_put_u32(out, m->isr) != 0 || hx_buf_put_u32(out, m->isr_version) != 0;
        break;
    default:
        bad = 1;
        break;
    }
    return bad ? -1 : 0;
}

int hx_repl_msg_decode(const uint8_t *p, size_t len, hx_repl_rec *scratch, size_t cap,
                       hx_repl_msg *m)
{
    hx_rd rd;
    hx_rd_init(&rd, p, len);
    memset(m, 0, sizeof *m);
    const uint8_t type = hx_rd_u8(&rd);
    m->from = hx_rd_u32(&rd);
    m->to = hx_rd_u32(&rd);
    m->epoch = hx_rd_u32(&rd);
    if (!rd.ok || type < HX_REPL_MSG_FETCH || type > HX_REPL_MSG_ALTER_ISR) {
        return -1;
    }
    m->type = (hx_repl_msg_type)type;
    switch (m->type) {
    case HX_REPL_MSG_FETCH:
        m->fetch_offset = hx_rd_u64(&rd);
        m->last_fetched_epoch = hx_rd_u32(&rd);
        break;
    case HX_REPL_MSG_FETCH_RESP: {
        const uint8_t st = hx_rd_u8(&rd);
        m->fetch_offset = hx_rd_u64(&rd);
        m->hw = hx_rd_u64(&rd);
        m->div_epoch = hx_rd_u32(&rd);
        m->div_end = hx_rd_u64(&rd);
        const uint32_t n = hx_rd_u32(&rd);
        if (!rd.ok || st > HX_REPL_RESP_FENCED || n > cap) {
            return -1;
        }
        m->status = (hx_repl_resp_status)st;
        for (uint32_t i = 0; i < n; i++) {
            scratch[i].epoch = hx_rd_u32(&rd);
            scratch[i].len = hx_rd_u32(&rd);
            scratch[i].data = hx_rd_take(&rd, scratch[i].len);
            if (!rd.ok) {
                return -1;
            }
        }
        m->recs = scratch;
        m->nrecs = n;
        break;
    }
    case HX_REPL_MSG_ALTER_ISR:
        m->isr = hx_rd_u32(&rd);
        m->isr_version = hx_rd_u32(&rd);
        break;
    default:
        return -1;
    }
    return (rd.ok && hx_rd_left(&rd) == 0) ? 0 : -1;
}
