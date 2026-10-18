#include <string.h>

#include "proto/codec.h"
#include "raft/raft.h"

/*
 * Wire layout (big-endian):  u8 type | u32 from | u32 to | u64 term | body
 *   PREVOTE, VOTE        : u64 last_index | u64 last_term
 *   PREVOTE_RESP, VOTE_RESP : u8 granted
 *   APPEND               : u64 prev_index | u64 prev_term | u64 commit | u32 n | n x (u64 term |
 * u32 len | data) APPEND_RESP          : u8 granted | u64 match
 *   SNAPSHOT             : u64 last_index | u64 last_term | u32 len | data
 */

int hx_raft_msg_encode(const hx_raft_msg *m, hx_buf *out)
{
    int bad = hx_buf_put_u8(out, (uint8_t)m->type) != 0 || hx_buf_put_u32(out, m->from) != 0 ||
              hx_buf_put_u32(out, m->to) != 0 || hx_buf_put_u64(out, m->term) != 0;
    switch (m->type) {
    case HX_RAFT_MSG_PREVOTE:
    case HX_RAFT_MSG_VOTE:
        bad = bad || hx_buf_put_u64(out, m->last_index) != 0 ||
              hx_buf_put_u64(out, m->last_term) != 0;
        break;
    case HX_RAFT_MSG_PREVOTE_RESP:
    case HX_RAFT_MSG_VOTE_RESP:
        bad = bad || hx_buf_put_u8(out, m->granted ? 1 : 0) != 0;
        break;
    case HX_RAFT_MSG_APPEND:
        bad = bad || hx_buf_put_u64(out, m->last_index) != 0 ||
              hx_buf_put_u64(out, m->last_term) != 0 || hx_buf_put_u64(out, m->commit) != 0 ||
              hx_buf_put_u32(out, m->nentries) != 0;
        for (uint32_t i = 0; i < m->nentries && !bad; i++) {
            bad = hx_buf_put_u64(out, m->entries[i].term) != 0 ||
                  hx_buf_put_u32(out, m->entries[i].len) != 0 ||
                  hx_buf_put(out, m->entries[i].data, m->entries[i].len) != 0;
        }
        break;
    case HX_RAFT_MSG_APPEND_RESP:
        bad = bad || hx_buf_put_u8(out, m->granted ? 1 : 0) != 0 ||
              hx_buf_put_u64(out, m->match) != 0;
        break;
    case HX_RAFT_MSG_SNAPSHOT:
        bad = bad || hx_buf_put_u64(out, m->last_index) != 0 ||
              hx_buf_put_u64(out, m->last_term) != 0 || hx_buf_put_u32(out, m->snap_len) != 0 ||
              hx_buf_put(out, m->snap, m->snap_len) != 0;
        break;
    default:
        bad = 1;
        break;
    }
    return bad ? -1 : 0;
}

int hx_raft_msg_decode(const uint8_t *p, size_t len, hx_raft_entry *scratch, size_t cap,
                       hx_raft_msg *m)
{
    hx_rd rd;
    hx_rd_init(&rd, p, len);
    memset(m, 0, sizeof *m);
    const uint8_t type = hx_rd_u8(&rd);
    m->from = hx_rd_u32(&rd);
    m->to = hx_rd_u32(&rd);
    m->term = hx_rd_u64(&rd);
    if (!rd.ok || type < HX_RAFT_MSG_PREVOTE || type > HX_RAFT_MSG_SNAPSHOT) {
        return -1;
    }
    m->type = (hx_raft_msg_type)type;
    switch (m->type) {
    case HX_RAFT_MSG_PREVOTE:
    case HX_RAFT_MSG_VOTE:
        m->last_index = hx_rd_u64(&rd);
        m->last_term = hx_rd_u64(&rd);
        break;
    case HX_RAFT_MSG_PREVOTE_RESP:
    case HX_RAFT_MSG_VOTE_RESP:
        m->granted = hx_rd_u8(&rd) != 0;
        break;
    case HX_RAFT_MSG_APPEND: {
        m->last_index = hx_rd_u64(&rd);
        m->last_term = hx_rd_u64(&rd);
        m->commit = hx_rd_u64(&rd);
        const uint32_t n = hx_rd_u32(&rd);
        if (!rd.ok || n > cap) {
            return -1;
        }
        for (uint32_t i = 0; i < n; i++) {
            scratch[i].term = hx_rd_u64(&rd);
            scratch[i].len = hx_rd_u32(&rd);
            scratch[i].data = hx_rd_take(&rd, scratch[i].len);
            if (!rd.ok) {
                return -1;
            }
        }
        m->entries = scratch;
        m->nentries = n;
        break;
    }
    case HX_RAFT_MSG_APPEND_RESP:
        m->granted = hx_rd_u8(&rd) != 0;
        m->match = hx_rd_u64(&rd);
        break;
    case HX_RAFT_MSG_SNAPSHOT:
        m->last_index = hx_rd_u64(&rd);
        m->last_term = hx_rd_u64(&rd);
        m->snap_len = hx_rd_u32(&rd);
        m->snap = hx_rd_take(&rd, m->snap_len);
        if (!rd.ok) {
            return -1;
        }
        break;
    default:
        return -1;
    }
    return (rd.ok && hx_rd_left(&rd) == 0) ? 0 : -1;
}
