#include "codec.h"

#include "protocol.h"

void hx_rd_init(hx_rd *r, const uint8_t *p, size_t len)
{
    r->p = p;
    r->len = len;
    r->pos = 0;
    r->ok = true;
}

size_t hx_rd_left(const hx_rd *r)
{
    return r->len - r->pos;
}

const uint8_t *hx_rd_take(hx_rd *r, size_t n)
{
    if (!r->ok || n > hx_rd_left(r)) {
        r->ok = false;
        return NULL;
    }
    const uint8_t *out = r->p + r->pos;
    r->pos += n;
    return out;
}

uint8_t hx_rd_u8(hx_rd *r)
{
    const uint8_t *b = hx_rd_take(r, 1);
    return b != NULL ? b[0] : 0;
}

uint16_t hx_rd_u16(hx_rd *r)
{
    const uint8_t *b = hx_rd_take(r, 2);
    if (b == NULL) {
        return 0;
    }
    return (uint16_t)((uint32_t)b[0] << 8 | b[1]);
}

uint32_t hx_rd_u32(hx_rd *r)
{
    const uint8_t *b = hx_rd_take(r, 4);
    return b != NULL ? hx_load_be32(b) : 0;
}

uint64_t hx_rd_u64(hx_rd *r)
{
    const uint8_t *b = hx_rd_take(r, 8);
    if (b == NULL) {
        return 0;
    }
    uint64_t v = 0;
    for (size_t i = 0; i < 8; i++) {
        v = (v << 8) | b[i];
    }
    return v;
}

int64_t hx_rd_i64(hx_rd *r)
{
    return (int64_t)hx_rd_u64(r);
}

const char *hx_wire_strerror(uint16_t code)
{
    switch (code) {
    case HXE_OK:
        return "ok";
    case HXE_BAD_REQUEST:
        return "malformed request";
    case HXE_UNKNOWN_API:
        return "unknown api";
    case HXE_UNSUPPORTED_VERSION:
        return "unsupported protocol version";
    case HXE_INVALID_TOPIC:
        return "invalid topic name";
    case HXE_OFFSET_OUT_OF_RANGE:
        return "offset out of range";
    case HXE_RECORD_TOO_LARGE:
        return "record too large";
    case HXE_STORAGE:
        return "storage error";
    case HXE_UNKNOWN_PARTITION:
        return "unknown topic or partition";
    case HXE_RESOURCE_LIMIT:
        return "server resource limit reached";
    case HXE_INVALID_GROUP:
        return "invalid group name";
    case HXE_UNKNOWN_MEMBER:
        return "unknown group or member";
    case HXE_REBALANCE_IN_PROGRESS:
        return "rebalance in progress";
    case HXE_ILLEGAL_GENERATION:
        return "stale group generation";
    case HXE_NOT_ASSIGNED:
        return "partition not assigned to this member";
    case HXE_UNKNOWN_PRODUCER:
        return "unknown producer id (expired or broker restarted)";
    case HXE_OUT_OF_ORDER_SEQUENCE:
        return "out-of-order producer sequence";
    case HXE_TOPIC_EXISTS:
        return "topic already exists";
    case HXE_NOT_LEADER:
        return "not the leader of this partition (see ClusterMetadata)";
    case HXE_NOT_ENOUGH_REPLICAS:
        return "not enough in-sync replicas";
    case HXE_REQUEST_TIMEOUT:
        return "replication timed out; the record may still commit";
    case HXE_UNSUPPORTED:
        return "not supported on a replicated topic";
    default:
        return "unknown error code";
    }
}
