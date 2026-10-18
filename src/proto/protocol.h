/*
 * Helix wire protocol v1 constants. See docs/PROTOCOL.md.
 * All multi-byte integers on the wire are big-endian.
 *
 *   Request : u32 len | u16 api | u16 version | u32 correlation_id | body
 *   Response: u32 len | u32 correlation_id | u16 error | body
 *   (len counts the bytes AFTER the length field itself)
 */
#ifndef HELIX_PROTOCOL_H
#define HELIX_PROTOCOL_H

#include <stdint.h>

#define HX_PROTO_VERSION 1U
#define HX_REQ_HDR_LEN   8U /* api + version + correlation id */
#define HX_RESP_HDR_LEN  6U /* correlation id + error         */

#define HX_MAX_TOPIC_LEN     128U
#define HX_MAX_GROUP_LEN     128U
#define HX_MAX_MEMBER_ID     63U
#define HX_MAX_SUBSCRIPTIONS 64U
#define HX_MIN_SESSION_MS    1000U
#define HX_MAX_SESSION_MS    300000U
#define HX_MAX_PARTITION     1023U
/* Topics whose name starts with "__" are reserved for the broker itself. */
#define HX_INTERNAL_TOPIC "__consumer_offsets"

typedef enum {
    HX_API_PING = 0,
    HX_API_PRODUCE = 1,
    HX_API_FETCH = 2,
    HX_API_METADATA = 3,
    HX_API_OFFSET_COMMIT = 4,
    HX_API_OFFSET_FETCH = 5,
    HX_API_JOIN_GROUP = 6,
    HX_API_HEARTBEAT = 7,
    HX_API_LEAVE_GROUP = 8,
    HX_API_DESCRIBE_GROUP = 9,
    HX_API_GROUP_COMMIT = 10,
    HX_API_INIT_PRODUCER = 11,
    HX_API_PRODUCE_IDEMPOTENT = 12,
    HX_API_CREATE_TOPIC = 13,
    HX_API_DELETE_TOPIC = 14,
    HX_API_CLUSTER_METADATA = 15
} hx_api;

typedef enum {
    HXE_OK = 0,
    HXE_BAD_REQUEST = 1,
    HXE_UNKNOWN_API = 2,
    HXE_UNSUPPORTED_VERSION = 3,
    HXE_INVALID_TOPIC = 4,
    HXE_OFFSET_OUT_OF_RANGE = 5,
    HXE_RECORD_TOO_LARGE = 6,
    HXE_STORAGE = 7,
    HXE_UNKNOWN_PARTITION = 8,
    HXE_RESOURCE_LIMIT = 9,
    HXE_INVALID_GROUP = 10,
    HXE_UNKNOWN_MEMBER = 11,        /* unknown group or member: rejoin with an empty member id */
    HXE_REBALANCE_IN_PROGRESS = 12, /* stop consuming, commit, and JoinGroup again */
    HXE_ILLEGAL_GENERATION = 13,    /* the group moved on to a newer generation */
    HXE_NOT_ASSIGNED = 14,          /* partition is not in this member's assignment */
    HXE_UNKNOWN_PRODUCER = 15,      /* producer id never issued, expired, or lost in a restart */
    HXE_OUT_OF_ORDER_SEQUENCE = 16, /* sequence is neither the next one nor a recent duplicate */
    HXE_TOPIC_EXISTS = 17,          /* CreateTopic on a topic that already has partitions */
    HXE_NOT_LEADER = 18,            /* replicated partition: ask ClusterMetadata for the leader */
    HXE_NOT_ENOUGH_REPLICAS =
        19,                   /* replicated partition: ISR below the minimum, nothing appended */
    HXE_REQUEST_TIMEOUT = 20, /* replicated Produce: outcome unknown, the record may still commit */
    HXE_UNSUPPORTED = 21      /* operation not available on a replicated topic */
} hx_wire_err;

const char *hx_wire_strerror(uint16_t code);

#endif /* HELIX_PROTOCOL_H */
