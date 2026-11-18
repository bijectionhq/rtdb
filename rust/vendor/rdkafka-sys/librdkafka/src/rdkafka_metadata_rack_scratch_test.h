#include "rdkafka_metadata_cache_admission_fixture.h"

int unittest_metadata_rack_scratch(void);

static void *ut_rack_scratch_malloc_denied(size_t bytes) {
        (void)bytes;
        return NULL;
}

static int ut_rack_scratch_exact_short_zero_and_malloc_failure(void) {
        struct rd_kafka_metadata_rack_scratch scratch;
        const size_t bytes = 3 * sizeof(void *);
        size_t measured = 123;
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(bytes);
        rd_kafka_byte_account_t *short_account = rd_kafka_byte_account_new(bytes - 1);
        RD_UT_ASSERT(account && short_account, "create exact scratch accounts");
        RD_UT_ASSERT(!rd_kafka_metadata_rack_scratch_size(SIZE_MAX, &measured) && measured == 123 &&
                         !rd_kafka_metadata_rack_scratch_size((size_t)INT_MAX + 1, &measured) && measured == 123,
                     "pointer-product and native list-count overflow preserve size output");
        RD_UT_ASSERT(!rd_kafka_metadata_rack_scratch_init(&scratch, short_account, 3, malloc) &&
                         !rd_kafka_byte_account_used(short_account),
                     "one-byte-short scratch denial retains no allocation charge");
        rd_kafka_metadata_rack_scratch_destroy(&scratch);
        RD_UT_ASSERT(!rd_kafka_metadata_rack_scratch_init(&scratch, account, 3, ut_rack_scratch_malloc_denied) &&
                         !rd_kafka_byte_account_used(account),
                     "fallible scratch malloc returns its prior admission");
        rd_kafka_metadata_rack_scratch_destroy(&scratch);
        RD_UT_ASSERT(rd_kafka_metadata_rack_scratch_init(&scratch, account, 0, ut_rack_scratch_malloc_denied) &&
                         !scratch.racks.rl_elems && !scratch.charge &&
                         !rd_kafka_metadata_rack_scratch_add(&scratch, "rack"),
                     "zero-capacity scratch allocates nothing and cannot grow");
        rd_kafka_metadata_rack_scratch_destroy(&scratch);
        RD_UT_ASSERT(rd_kafka_metadata_rack_scratch_init(&scratch, account, 3, malloc) &&
                         rd_kafka_byte_account_used(account) == bytes &&
                         rd_kafka_metadata_rack_scratch_add(&scratch, "c") &&
                         rd_kafka_metadata_rack_scratch_add(&scratch, "a") &&
                         rd_kafka_metadata_rack_scratch_add(&scratch, "b") &&
                         !rd_kafka_metadata_rack_scratch_add(&scratch, "overflow") &&
                         rd_list_cnt(&scratch.racks) == 3 && rd_kafka_byte_account_used(account) == bytes,
                     "exact pointer storage is admitted once and bounded appends cannot reallocate");
        rd_kafka_metadata_rack_scratch_destroy(&scratch);
        RD_UT_ASSERT(!rd_kafka_byte_account_used(account), "scratch final free releases its exact buffer charge");
        RD_UT_ASSERT(rd_kafka_metadata_rack_scratch_init(&scratch, NULL, 1, ut_rack_scratch_malloc_denied) &&
                         scratch.racks.rl_elems && !scratch.account && !scratch.charge &&
                         rd_kafka_metadata_rack_scratch_add(&scratch, "legacy"),
                     "NULL-account scratch retains native allocation policy without an account charge");
        rd_kafka_metadata_rack_scratch_destroy(&scratch);
        rd_kafka_byte_account_destroy(short_account);
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

static int ut_rack_scratch_matches_source_sort_and_representatives(void) {
        char first_a[] = "a", second_a[] = "a", first_empty[] = "", second_empty[] = "";
        char *values[] = {"z", first_a, first_empty, "m", second_a, second_empty, "z"};
        struct rd_kafka_metadata_rack_scratch scratch;
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(sizeof(values));
        rd_list_t *source = rd_list_new(0, NULL);
        int i;
        RD_UT_ASSERT(account && rd_kafka_metadata_rack_scratch_init(&scratch, account, RD_ARRAYSIZE(values), malloc),
                     "create exact source-comparison scratch");
        for (i = 0; i < (int)RD_ARRAYSIZE(values); i++) {
                rd_list_add(source, values[i]);
                RD_UT_ASSERT(rd_kafka_metadata_rack_scratch_add(&scratch, values[i]), "fill exact encounter sequence");
        }
        rd_list_deduplicate(&source, rd_strcmp2);
        rd_kafka_metadata_rack_scratch_deduplicate(&scratch);
        RD_UT_ASSERT(rd_list_cnt(&scratch.racks) == 4 && rd_list_cnt(source) == 4 &&
                         !strcmp(rd_list_elem(&scratch.racks, 0), "") &&
                         !strcmp(rd_list_elem(&scratch.racks, 1), "a") &&
                         !strcmp(rd_list_elem(&scratch.racks, 2), "m") &&
                         !strcmp(rd_list_elem(&scratch.racks, 3), "z"),
                     "in-place dedup preserves source lexical value order including empty rack text");
        for (i = 0; i < rd_list_cnt(source); i++)
                RD_UT_ASSERT(rd_list_elem(source, i) == rd_list_elem(&scratch.racks, i),
                             "equal-text representative pointers match the original native sort/dedup path");
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == sizeof(values),
                     "compaction retains the actual allocated capacity until final free");
        rd_list_destroy(source);
        rd_kafka_metadata_rack_scratch_destroy(&scratch);
        RD_UT_ASSERT(!rd_kafka_byte_account_used(account), "compacted scratch releases its full capacity");
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

static int ut_rack_scratch_output_segment_overlap(int boundary) {
        const size_t root = RD_ROUNDUP(sizeof(rd_kafka_metadata_internal_t), 8) + 8;
        const size_t scratch_bytes = 3 * sizeof(void *);
        const size_t output_bytes = RD_ROUNDUP(sizeof(struct rd_kafka_metadata_segment), 8) + sizeof(char *);
        size_t capacity = boundary == 0 ? root + scratch_bytes - 1 : root + scratch_bytes + output_bytes - (boundary == 1);
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(capacity);
        rd_tmpabuf_t arena = RD_ZERO_INIT;
        rd_kafka_metadata_internal_t *metadata;
        rd_kafka_metadata_broker_internal_t broker = {.id = 7};
        int32_t replicas[] = {7, 7, 7};
        rd_kafka_metadata_partition_t partition = {.replica_cnt = 3, .replicas = replicas};
        rd_kafka_metadata_partition_internal_t internal_partition = RD_ZERO_INIT;
        rd_kafka_metadata_topic_t topic = {.partition_cnt = 1, .partitions = &partition};
        rd_kafka_metadata_topic_internal_t internal_topic = {.partitions = &internal_partition};
        rd_bool_t populated;
        RD_UT_ASSERT(account, "create segmented rack overlap account");
        metadata = rd_kafka_metadata_arena_new(&arena, account, root, malloc);
        RD_UT_ASSERT(metadata, "admit rack source owner");
        broker.rack_id = rd_tmpabuf_write_str(&arena, "rack");
        metadata->metadata.broker_cnt = 1;
        metadata->metadata.topic_cnt = 1;
        metadata->metadata.topics = &topic;
        metadata->brokers = &broker;
        metadata->topics = &internal_topic;
        populated = rd_kafka_populate_metadata_topic_racks(&arena, 0, metadata);
        RD_UT_ASSERT(populated == (boundary == 2) &&
                         rd_kafka_byte_account_used(account) == (boundary == 2 ? root + output_bytes : root),
                     "scratch and new output segment must overlap within admission and temporary storage releases on every return");
        if (boundary == 0)
                RD_UT_ASSERT(!internal_partition.racks && !internal_partition.racks_cnt && !metadata->segments,
                             "scratch denial leaves the current partition mapping untouched");
        else if (boundary == 1)
                RD_UT_ASSERT(!internal_partition.racks && internal_partition.racks_cnt == 1 && !metadata->segments,
                             "output denial retains source count-before-pointer mutation and returns the scratch charge");
        else
                RD_UT_ASSERT(metadata->segments && internal_partition.racks_cnt == 1 && internal_partition.racks &&
                                 internal_partition.racks[0] == broker.rack_id && !strcmp(internal_partition.racks[0], "rack"),
                             "output segment borrows rack text owned by its retained parsed root");
        rd_kafka_metadata_destroy(&metadata->metadata);
        RD_UT_ASSERT(!rd_kafka_byte_account_used(account), "root final destruction releases output segment and source storage");
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

static int ut_rack_scratch_zero_matching_geometry(void) {
        const size_t root = RD_ROUNDUP(sizeof(rd_kafka_metadata_internal_t), 8);
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(root);
        rd_tmpabuf_t arena = RD_ZERO_INIT;
        rd_kafka_metadata_internal_t *metadata;
        rd_kafka_metadata_broker_internal_t broker = {.id = 7};
        int32_t replicas[] = {7, 8};
        rd_kafka_metadata_partition_t partitions[2] = {{.replica_cnt = 2, .replicas = replicas}, {0}};
        rd_kafka_metadata_partition_internal_t internal_partitions[2] = RD_ZERO_INIT;
        rd_kafka_metadata_topic_t topic = {.partition_cnt = 2, .partitions = partitions};
        rd_kafka_metadata_topic_internal_t internal_topic = {.partitions = internal_partitions};
        RD_UT_ASSERT(account, "create a full source-only account");
        metadata = rd_kafka_metadata_arena_new(&arena, account, root, malloc);
        RD_UT_ASSERT(metadata, "admit root with no remaining capacity");
        metadata->metadata.broker_cnt = 1;
        metadata->metadata.topic_cnt = 1;
        metadata->metadata.topics = &topic;
        metadata->brokers = &broker;
        metadata->topics = &internal_topic;
        RD_UT_ASSERT(rd_kafka_populate_metadata_topic_racks(&arena, 0, metadata) &&
                         internal_partitions[0].racks && !internal_partitions[0].racks_cnt &&
                         !internal_partitions[1].racks && !internal_partitions[1].racks_cnt &&
                         !metadata->segments && rd_kafka_byte_account_used(account) == root,
                     "unknown and rackless replicas require no scratch and preserve present-empty versus zero-replica absence");
        rd_kafka_metadata_destroy(&metadata->metadata);
        RD_UT_ASSERT(!rd_kafka_byte_account_used(account), "zero-match mapping retains no separate owner");
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

static int ut_rack_scratch_borrows_segment_owned_rack_text(void) {
        const size_t root = RD_ROUNDUP(sizeof(rd_kafka_metadata_internal_t), 8);
        const size_t segment_header = RD_ROUNDUP(sizeof(struct rd_kafka_metadata_segment), 8);
        const size_t source_bytes = segment_header + 5;
        const size_t output_bytes = segment_header + sizeof(char *);
        const size_t scratch_bytes = 3 * sizeof(void *);
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(root + source_bytes + output_bytes + scratch_bytes);
        rd_tmpabuf_t arena = RD_ZERO_INIT;
        rd_kafka_metadata_internal_t *metadata;
        rd_kafka_metadata_broker_internal_t broker = {.id = 7};
        int32_t replicas[] = {7, 7, 7};
        rd_kafka_metadata_partition_t partition = {.replica_cnt = 3, .replicas = replicas};
        rd_kafka_metadata_partition_internal_t internal_partition = RD_ZERO_INIT;
        rd_kafka_metadata_topic_t topic = {.partition_cnt = 1, .partitions = &partition};
        rd_kafka_metadata_topic_internal_t internal_topic = {.partitions = &internal_partition};
        RD_UT_ASSERT(account, "create exact root/source-segment/scratch/output-segment overlap account");
        metadata = rd_kafka_metadata_arena_new(&arena, account, root, malloc);
        RD_UT_ASSERT(metadata, "admit the full parsed root");
        broker.rack_id = rd_kafka_metadata_alloc(&arena, metadata, 5);
        RD_UT_ASSERT(broker.rack_id && metadata->segments, "retain rack text in its own admitted source segment");
        memcpy(broker.rack_id, "rack", 5);
        metadata->metadata.broker_cnt = 1;
        metadata->metadata.topic_cnt = 1;
        metadata->metadata.topics = &topic;
        metadata->brokers = &broker;
        metadata->topics = &internal_topic;
        RD_UT_ASSERT(rd_kafka_populate_metadata_topic_racks(&arena, 0, metadata) &&
                         internal_partition.racks_cnt == 1 && internal_partition.racks &&
                         internal_partition.racks[0] == broker.rack_id &&
                         !strcmp(internal_partition.racks[0], "rack") &&
                         rd_kafka_byte_account_used(account) == root + source_bytes + output_bytes,
                     "scratch release retains exact borrowed segment-owned text and its independent output segment");
        rd_kafka_metadata_destroy(&metadata->metadata);
        RD_UT_ASSERT(!rd_kafka_byte_account_used(account), "parsed owner final free releases both retained segment extents");
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

static const unsigned char ut_rack_response_first[] = {
        0, 0, 0, 1, 0, 0, 0, 7, 0, 9, 49, 50, 55, 46, 48, 46,
        48, 46, 49, 0, 0, 0, 0, 0, 4, 114, 97, 99, 107, 255, 255, 255,
        255, 0, 0, 0, 2, 0, 0, 0, 5, 102, 105, 114, 115, 116, 0, 0,
        0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0,
        32, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0,
        7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0,
        7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0,
        7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0,
        7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0,
        7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0,
        7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0,
        7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0,
        7, 0, 0, 0, 0, 0, 0, 0, 6, 115, 101, 99, 111, 110, 100, 0,
        0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0,
        0, 32, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0,
        0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0,
        0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0,
        0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0,
        0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0,
        0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0,
        0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0,
        0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0,
        0, 7, 0, 0, 0, 0,
};

static const unsigned char ut_rack_response_later[] = {
        0, 0, 0, 1, 0, 0, 0, 7, 0, 9, 49, 50, 55, 46, 48, 46,
        48, 46, 49, 0, 0, 0, 0, 0, 4, 114, 97, 99, 107, 255, 255, 255,
        255, 0, 0, 0, 2, 0, 0, 0, 5, 102, 105, 114, 115, 116, 0, 0,
        0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 6, 115, 101, 99, 111, 110, 100, 0,
        0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0,
        0, 32, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0,
        0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0,
        0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0,
        0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0,
        0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0,
        0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0,
        0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0,
        0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0, 0, 7, 0, 0,
        0, 7, 0, 0, 0, 0,
};

static int ut_rack_broker_id_cmp(const void *left, const void *right) {
        const rd_kafka_broker_t *a = left, *b = right;
        return RD_CMP(a->rkb_nodeid, b->rkb_nodeid);
}

static void ut_rack_parser_client_init(struct ut_cache_admission_client *fixture,
                                       rd_kafka_byte_account_t *account) {
        rd_kafka_t *rk;
        rd_kafka_broker_t *broker;
        ut_cache_admission_client_init(fixture, account);
        rk = &fixture->rk;
        broker = &fixture->broker;
        rk->rk_conf.client_rack = rd_kafkap_str_new("client", -1);
        rd_refcnt_init(&broker->rkb_refcnt, 2);
        broker->rkb_nodeid = 7;
        broker->rkb_source = RD_KAFKA_LEARNED;
        rd_strlcpy(broker->rkb_nodename, "127.0.0.1:0", sizeof(broker->rkb_nodename));
        rd_kafka_wrlock(rk);
        rd_list_init(&rk->rk_broker_by_id, 1, NULL);
        rd_list_add(&rk->rk_broker_by_id, broker);
        rd_list_sort(&rk->rk_broker_by_id, ut_rack_broker_id_cmp);
        TAILQ_INSERT_TAIL(&rk->rk_brokers, broker, rkb_link);
        rd_atomic32_set(&rk->rk_broker_cnt, 1);
        rd_list_init(&rk->wait_decommissioned_brokers, 0, NULL);
        rd_list_init(&rk->wait_decommissioned_thrds, 0, NULL);
        rd_kafka_wrunlock(rk);
}

static rd_bool_t ut_rack_parser_client_destroy(struct ut_cache_admission_client *fixture) {
        rd_kafka_t *rk = &fixture->rk;
        rd_kafka_broker_t *broker = &fixture->broker;
        int references, first, last;
        rd_bool_t idle;
        rd_kafka_wrlock(rk);
        idle = rd_list_cnt(&rk->wait_decommissioned_brokers) == 0 &&
            rd_list_cnt(&rk->wait_decommissioned_thrds) == 0 &&
            rd_list_cnt(&rk->rk_broker_by_id) == 1 &&
            rd_list_elem(&rk->rk_broker_by_id, 0) == broker;
        TAILQ_REMOVE(&rk->rk_brokers, broker, rkb_link);
        rd_list_remove(&rk->rk_broker_by_id, broker);
        rd_atomic32_set(&rk->rk_broker_cnt, 0);
        rd_list_destroy(&rk->rk_broker_by_id);
        rd_list_destroy(&rk->wait_decommissioned_brokers);
        rd_list_destroy(&rk->wait_decommissioned_thrds);
        rd_kafka_wrunlock(rk);
        references = rd_refcnt_get(&broker->rkb_refcnt);
        first = rd_refcnt_sub(&broker->rkb_refcnt);
        last = rd_refcnt_sub(&broker->rkb_refcnt);
        rd_refcnt_destroy(&broker->rkb_refcnt);
        rd_kafkap_str_destroy(rk->rk_conf.client_rack);
        rk->rk_conf.client_rack = NULL;
        ut_cache_admission_client_destroy(fixture);
        return idle && references == 2 && first == 1 && last == 0;
}

static rd_kafka_buf_t *ut_rack_parser_response(rd_bool_t later) {
        rd_kafka_buf_t *response = rd_kafka_buf_new_shadow(
            later ? ut_rack_response_later : ut_rack_response_first,
            later ? sizeof(ut_rack_response_later) : sizeof(ut_rack_response_first), NULL);
        response->rkbuf_reqhdr.ApiVersion = 1;
        return response;
}

static size_t ut_rack_decoded_charge(const rd_kafka_metadata_internal_t *metadata) {
        size_t total = metadata->byte_charge;
        const size_t header = RD_ROUNDUP(sizeof(struct rd_kafka_metadata_segment), 8);
        struct rd_kafka_metadata_segment *segment;
        for (segment = metadata->segments; segment; segment = segment->next) {
                int i, j;
                rd_bool_t rack_output = rd_false;
                void *payload = (char *)segment + header;
                for (i = 0; i < metadata->metadata.topic_cnt; i++)
                        for (j = 0; j < metadata->metadata.topics[i].partition_cnt; j++) {
                                const rd_kafka_metadata_partition_internal_t *part = &metadata->topics[i].partitions[j];
                                if (part->racks_cnt && (void *)part->racks == payload)
                                        rack_output = rd_true;
                        }
                if (!rack_output)
                        total += segment->charge;
        }
        return total;
}

static rd_bool_t ut_rack_parser_measure(rd_bool_t later, size_t *decoded, size_t *first_cache) {
        struct ut_cache_admission_client fixture;
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(1 << 20);
        rd_kafka_metadata_internal_t *metadata = NULL;
        rd_kafka_buf_t *response;
        rd_kafka_resp_err_t error;
        rd_bool_t measured = rd_false, released;
        if (!account)
                return rd_false;
        ut_rack_parser_client_init(&fixture, account);
        response = ut_rack_parser_response(later);
        error = rd_kafka_parse_Metadata_admin(&fixture.broker, response, NULL, &metadata);
        if (!error && metadata && metadata->metadata.topic_cnt == 2 &&
            metadata->metadata.topics && metadata->topics &&
            metadata->metadata.topics[0].partition_cnt == 1 &&
            metadata->metadata.topics[1].partition_cnt == 1 &&
            metadata->topics[1].partitions &&
            metadata->topics[1].partitions[0].racks_cnt == 1 &&
            metadata->topics[1].partitions[0].racks &&
            !strcmp(metadata->topics[1].partitions[0].racks[0], "rack")) {
                const struct rd_kafka_metadata_cache_entry *entry;
                *decoded = ut_rack_decoded_charge(metadata);
                rd_kafka_rdlock(&fixture.rk);
                entry = rd_kafka_metadata_cache_find(&fixture.rk, "first", 1);
                if (entry) {
                        *first_cache = entry->rkmce_byte_charge;
                        measured = *decoded > 0 && *first_cache > 0;
                }
                rd_kafka_rdunlock(&fixture.rk);
        }
        rd_kafka_metadata_destroy(metadata ? &metadata->metadata : NULL);
        rd_kafka_buf_destroy(response);
        released = ut_rack_parser_client_destroy(&fixture);
        if (rd_kafka_byte_account_used(account))
                return rd_false;
        rd_kafka_byte_account_destroy(account);
        return measured && released;
}

static int ut_rack_scratch_actual_parser_denial(rd_bool_t admin, rd_bool_t later, rd_bool_t hint) {
        struct ut_cache_admission_client fixture;
        const size_t scratch_bytes = 32 * sizeof(void *);
        size_t decoded = 0, first_cache = 0;
        size_t old_bytes = ut_cache_admission_empty_size("second");
        size_t hint_bytes = hint ? ut_cache_admission_empty_size("hint") : 0;
        rd_kafka_byte_account_t *account;
        rd_kafka_metadata_topic_t old_topic = {.topic = "second", .err = RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED};
        rd_kafka_metadata_topic_internal_t old_internal = RD_ZERO_INIT;
        struct rd_kafka_metadata_cache_entry *old, *current;
        rd_kafka_metadata_internal_t *metadata = NULL;
        rd_kafka_buf_t request = RD_ZERO_INIT, *response;
        rd_list_t topics, hints;
        rd_kafka_q_t *notifications;
        rd_kafka_enq_once_t *observer;
        rd_kafka_op_t *notice, *pending, *extra;
        rd_kafka_resp_err_t error;
        rd_ts_t inserted, expires;
        rd_bool_t preserved, notified, released;
        RD_UT_ASSERT(ut_rack_parser_measure(later, &decoded, &first_cache),
                     "measure actual decoded root/segments and prior cache storage without assuming arena geometry");
        account = rd_kafka_byte_account_new(decoded + old_bytes + hint_bytes +
                    (later ? first_cache : 0) + scratch_bytes - 1);
        RD_UT_ASSERT(account, "create exact first/later rack scratch denial boundary");
        ut_rack_parser_client_init(&fixture, account);
        rd_list_init(&topics, 3, NULL);
        rd_list_add(&topics, "first");
        rd_list_add(&topics, "second");
        rd_list_add(&topics, "hint");
        rd_list_init(&hints, 1, NULL);
        rd_list_add(&hints, "hint");
        rd_kafka_wrlock(&fixture.rk);
        RD_UT_ASSERT(rd_kafka_metadata_cache_topic_update(&fixture.rk, &old_topic, &old_internal,
                         rd_false, rd_false, rd_false) == RD_KAFKA_METADATA_CACHE_CHANGED,
                     "install retained current-topic cache generation");
        old = rd_kafka_metadata_cache_find(&fixture.rk, "second", 1);
        RD_UT_ASSERT(old, "retain the installed current-topic cache generation");
        inserted = old->rkmce_ts_insert;
        expires = old->rkmce_ts_expires;
        if (hint)
                RD_UT_ASSERT(rd_kafka_metadata_cache_hint(&fixture.rk, &hints, NULL, RD_KAFKA_RESP_ERR__NOENT) == 1,
                             "install requested failure-cleanup hint");
        rd_kafka_wrunlock(&fixture.rk);
        notifications = rd_kafka_q_new(&fixture.rk);
        observer = rd_kafka_enq_once_new(rd_kafka_op_new(RD_KAFKA_OP_WAKEUP), RD_KAFKA_REPLYQ(notifications, 0));
        rd_kafka_wrlock(&fixture.rk);
        rd_kafka_metadata_cache_wait_state_change_async(&fixture.rk, observer);
        rd_kafka_wrunlock(&fixture.rk);
        response = ut_rack_parser_response(later);
        request.rkbuf_u.Metadata.topics = &topics;
        request.rkbuf_u.Metadata.cgrp_subscription_version = -1;
        error = admin ? rd_kafka_parse_Metadata_admin(&fixture.broker, response, &topics, &metadata) :
                        rd_kafka_parse_Metadata(&fixture.broker, &request, response, &metadata);
        rd_kafka_rdlock(&fixture.rk);
        current = rd_kafka_metadata_cache_find(&fixture.rk, "second", 1);
        preserved = current && current == old && current->rkmce_ts_insert == inserted && current->rkmce_ts_expires == expires &&
            current->rkmce_mtopic.err == RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED &&
            !!rd_kafka_metadata_cache_find(&fixture.rk, "first", 1) == !!later &&
            !rd_kafka_metadata_cache_find(&fixture.rk, "hint", 0) &&
            fixture.rk.rk_metadata_cache.rkmc_cnt == 1 + !!later &&
            fixture.rk.rk_ts_metadata == 123 && fixture.rk.rk_ts_full_metadata == 456 &&
            rd_kafka_byte_account_used(account) == old_bytes + (later ? first_cache : 0) &&
            !!rd_kafka_timer_is_started(&fixture.rk.rk_timers,
                &fixture.rk.rk_metadata_cache.rkmc_expiry_tmr) == !!later;
        rd_kafka_rdunlock(&fixture.rk);
        notice = rd_kafka_q_pop(notifications, RD_POLL_NOWAIT, 0);
        notified = (later || hint) ? (notice && notice->rko_err == RD_KAFKA_RESP_ERR_NO_ERROR) : !notice;
        if (notice)
                rd_kafka_op_destroy(notice);
        extra = rd_kafka_q_pop(notifications, RD_POLL_NOWAIT, 0);
        notified = notified && !extra;
        if (extra)
                rd_kafka_op_destroy(extra);
        pending = rd_kafka_enq_once_disable(observer);
        if (pending)
                rd_kafka_op_destroy(pending);
        rd_kafka_q_destroy_owner(notifications);
        RD_UT_ASSERT(error == RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE && !metadata &&
                         response->rkbuf_err == RD_KAFKA_RESP_ERR_NO_ERROR && preserved && notified,
                     "actual ordinary/admin parser denial precedes current-topic mutation, preserves earlier cache work and returns only the original resource error");
        rd_kafka_buf_destroy(response);
        rd_list_destroy(&hints);
        rd_list_destroy(&topics);
        released = ut_rack_parser_client_destroy(&fixture);
        RD_UT_ASSERT(released && !rd_kafka_byte_account_used(account),
                     "indexed borrowed broker references and all parsed/cache owners finish before account teardown");
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

int unittest_metadata_rack_scratch(void) {
        int failures = ut_rack_scratch_exact_short_zero_and_malloc_failure() +
            ut_rack_scratch_matches_source_sort_and_representatives() +
            ut_rack_scratch_output_segment_overlap(0) + ut_rack_scratch_output_segment_overlap(1) +
            ut_rack_scratch_output_segment_overlap(2) + ut_rack_scratch_zero_matching_geometry() +
            ut_rack_scratch_borrows_segment_owned_rack_text();
        int admin, later, hint;
        for (admin = 0; admin < 2; admin++)
                for (later = 0; later < 2; later++)
                        for (hint = 0; hint < 2; hint++)
                                failures += ut_rack_scratch_actual_parser_denial(admin, later, hint);
        return failures;
}
