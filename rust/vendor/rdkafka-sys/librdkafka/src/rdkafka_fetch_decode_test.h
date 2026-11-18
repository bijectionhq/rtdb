int unittest_fetch_decode_admission(void);

static void *ut_fetch_decode_no_malloc(size_t size) {
        (void)size;
        return NULL;
}

static int ut_fetch_decode_malloc_remaining;

static void *ut_fetch_decode_bounded_malloc(size_t size) {
        if (!ut_fetch_decode_malloc_remaining--)
                return NULL;
        return malloc(size);
}

static int ut_fetch_decode_output_admission(void) {
        size_t bytes;
        size_t unchanged = 71;
        void *payload;
        rd_kafka_byte_account_t *exact, *short_account, *empty;
        RD_UT_ASSERT(rd_kafka_fetch_decode_size(9, &bytes), "measure nine-byte output owner");
        exact = rd_kafka_byte_account_new(bytes);
        short_account = rd_kafka_byte_account_new(bytes - 1);
        empty = rd_kafka_byte_account_new(sizeof(rd_kafka_fetch_decode_owner_t));
        RD_UT_ASSERT(exact && short_account && empty, "create bounded output accounts");
        RD_UT_ASSERT(!rd_kafka_fetch_decode_size(SIZE_MAX, &unchanged) && unchanged == 71,
                     "output size overflow leaves its size result untouched");
        RD_UT_ASSERT(!rd_kafka_fetch_decode_alloc(short_account, 9, malloc) &&
                         !rd_kafka_byte_account_used(short_account),
                     "one-byte-short output admission returns no storage or charge");
        RD_UT_ASSERT(!rd_kafka_fetch_decode_alloc(exact, 9, ut_fetch_decode_no_malloc) &&
                         !rd_kafka_byte_account_used(exact),
                     "output malloc failure returns the acquired charge");
        RD_UT_ASSERT(!rd_kafka_fetch_decode_alloc(exact, SIZE_MAX, malloc) &&
                         !rd_kafka_byte_account_used(exact),
                     "overflow is rejected before allocation and admission");
        payload = rd_kafka_fetch_decode_alloc(exact, 9, malloc);
        RD_UT_ASSERT(payload && rd_kafka_byte_account_used(exact) == bytes,
                     "exact output admission retains prefix and physical capacity");
        memcpy(payload, "123456789", 9);
        rd_kafka_fetch_decode_free(payload);
        RD_UT_ASSERT(!rd_kafka_byte_account_used(exact), "output final free returns its complete charge");
        payload = rd_kafka_fetch_decode_alloc(empty, 0, malloc);
        RD_UT_ASSERT(payload && rd_kafka_byte_account_used(empty) == sizeof(rd_kafka_fetch_decode_owner_t),
                     "zero decoded bytes still own a positive charged prefix");
        rd_kafka_fetch_decode_free(payload);
        RD_UT_ASSERT(!rd_kafka_byte_account_used(empty), "zero-length output prefix is released");
        rd_kafka_byte_account_destroy(empty);
        rd_kafka_byte_account_destroy(short_account);
        rd_kafka_byte_account_destroy(exact);
        RD_UT_PASS();
}

static int ut_fetch_decode_shadow_failure(void) {
        size_t payload_bytes;
        size_t metadata_bytes = rd_kafka_fetch_decode_shadow_size();
        rd_kafka_byte_account_t *account;
        void *payload;
        int fail_after;
        RD_UT_ASSERT(rd_kafka_fetch_decode_size(3, &payload_bytes), "measure retained output");
        account = rd_kafka_byte_account_new(payload_bytes + metadata_bytes - 1);
        RD_UT_ASSERT(account, "create one-byte-short shadow account");
        payload = rd_kafka_fetch_decode_alloc(account, 3, malloc);
        RD_UT_ASSERT(payload && !rd_kafka_fetch_decode_shadow(payload, 3, malloc) &&
                         rd_kafka_byte_account_used(account) == payload_bytes,
                     "metadata denial leaves the caller's admitted output intact");
        rd_kafka_fetch_decode_free(payload);
        rd_kafka_byte_account_destroy(account);
        for (fail_after = 0; fail_after <= 1; fail_after++) {
                account = rd_kafka_byte_account_new(payload_bytes + metadata_bytes);
                RD_UT_ASSERT(account, "create exact output and shadow account");
                payload = rd_kafka_fetch_decode_alloc(account, 3, malloc);
                RD_UT_ASSERT(payload, "admit output before shadow allocation");
                memcpy(payload, "abc", 3);
                ut_fetch_decode_malloc_remaining = fail_after;
                RD_UT_ASSERT(!rd_kafka_fetch_decode_shadow(
                                 payload, 3, ut_fetch_decode_bounded_malloc) &&
                                 rd_kafka_byte_account_used(account) == payload_bytes &&
                                 !memcmp(payload, "abc", 3),
                             "either shadow malloc failure returns metadata storage and charge without consuming output");
                rd_kafka_fetch_decode_free(payload);
                RD_UT_ASSERT(!rd_kafka_byte_account_used(account), "caller can release output after shadow failure");
                rd_kafka_byte_account_destroy(account);
        }
        RD_UT_PASS();
}

static int ut_fetch_decode_final_fetch_owner(void) {
        size_t payload_bytes;
        size_t charge;
        rd_kafka_byte_account_t *account;
        rd_kafka_buf_t *shadow;
        rd_kafka_op_t *first, *last;
        void *payload;
        RD_UT_ASSERT(rd_kafka_fetch_decode_size(7, &payload_bytes), "measure shared output");
        charge = payload_bytes + rd_kafka_fetch_decode_shadow_size();
        account = rd_kafka_byte_account_new(charge);
        RD_UT_ASSERT(account, "create exact shared-frame account");
        payload = rd_kafka_fetch_decode_alloc(account, 7, malloc);
        RD_UT_ASSERT(payload, "admit shared output bytes");
        memcpy(payload, "payload", 7);
        shadow = rd_kafka_fetch_decode_shadow(payload, 7, malloc);
        RD_UT_ASSERT(shadow && rd_kafka_byte_account_used(account) == charge &&
                         shadow->rkbuf_totlen == 7 &&
                         shadow->rkbuf_buf.rbuf_extra_len == sizeof(rd_segment_t),
                     "shadow uses exactly its admitted root and preallocated segment");
        first = rd_kafka_op_new(RD_KAFKA_OP_FETCH);
        last = rd_kafka_op_new(RD_KAFKA_OP_FETCH);
        first->rko_evtype = RD_KAFKA_EVENT_FETCH;
        last->rko_evtype = RD_KAFKA_EVENT_FETCH;
        first->rko_u.fetch.rkbuf = shadow;
        last->rko_u.fetch.rkbuf = shadow;
        first->rko_u.fetch.rkm.rkm_payload = payload;
        first->rko_u.fetch.rkm.rkm_len = 7;
        last->rko_u.fetch.rkm.rkm_payload = payload;
        last->rko_u.fetch.rkm.rkm_len = 7;
        rd_kafka_buf_keep(shadow);
        rd_kafka_buf_keep(shadow);
        rd_kafka_buf_destroy(shadow);
        rd_kafka_event_destroy(first);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == charge &&
                         !memcmp(payload, "payload", 7),
                     "all output and metadata charges survive through the last Fetch op");
        rd_kafka_event_destroy(last);
        RD_UT_ASSERT(!rd_kafka_byte_account_used(account), "last Fetch op releases both storage owners");
        payload = rd_kafka_fetch_decode_alloc(account, 0, malloc);
        RD_UT_ASSERT(payload, "admit an empty decoded owner");
        shadow = rd_kafka_fetch_decode_shadow(payload, 0, malloc);
        RD_UT_ASSERT(shadow && rd_kafka_byte_account_used(account) ==
                                  sizeof(rd_kafka_fetch_decode_owner_t) + rd_kafka_fetch_decode_shadow_size(),
                     "empty shadow still retains its output prefix and metadata");
        rd_kafka_buf_destroy(shadow);
        RD_UT_ASSERT(!rd_kafka_byte_account_used(account), "zero-length segment invokes its output finalizer");
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

#if WITH_SNAPPY
static int ut_fetch_snappy_raw_demand(void) {
        const unsigned char raw[] = {3, 8, 'a', 'b', 'c'};
        const unsigned char bad_header[] = {0x80};
        const unsigned char bad_body[] = {3, 1};
        const unsigned char empty[] = {0};
        size_t bytes;
        size_t output_size = 77;
        void *output = NULL;
        char error[128];
        rd_kafka_byte_account_t *account, *denied;
        RD_UT_ASSERT(rd_kafka_fetch_decode_size(3, &bytes), "measure raw Snappy output");
        account = rd_kafka_byte_account_new(bytes);
        denied = rd_kafka_byte_account_new(0);
        RD_UT_ASSERT(account && denied, "create raw Snappy accounts");
        RD_UT_ASSERT(rd_kafka_fetch_snappy(denied, bad_header, sizeof(bad_header),
                                          &output, &output_size, error, sizeof(error), malloc) ==
                         RD_KAFKA_RESP_ERR__BAD_COMPRESSION && !output && output_size == 77,
                     "invalid raw length is diagnosed before output admission");
        RD_UT_ASSERT(rd_kafka_fetch_snappy(denied, bad_body, sizeof(bad_body),
                                          &output, &output_size, error, sizeof(error), malloc) ==
                         RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE && !output && output_size == 77,
                     "valid raw length demands output before payload validation");
        RD_UT_ASSERT(rd_kafka_fetch_snappy(account, bad_body, sizeof(bad_body),
                                          &output, &output_size, error, sizeof(error), malloc) ==
                         RD_KAFKA_RESP_ERR__BAD_COMPRESSION && !rd_kafka_byte_account_used(account),
                     "raw payload validation failure returns admitted output");
        RD_UT_ASSERT(rd_kafka_fetch_snappy(account, raw, sizeof(raw), &output, &output_size,
                                          error, sizeof(error), ut_fetch_decode_no_malloc) ==
                         RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE && !rd_kafka_byte_account_used(account),
                     "raw output malloc failure has resource error and exact rollback");
        RD_UT_ASSERT(!rd_kafka_fetch_snappy(account, raw, sizeof(raw), &output, &output_size,
                                           error, sizeof(error), malloc) && output_size == 3 &&
                         !memcmp(output, "abc", 3) && rd_kafka_byte_account_used(account) == bytes,
                     "raw Snappy output matches the pinned literal encoding");
        rd_kafka_fetch_decode_free(output);
        output = NULL;
        RD_UT_ASSERT(!rd_kafka_fetch_snappy(account, empty, sizeof(empty), &output, &output_size,
                                           error, sizeof(error), malloc) && output && !output_size &&
                         rd_kafka_byte_account_used(account) == sizeof(rd_kafka_fetch_decode_owner_t),
                     "valid empty raw Snappy retains its prefix until release");
        rd_kafka_fetch_decode_free(output);
        RD_UT_ASSERT(!rd_kafka_byte_account_used(account) && !rd_kafka_byte_account_used(denied),
                     "raw decode leaves no outstanding storage");
        rd_kafka_byte_account_destroy(denied);
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

static int ut_fetch_snappy_java_demand(void) {
        const unsigned char framed[] = {
            0x82, 'S', 'N', 'A', 'P', 'P', 'Y', 0,
            0xff, 0xff, 0xff, 0xff, 0x42, 0x42, 0x42, 0x42,
            0, 0, 0, 5, 3, 8, 'a', 'b', 'c',
            0, 0, 0, 5, 3, 8, 'x', 'y', 'z'};
        const unsigned char bad_length[] = {0, 0, 0, 5, 3};
        const unsigned char bad_second_body[] = {
            0, 0, 0, 5, 3, 8, 'a', 'b', 'c', 0, 0, 0, 2, 3, 1};
        const unsigned char empty[] = {0, 0, 0, 1, 0};
        unsigned char trailing[sizeof(framed) + 1];
        size_t bytes;
        size_t output_size = 91;
        size_t legacy_size;
        void *output = NULL;
        char *legacy;
        char error[128], legacy_error[128];
        rd_kafka_byte_account_t *account, *denied;
        RD_UT_ASSERT(rd_kafka_fetch_decode_size(6, &bytes), "measure multi-chunk output");
        account = rd_kafka_byte_account_new(bytes);
        denied = rd_kafka_byte_account_new(0);
        RD_UT_ASSERT(account && denied, "create framed Snappy accounts");
        RD_UT_ASSERT(rd_kafka_fetch_snappy_java(denied, (const char *)bad_length, sizeof(bad_length),
                                               &output, &output_size, error, sizeof(error), malloc) ==
                         RD_KAFKA_RESP_ERR__BAD_COMPRESSION && !output && output_size == 91,
                     "all chunk bounds are checked before admission");
        RD_UT_ASSERT(rd_kafka_fetch_snappy_java(denied, (const char *)bad_second_body, sizeof(bad_second_body),
                                               &output, &output_size, error, sizeof(error), malloc) ==
                         RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE,
                     "two-pass framing admits aggregate output before validating chunk bodies");
        RD_UT_ASSERT(rd_kafka_fetch_snappy_java(account, (const char *)bad_second_body, sizeof(bad_second_body),
                                               &output, &output_size, error, sizeof(error), malloc) ==
                         RD_KAFKA_RESP_ERR__BAD_COMPRESSION && !rd_kafka_byte_account_used(account),
                     "invalid second body returns output after the first body decoded");
        RD_UT_ASSERT(rd_kafka_fetch_snappy_java(denied, (const char *)empty, sizeof(empty),
                                               &output, &output_size, error, sizeof(error), malloc) ==
                         RD_KAFKA_RESP_ERR__BAD_COMPRESSION && !strcmp(error, "Empty Snappy-java framed data"),
                     "zero total framed output is rejected before admission");
        memcpy(trailing, framed, sizeof(framed));
        trailing[sizeof(framed)] = 0;
        RD_UT_ASSERT(rd_kafka_fetch_snappy(denied, trailing, sizeof(trailing), &output, &output_size,
                                          error, sizeof(error), malloc) == RD_KAFKA_RESP_ERR__BAD_COMPRESSION,
                     "trailing Xerial bytes are rejected in the measuring pass");
        RD_UT_ASSERT(!rd_kafka_fetch_snappy(account, framed, sizeof(framed), &output, &output_size,
                                           error, sizeof(error), malloc) && output_size == 6 &&
                         !memcmp(output, "abcxyz", 6) && rd_kafka_byte_account_used(account) == bytes,
                     "Xerial detection ignores version fields and concatenates chunk encounter order");
        legacy = rd_kafka_snappy_java_uncompress((const char *)framed + 16, sizeof(framed) - 16,
                                                &legacy_size, legacy_error, sizeof(legacy_error));
        RD_UT_ASSERT(legacy && legacy_size == output_size && !memcmp(legacy, output, output_size),
                     "accounted output equals the original native framing decoder");
        rd_free(legacy);
        rd_kafka_fetch_decode_free(output);
        RD_UT_ASSERT(!rd_kafka_byte_account_used(account) && !rd_kafka_byte_account_used(denied),
                     "framed decode releases its complete output owner");
        rd_kafka_byte_account_destroy(denied);
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}
#endif

#include "rdkafka_fetch_gzip_test.h"

int unittest_fetch_decode_admission(void) {
        int fails = 0;
        fails += ut_fetch_decode_output_admission();
        fails += ut_fetch_decode_shadow_failure();
        fails += ut_fetch_decode_final_fetch_owner();
#if WITH_SNAPPY
        fails += ut_fetch_snappy_raw_demand();
        fails += ut_fetch_snappy_java_demand();
#endif
#if WITH_ZLIB
        fails += ut_fetch_gzip_allocator_boundaries();
        fails += ut_fetch_gzip_matches_terminating_source();
        fails += ut_fetch_gzip_partial_input_terminates_without_progress();
        fails += ut_fetch_gzip_overlap_and_each_malloc_failure();
        fails += ut_fetch_gzip_final_event_retains_the_extra_output_byte();
#endif
        return fails;
}
