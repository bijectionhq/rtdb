#include "rdunittest.h"

int unittest_metadata_account(void);

static void *ut_metadata_allocation_denied(size_t size) {
        (void)size;
        return NULL;
}

static int ut_metadata_arena_admission(void) {
        size_t size, unchanged = 19;
        rd_tmpabuf_t tbuf = RD_ZERO_INIT;
        rd_kafka_metadata_internal_t *mdi;
        rd_kafka_byte_account_t *account;
        RD_UT_ASSERT(offsetof(rd_kafka_metadata_internal_t, metadata) == 0,
                     "public metadata remains the arena prefix");
        RD_UT_ASSERT(rd_kafka_metadata_arena_size(2, 13, rd_false, &size) &&
                         size == RD_ROUNDUP(sizeof(*mdi), 8) + 8 + 56,
                     "charge equals the aligned allocation request");
        RD_UT_ASSERT(!rd_kafka_metadata_arena_size(SIZE_MAX, 1, rd_false, &unchanged) &&
                         unchanged == 19 &&
                         !rd_kafka_metadata_arena_size(1, SIZE_MAX, rd_true, &unchanged),
                     "alignment and wire-factor overflow reject before allocation");
        account = rd_kafka_byte_account_new(size - 1);
        RD_UT_ASSERT(account && !rd_kafka_metadata_arena_new(&tbuf, account, size, malloc) &&
                         !tbuf.buf && rd_kafka_byte_account_used(account) == 0,
                     "one-byte-short admission leaves no allocation or charge");
        rd_kafka_byte_account_destroy(account);
        account = rd_kafka_byte_account_new(size);
        RD_UT_ASSERT(account && !rd_kafka_metadata_arena_new(&tbuf, account, size,
                                                            ut_metadata_allocation_denied) &&
                         !tbuf.buf && rd_kafka_byte_account_used(account) == 0,
                     "allocator denial rolls the admitted charge back");
        mdi = rd_kafka_metadata_arena_new(&tbuf, account, size, malloc);
        RD_UT_ASSERT(mdi && mdi->byte_account == account && mdi->byte_charge == size &&
                         rd_kafka_byte_account_used(account) == size,
                     "exact capacity retains the arena charge in its root");
        rd_kafka_metadata_destroy(&mdi->metadata);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == 0,
                     "public final free returns the arena charge");
        RD_UT_ASSERT(!rd_kafka_metadata_arena_new(&tbuf, account, size - 1, malloc),
                     "unaligned allocation size is not admitted");
        rd_kafka_metadata_destroy(NULL);
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

static int ut_metadata_segments_admit_exact_storage(void) {
        const size_t root = RD_ROUNDUP(sizeof(rd_kafka_metadata_internal_t), 8);
        const size_t header = RD_ROUNDUP(sizeof(struct rd_kafka_metadata_segment), 8);
        const char name[] = "metadata";
        rd_kafka_byte_account_t *account =
            rd_kafka_byte_account_new(root + header + sizeof(name));
        rd_kafka_metadata_internal_t *metadata;
        rd_tmpabuf_t arena = RD_ZERO_INIT;
        char *payload;

        RD_UT_ASSERT(account, "create an exact decoded metadata budget");
        metadata = rd_kafka_metadata_arena_new(&arena, account, root, malloc);
        RD_UT_ASSERT(metadata, "admit root before decoded data");
        RD_UT_ASSERT(!rd_kafka_metadata_alloc(&arena, metadata, sizeof(name) + 1) &&
                         !metadata->segments &&
                         rd_kafka_byte_account_used(account) == root,
                     "denied segment leaves the existing owner intact");
        payload = rd_kafka_metadata_alloc(&arena, metadata, sizeof(name));
        RD_UT_ASSERT(payload && rd_kafka_byte_account_used(account) ==
                                    root + header + sizeof(name),
                     "segment charges its header and decoded bytes exactly");
        memcpy(payload, name, sizeof(name));
        metadata->metadata.orig_broker_name = payload;
        RD_UT_ASSERT(!strcmp(metadata->metadata.orig_broker_name, name) &&
                         !rd_kafka_metadata_alloc(&arena, metadata, 1),
                     "subsequent admission failure preserves present data");
        rd_kafka_metadata_destroy(&metadata->metadata);
        RD_UT_ASSERT(!rd_kafka_byte_account_used(account),
                     "the final root releases its segment charge as well");
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

static int ut_metadata_arena_clone_overlap(void) {
        size_t size;
        rd_tmpabuf_t first_buf = RD_ZERO_INIT, second_buf = RD_ZERO_INIT;
        rd_kafka_metadata_internal_t *first, *copy, *racks;
        rd_kafka_metadata_t *mock;
        rd_kafka_byte_account_t *account;
        RD_UT_ASSERT(rd_kafka_metadata_arena_size(2, 64, rd_false, &size), "size source arena");
        account = rd_kafka_byte_account_new(size * 2 - 1);
        RD_UT_ASSERT(account, "create overlapping arena account");
        first = rd_kafka_metadata_arena_new(&first_buf, account, size, malloc);
        RD_UT_ASSERT(first, "create retained source arena");
        first->metadata.orig_broker_name = rd_tmpabuf_write_str(&first_buf, "b");
        first->metadata.orig_broker_id = 23;
        RD_UT_ASSERT(!rd_kafka_metadata_arena_new(&second_buf, account, size, malloc) &&
                         first->metadata.orig_broker_id == 23 &&
                         rd_kafka_byte_account_used(account) == size,
                     "denied replacement preserves the independently retained source");
        copy = rd_kafka_metadata_copy(first, size);
        racks = rd_kafka_metadata_copy_add_racks(first, size);
        RD_UT_ASSERT(copy && racks && !copy->byte_account && !copy->byte_charge &&
                         !racks->byte_account && !racks->byte_charge,
                     "independent unaccounted clones never inherit the source charge");
        rd_kafka_metadata_destroy(&copy->metadata);
        rd_kafka_metadata_destroy(&racks->metadata);
        mock = rd_kafka_metadata_new_topic_mock(NULL, 0, 0, 0);
        RD_UT_ASSERT(!rd_kafka_metadata_get_internal(mock)->byte_account &&
                         !rd_kafka_metadata_get_internal(mock)->byte_charge,
                     "mock constructors initialize an uncharged root");
        rd_kafka_metadata_destroy(mock);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == size,
                     "uncharged clone and mock destruction leave the source charged");
        rd_kafka_metadata_destroy(&first->metadata);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == 0, "source final owner releases once");
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

static int ut_metadata_arena_op_ownership(void) {
        const rd_kafka_op_type_t types[] = {RD_KAFKA_OP_METADATA, RD_KAFKA_OP_METADATA_UPDATE};
        const size_t size = RD_ROUNDUP(sizeof(rd_kafka_metadata_internal_t), 8);
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(size);
        rd_kafka_metadata_internal_t *mdi;
        rd_kafka_op_t *op;
        rd_tmpabuf_t tbuf;
        size_t i;
        RD_UT_ASSERT(account, "create operation arena account");
        for (i = 0; i < RD_ARRAYSIZE(types); i++) {
                mdi = rd_kafka_metadata_arena_new(&tbuf, account, size, malloc);
                op = rd_kafka_op_new(types[i]);
                RD_UT_ASSERT(mdi && op, "construct metadata operation owner");
                op->rko_u.metadata.md = &mdi->metadata;
                op->rko_u.metadata.mdi = mdi;
                rd_kafka_op_destroy(op);
                RD_UT_ASSERT(rd_kafka_byte_account_used(account) == 0,
                             "each operation type disposes its root charge");
        }
        mdi = rd_kafka_metadata_arena_new(&tbuf, account, size, malloc);
        op = rd_kafka_op_new(RD_KAFKA_OP_METADATA);
        op->rko_u.metadata.md = &mdi->metadata;
        op->rko_u.metadata.mdi = mdi;
        op->rko_u.metadata.md = NULL;
        op->rko_u.metadata.mdi = NULL;
        rd_kafka_op_destroy(op);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == size,
                     "public pointer transfer survives its emptied operation");
        rd_kafka_metadata_destroy(&mdi->metadata);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == 0, "public owner completes final release");
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

static int ut_metadata_parser_rollback(void) {
        const unsigned char truncated[] = {0, 0, 0, 0};
        const unsigned char empty[] = {0, 0, 0, 0, 0, 0, 0, 0};
        rd_kafka_t rk = RD_ZERO_INIT;
        rd_kafka_broker_t broker = RD_ZERO_INIT;
        rd_kafka_buf_t *response;
        rd_kafka_metadata_internal_t *mdi;
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(4096);
        RD_UT_ASSERT(account && mtx_init(&broker.rkb_lock, mtx_plain) == thrd_success,
                     "initialize parser actor and account");
#if defined(_TTHREAD_WIN32_)
        RD_UT_ASSERT(DuplicateHandle(GetCurrentProcess(), thrd_current(), GetCurrentProcess(),
                                    &rk.rk_thread, 0, FALSE, DUPLICATE_SAME_ACCESS),
                     "retain main-thread fixture identity");
#else
        rk.rk_thread = thrd_current();
#endif
        rk.rk_byte_account = account;
        broker.rkb_rk = &rk;
        broker.rkb_features = RD_KAFKA_FEATURE_UNITTEST;
        rd_strlcpy(broker.rkb_name, "b", sizeof(broker.rkb_name));
        response = rd_kafka_buf_new_shadow(truncated, sizeof(truncated), NULL);
        response->rkbuf_reqhdr.ApiVersion = 0;
        mdi = (rd_kafka_metadata_internal_t *)&rk;
        RD_UT_ASSERT(rd_kafka_parse_Metadata_admin(&broker, response, NULL, &mdi) ==
                             RD_KAFKA_RESP_ERR__UNDERFLOW && !mdi &&
                         rd_kafka_byte_account_used(account) == 0,
                     "actual truncated parser rollback frees the admitted arena and clears output");
        rd_kafka_buf_destroy(response);
        response = rd_kafka_buf_new_shadow(empty, sizeof(empty), NULL);
        response->rkbuf_reqhdr.ApiVersion = 0;
        RD_UT_ASSERT(rd_kafka_parse_Metadata_admin(&broker, response, NULL, &mdi) ==
                             RD_KAFKA_RESP_ERR__PARTIAL && !mdi &&
                         rd_kafka_byte_account_used(account) == 0,
                     "empty response semantic failure releases the arena");
        rd_atomic32_set(&rk.rk_terminate, RD_KAFKA_DESTROY_F_TERMINATE);
        RD_UT_ASSERT(rd_kafka_parse_Metadata_admin(&broker, response, NULL, &mdi) ==
                             RD_KAFKA_RESP_ERR__DESTROY && !mdi &&
                         rd_kafka_byte_account_used(account) == 0,
                     "termination returns before charging or publishing metadata");
        rd_kafka_buf_destroy(response);
        rd_kafka_byte_account_destroy(account);
        mtx_destroy(&broker.rkb_lock);
#if defined(_TTHREAD_WIN32_)
        RD_UT_ASSERT(CloseHandle(rk.rk_thread), "release main-thread fixture identity");
#endif
        RD_UT_PASS();
}

extern int unittest_metadata_admin_account(void);

int unittest_metadata_account(void) {
        return ut_metadata_arena_admission() + ut_metadata_segments_admit_exact_storage() +
               ut_metadata_arena_clone_overlap() +
               ut_metadata_arena_op_ownership() + ut_metadata_parser_rollback() +
               unittest_metadata_admin_account();
}
