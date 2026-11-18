#include "rdkafka_metadata_cache_admission_fixture.h"

int unittest_metadata_cache_admission(void);
extern int unittest_metadata_cache_admission_responses(void);
extern int unittest_metadata_cache_admission_admin(void);

static void *ut_cache_admission_malloc_denied(size_t size) {
        (void)size;
        return NULL;
}

static int ut_cache_admission_checked_layout(void) {
        struct ut_cache_value input;
        size_t size = 0, preserved;
        RD_UT_ASSERT(!rd_kafka_metadata_cache_size_add(&size, SIZE_MAX, 2) && !size,
                     "product overflow preserves total");
        RD_UT_ASSERT(!rd_kafka_metadata_cache_size_add(&size, 1, SIZE_MAX) && !size,
                     "alignment overflow preserves total");
        size = SIZE_MAX - 7;
        preserved = size;
        RD_UT_ASSERT(!rd_kafka_metadata_cache_size_add(&size, 8, 1) && size == preserved,
                     "aggregate overflow preserves total");
        ut_cache_value_init(&input, 2, 1);
        RD_UT_ASSERT(rd_kafka_metadata_cache_full_size(&input.topic, &input.internal, &size) &&
                         size == RD_ROUNDUP(sizeof(struct rd_kafka_metadata_cache_entry), 8) +
                             RD_ROUNDUP(strlen(input.topic.topic) + 1, 8) +
                             RD_ROUNDUP(2 * sizeof(*input.topic.partitions), 8) +
                             RD_ROUNDUP(2 * sizeof(*input.internal.partitions), 8) +
                             2 * (RD_ROUNDUP(sizeof(char *), 8) + RD_ROUNDUP(9, 8)),
                     "full layout charges each retained extent with its eight-byte padding");
        input.internal.partitions[0].racks_cnt = SIZE_MAX;
        preserved = size;
        RD_UT_ASSERT(!rd_kafka_metadata_cache_full_size(&input.topic, &input.internal, &size) && size == preserved,
                     "rack pointer extent overflow precedes payload traversal");
        input.internal.partitions[0].racks_cnt = 1;
        ut_cache_value_destroy(&input);
        RD_UT_PASS();
}

static int ut_cache_admission_exact_and_malloc_failure(void) {
        struct ut_cache_value input;
        struct rd_kafka_metadata_cache_entry *entry;
        rd_kafka_byte_account_t *exact, *short_account;
        size_t size;
        ut_cache_value_init(&input, 1, 1);
        RD_UT_ASSERT(rd_kafka_metadata_cache_full_size(&input.topic, &input.internal, &size),
                     "measure full retained value");
        exact = rd_kafka_byte_account_new(size);
        short_account = rd_kafka_byte_account_new(size - 1);
        RD_UT_ASSERT(exact && short_account, "create exact and undersized accounts");
        RD_UT_ASSERT(!rd_kafka_metadata_cache_entry_new_accounted(
                         short_account, &input.topic, &input.internal, malloc) &&
                         rd_kafka_byte_account_used(short_account) == 0,
                     "one-byte-short denial retains no arena charge");
        RD_UT_ASSERT(!rd_kafka_metadata_cache_entry_new_accounted(
                         exact, &input.topic, &input.internal, ut_cache_admission_malloc_denied) &&
                         rd_kafka_byte_account_used(exact) == 0,
                     "fallible malloc denial releases the prior arena admission");
        entry = rd_kafka_metadata_cache_entry_new_accounted(exact, &input.topic, &input.internal, malloc);
        RD_UT_ASSERT(entry && entry->rkmce_byte_account == exact &&
                         entry->rkmce_byte_charge == size && rd_kafka_byte_account_used(exact) == size,
                     "exact admission remains with the actual arena");
        ut_cache_value_destroy(&input);
        RD_UT_ASSERT(!strcmp(entry->rkmce_metadata_internal_topic.partitions[0].racks[0], "old-rack"),
                     "charged retained payload survives input destruction");
        rd_kafka_metadata_cache_entry_destroy(entry);
        RD_UT_ASSERT(rd_kafka_byte_account_used(exact) == 0, "common final free releases the full charge");
        rd_kafka_byte_account_destroy(short_account);
        rd_kafka_byte_account_destroy(exact);
        RD_UT_PASS();
}

static int ut_cache_admission_merge_accounts_and_seed(void) {
        struct ut_cache_value seed, first, next;
        struct rd_kafka_metadata_cache_entry *base, *old, *merged;
        rd_kafka_t rk = RD_ZERO_INIT;
        rd_kafka_byte_account_t *a = rd_kafka_byte_account_new(1 << 20);
        rd_kafka_byte_account_t *b = rd_kafka_byte_account_new(1 << 20);
        size_t old_size, next_size;
        RD_UT_ASSERT(a && b, "create independent cache arena accounts");
        ut_cache_value_init(&seed, 2, 0);
        base = rd_kafka_metadata_cache_entry_new(&seed.topic, &seed.internal);
        ut_cache_value_destroy(&seed);
        ut_cache_value_init(&first, 2, 1);
        rk.rk_byte_account = a;
        old = rd_kafka_metadata_cache_topic_update_merge_partitions(
            &rk, base, &first.topic, &first.internal, rd_true, rd_true);
        RD_UT_ASSERT(old, "admit reliable old value");
        old_size = old->rkmce_byte_charge;
        rd_kafka_metadata_cache_entry_destroy(base);
        ut_cache_value_destroy(&first);
        ut_cache_value_init(&next, 3, 2);
        next.internal.partitions[0].leader_epoch = 0;
        RD_UT_ASSERT(rd_kafka_metadata_cache_merge_size(old, &next.topic, &next.internal, rd_true, &next_size) &&
                         next_size == 2 * RD_ROUNDUP(sizeof(*old), 8) +
                             RD_ROUNDUP(strlen(next.topic.topic) + 1, 8) +
                             RD_ROUNDUP(3 * sizeof(*next.topic.partitions), 8) +
                             RD_ROUNDUP(3 * sizeof(*next.internal.partitions), 8) +
                             3 * (RD_ROUNDUP(2 * sizeof(int32_t), 8) + RD_ROUNDUP(sizeof(int32_t), 8) +
                                  RD_ROUNDUP(sizeof(char *), 8) + RD_ROUNDUP(9, 8)),
                     "reliable merge charges its source seed, max extent and selected payloads");
        rk.rk_byte_account = b;
        RD_UT_ASSERT(!rd_kafka_metadata_cache_topic_update_merge_partitions0(
                         &rk, old, &next.topic, &next.internal, rd_true, rd_true,
                         ut_cache_admission_malloc_denied) &&
                         rd_kafka_byte_account_used(a) == old_size &&
                         rd_kafka_byte_account_used(b) == 0,
                     "merge malloc failure releases only its newly admitted target charge");
        merged = rd_kafka_metadata_cache_topic_update_merge_partitions(
            &rk, old, &next.topic, &next.internal, rd_true, rd_true);
        RD_UT_ASSERT(merged && merged->rkmce_byte_account == b &&
                         rd_kafka_byte_account_used(a) == old_size &&
                         rd_kafka_byte_account_used(b) == next_size,
                     "new storage belongs to the actual client account while old storage remains charged");
        ut_cache_value_destroy(&next);
        rd_kafka_metadata_cache_entry_destroy(old);
        RD_UT_ASSERT(rd_kafka_byte_account_used(a) == 0 &&
                         merged->rkmce_mtopic.partitions[0].replicas[0] == 101 &&
                         merged->rkmce_mtopic.partitions[2].replicas[1] == 204 &&
                         !strcmp(merged->rkmce_metadata_internal_topic.partitions[0].racks[0], "old-rack"),
                     "carried and incoming payloads retain no old arena borrow");
        rd_kafka_metadata_cache_entry_destroy(merged);
        RD_UT_ASSERT(rd_kafka_byte_account_used(b) == 0, "new account releases after its own arena");
        rd_kafka_byte_account_destroy(b);
        rd_kafka_byte_account_destroy(a);
        RD_UT_PASS();
}

static int ut_cache_admission_replacement_overlap(void) {
        struct ut_cache_value initial, next;
        struct rd_kafka_metadata_cache_entry *layout, *old, *current;
        struct ut_cache_admission_client fixture;
        size_t full_size, merge_size;
        int exact;
        ut_cache_value_init(&initial, 1, 1);
        ut_cache_value_init(&next, 1, 2);
        layout = rd_kafka_metadata_cache_entry_new(&initial.topic, &initial.internal);
        RD_UT_ASSERT(rd_kafka_metadata_cache_full_size(&initial.topic, &initial.internal, &full_size) &&
                         rd_kafka_metadata_cache_merge_size(layout, &next.topic, &next.internal, rd_true, &merge_size),
                     "measure replacement overlap");
        rd_kafka_metadata_cache_entry_destroy(layout);
        for (exact = 0; exact < 2; exact++) {
                rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(full_size + merge_size - !exact);
                rd_kafka_metadata_cache_update_result_t result;
                rd_ts_t inserted, expires, timer;
                RD_UT_ASSERT(account, "create replacement account");
                ut_cache_admission_client_init(&fixture, account);
                rd_kafka_wrlock(&fixture.rk);
                RD_UT_ASSERT(rd_kafka_metadata_cache_topic_update(&fixture.rk, &initial.topic, &initial.internal,
                                 rd_false, rd_true, rd_false) == RD_KAFKA_METADATA_CACHE_CHANGED,
                             "install initial charged value");
                old = rd_kafka_metadata_cache_find(&fixture.rk, initial.topic.topic, 1);
                inserted = old->rkmce_ts_insert;
                expires = old->rkmce_ts_expires;
                rd_kafka_metadata_cache_expiry_start(&fixture.rk);
                timer = fixture.rk.rk_metadata_cache.rkmc_expiry_tmr.rtmr_next;
                next.topic.err = RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE;
                RD_UT_ASSERT(rd_kafka_metadata_cache_topic_update(&fixture.rk, &next.topic, &next.internal,
                                 rd_false, rd_true, rd_true) == RD_KAFKA_METADATA_CACHE_UNCHANGED &&
                                 rd_kafka_metadata_cache_find(&fixture.rk, initial.topic.topic, 1) == old &&
                                 rd_kafka_byte_account_used(account) == full_size,
                             "temporary topic error returns exact unchanged outcome without a replacement charge");
                next.topic.err = RD_KAFKA_RESP_ERR_NO_ERROR;
                result = rd_kafka_metadata_cache_topic_update(&fixture.rk, &next.topic, &next.internal,
                                                              rd_false, rd_true, rd_true);
                current = rd_kafka_metadata_cache_find(&fixture.rk, initial.topic.topic, 1);
                RD_UT_ASSERT(result == (exact ? RD_KAFKA_METADATA_CACHE_CHANGED : RD_KAFKA_METADATA_CACHE_DENIED) &&
                                 fixture.rk.rk_metadata_cache.rkmc_cnt == 1 &&
                                 rd_kafka_metadata_cache_find_by_id(&fixture.rk, initial.internal.topic_id, 1) == current &&
                                 TAILQ_FIRST(&fixture.rk.rk_metadata_cache.rkmc_expiry) == current &&
                                 rd_kafka_byte_account_used(account) == (exact ? merge_size : full_size),
                             "replacement requires simultaneous old and new charge before index publication");
                RD_UT_ASSERT(exact ? current != old :
                                 (current == old && current->rkmce_ts_insert == inserted &&
                                  current->rkmce_ts_expires == expires &&
                                  fixture.rk.rk_metadata_cache.rkmc_expiry_tmr.rtmr_next == timer &&
                                  current->rkmce_metadata_internal_topic.partitions[0].leader_epoch == 1),
                             "denial preserves the complete indexed expiry generation");
                rd_kafka_wrunlock(&fixture.rk);
                ut_cache_admission_client_destroy(&fixture);
                RD_UT_ASSERT(rd_kafka_byte_account_used(account) == 0, "teardown frees the installed generation once");
                rd_kafka_byte_account_destroy(account);
        }
        ut_cache_value_destroy(&next);
        ut_cache_value_destroy(&initial);
        RD_UT_PASS();
}

static int ut_cache_admission_hint_denial_queries_without_mutating(void) {
        struct ut_cache_admission_client fixture;
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(4 * ut_cache_admission_empty_size("old"));
        rd_kafka_metadata_topic_t topic = {.topic = "valid"};
        rd_kafka_metadata_topic_internal_t internal = RD_ZERO_INIT;
        struct rd_kafka_metadata_cache_entry *old, *waiting;
        rd_list_t names, query;
        const char *input[] = {"fresh", "old", "other", "wait", "valid"};
        size_t i;
        rd_ts_t old_insert, old_expiry;
        RD_UT_ASSERT(account, "create mixed hint account");
        ut_cache_admission_client_init(&fixture, account);
        rd_list_init(&names, 5, NULL);
        rd_list_init(&query, 5, rd_free);
        rd_kafka_wrlock(&fixture.rk);
        rd_list_add(&names, "old");
        RD_UT_ASSERT(rd_kafka_metadata_cache_hint(&fixture.rk, &names, NULL, RD_KAFKA_RESP_ERR__NOENT) == 1,
                     "install missing-topic hint");
        rd_list_clear(&names);
        rd_list_add(&names, "wait");
        RD_UT_ASSERT(rd_kafka_metadata_cache_hint(&fixture.rk, &names, NULL, RD_KAFKA_RESP_ERR__WAIT_CACHE) == 1 &&
                         rd_kafka_metadata_cache_topic_update(&fixture.rk, &topic, &internal,
                             rd_false, rd_false, rd_false) == RD_KAFKA_METADATA_CACHE_CHANGED,
                     "install active query and valid cache entry");
        old = rd_kafka_metadata_cache_find(&fixture.rk, "old", 0);
        waiting = rd_kafka_metadata_cache_find(&fixture.rk, "wait", 0);
        old_insert = old->rkmce_ts_insert;
        old_expiry = old->rkmce_ts_expires;
        rd_list_clear(&names);
        for (i = 0; i < RD_ARRAYSIZE(input); i++)
                rd_list_add(&names, (void *)input[i]);
        RD_UT_ASSERT(rd_kafka_metadata_cache_hint(&fixture.rk, &names, &query, RD_KAFKA_RESP_ERR__WAIT_CACHE) == 1 &&
                         rd_list_cnt(&query) == 3 && !strcmp(rd_list_elem(&query, 0), "fresh") &&
                         !strcmp(rd_list_elem(&query, 1), "old") && !strcmp(rd_list_elem(&query, 2), "other"),
                     "mixed batch counts accepted storage while denied eligible names still reach the query");
        RD_UT_ASSERT(rd_kafka_metadata_cache_find(&fixture.rk, "old", 0) == old &&
                         old->rkmce_mtopic.err == RD_KAFKA_RESP_ERR__NOENT &&
                         old->rkmce_ts_insert == old_insert && old->rkmce_ts_expires == old_expiry &&
                         rd_kafka_metadata_cache_find(&fixture.rk, "wait", 0) == waiting &&
                         !rd_kafka_metadata_cache_find(&fixture.rk, "other", 0) &&
                         fixture.rk.rk_metadata_cache.rkmc_cnt == 4,
                     "denied upgrade preserves original suppression state and active queries stay filtered");
        rd_list_clear(&names);
        rd_list_add(&names, "old");
        RD_UT_ASSERT(rd_kafka_metadata_cache_hint(&fixture.rk, &names, NULL, RD_KAFKA_RESP_ERR__WAIT_CACHE) == 0 &&
                         old->rkmce_mtopic.err == RD_KAFKA_RESP_ERR__NOENT &&
                         rd_kafka_byte_account_used(account) == 4 * ut_cache_admission_empty_size("old"),
                     "no-destination denial also preserves the old hint and charge");
        rd_list_clear(&names);
        rd_list_add(&names, "wait");
        RD_UT_ASSERT(rd_kafka_metadata_cache_hint(&fixture.rk, &names, NULL, RD_KAFKA_RESP_ERR__NOENT) == 0 &&
                         rd_kafka_metadata_cache_find(&fixture.rk, "wait", 0) == waiting &&
                         waiting->rkmce_mtopic.err == RD_KAFKA_RESP_ERR__WAIT_CACHE &&
                         !rd_kafka_timer_is_started(&fixture.rk.rk_timers,
                             &fixture.rk.rk_metadata_cache.rkmc_expiry_tmr),
                     "denied retry hint preserves active WAIT state and does not invent an expiry timer");
        rd_list_clear(&names);
        rd_list_add(&names, "old");
        rd_kafka_metadata_cache_purge_hints(&fixture.rk, &names);
        RD_UT_ASSERT(!rd_kafka_metadata_cache_find(&fixture.rk, "old", 0) &&
                         rd_kafka_byte_account_used(account) == 3 * ut_cache_admission_empty_size("old"),
                     "failed-request hint cleanup releases only the removed arena");
        rd_kafka_wrunlock(&fixture.rk);
        rd_list_destroy(&query);
        rd_list_destroy(&names);
        ut_cache_admission_client_destroy(&fixture);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == 0, "all retained hint and valid entries release at teardown");
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

static int ut_cache_admission_zero_length_pointer_presence(void) {
        struct ut_cache_value input;
        struct rd_kafka_metadata_cache_entry *entry;
        rd_kafka_byte_account_t *account;
        size_t size;
        ut_cache_value_init(&input, 2, 1);
        rd_free(input.internal.partitions[0].racks[0]);
        input.internal.partitions[0].racks_cnt = 0;
        rd_free(input.internal.partitions[1].racks[0]);
        rd_free(input.internal.partitions[1].racks);
        input.internal.partitions[1].racks = NULL;
        input.internal.partitions[1].racks_cnt = 0;
        RD_UT_ASSERT(rd_kafka_metadata_cache_full_size(&input.topic, &input.internal, &size) &&
                         size == RD_ROUNDUP(sizeof(*entry), 8) +
                             RD_ROUNDUP(strlen(input.topic.topic) + 1, 8) +
                             RD_ROUNDUP(2 * sizeof(*input.topic.partitions), 8) +
                             RD_ROUNDUP(2 * sizeof(*input.internal.partitions), 8),
                     "zero-length retained rack presence requires no payload extent");
        account = rd_kafka_byte_account_new(size);
        RD_UT_ASSERT(account, "create exact zero-length account");
        entry = rd_kafka_metadata_cache_entry_new_accounted(account, &input.topic, &input.internal, malloc);
        RD_UT_ASSERT(entry && entry->rkmce_metadata_internal_topic.partitions[0].racks &&
                         !entry->rkmce_metadata_internal_topic.partitions[0].racks_cnt &&
                         !entry->rkmce_metadata_internal_topic.partitions[1].racks &&
                         rd_kafka_byte_account_used(account) == size,
                     "admission preserves nonnull zero-length versus absent racks");
        ut_cache_value_destroy(&input);
        rd_kafka_metadata_cache_entry_destroy(entry);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == 0, "zero-length geometry releases its complete arena");
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

static int ut_cache_admission_common_disposal_routes(void) {
        struct ut_cache_admission_client fixture;
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(1 << 20);
        rd_kafka_metadata_topic_t topic = {.topic = "value"};
        rd_kafka_metadata_topic_internal_t internal = {
            .topic_id = {.most_significant_bits = 1, .least_significant_bits = 2}};
        struct rd_kafka_metadata_cache_entry *entry;
        rd_list_t ids;
        size_t charge = ut_cache_admission_empty_size("value");
        int route;
        RD_UT_ASSERT(account, "create disposal account");
        ut_cache_admission_client_init(&fixture, account);
        rd_kafka_wrlock(&fixture.rk);
        for (route = 0; route < 5; route++) {
                RD_UT_ASSERT(rd_kafka_metadata_cache_topic_update(&fixture.rk, &topic, &internal,
                                 rd_false, rd_false, rd_false) == RD_KAFKA_METADATA_CACHE_CHANGED &&
                                 rd_kafka_byte_account_used(account) == charge,
                             "install arena before final disposal route");
                switch (route) {
                case 0:
                        rd_kafka_metadata_cache_delete_by_name(&fixture.rk, topic.topic);
                        break;
                case 1:
                        rd_kafka_metadata_cache_delete_by_topic_id(&fixture.rk, internal.topic_id);
                        break;
                case 2: {
                        rd_kafka_metadata_topic_t unnamed = RD_ZERO_INIT;
                        RD_UT_ASSERT(rd_kafka_metadata_cache_topic_update(&fixture.rk, &unnamed, &internal,
                                         rd_false, rd_false, rd_false) == RD_KAFKA_METADATA_CACHE_CHANGED,
                                     "unnamed response removes its identified arena");
                        break;
                }
                case 3:
                        entry = rd_kafka_metadata_cache_find(&fixture.rk, topic.topic, 1);
                        entry->rkmce_ts_expires = 0;
                        RD_UT_ASSERT(rd_kafka_metadata_cache_evict(&fixture.rk) == 1, "expiry removes one arena");
                        break;
                default:
                        rd_kafka_metadata_cache_purge(&fixture.rk, rd_false);
                        break;
                }
                RD_UT_ASSERT(!fixture.rk.rk_metadata_cache.rkmc_cnt && rd_kafka_byte_account_used(account) == 0,
                             "every indexed disposal route reaches the common final release");
        }
        topic.err = RD_KAFKA_RESP_ERR__WAIT_CACHE;
        RD_UT_ASSERT(rd_kafka_metadata_cache_insert_new(&fixture.rk, &topic, &internal, 1, 2),
                     "install identified charged hint");
        rd_list_init(&ids, 1, NULL);
        rd_list_add(&ids, &internal.topic_id);
        rd_kafka_metadata_cache_purge_hints_by_id(&fixture.rk, &ids);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == 0,
                     "identified failed-request hint purge releases its arena");
        rd_list_destroy(&ids);
        RD_UT_ASSERT(rd_kafka_metadata_cache_insert_new(&fixture.rk, &topic, &internal, 1, 2) &&
                         rd_kafka_metadata_cache_purge_all_hints(&fixture.rk) == 1 &&
                         rd_kafka_byte_account_used(account) == 0,
                     "all-hints purge reaches the same final arena release");
        rd_kafka_wrunlock(&fixture.rk);
        ut_cache_admission_client_destroy(&fixture);
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

int unittest_metadata_cache_admission(void) {
        return ut_cache_admission_checked_layout() +
               ut_cache_admission_exact_and_malloc_failure() +
               ut_cache_admission_merge_accounts_and_seed() +
               ut_cache_admission_replacement_overlap() +
               ut_cache_admission_hint_denial_queries_without_mutating() +
               ut_cache_admission_zero_length_pointer_presence() +
               ut_cache_admission_common_disposal_routes() +
               unittest_metadata_cache_admission_responses() +
               unittest_metadata_cache_admission_admin();
}
