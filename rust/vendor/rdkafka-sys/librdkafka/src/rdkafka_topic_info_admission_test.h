#include "rdkafka_topic_info_admission_fixture.h"

int unittest_topic_info_admission(void);
extern int unittest_topic_info_metadata_admission(void);
extern int unittest_topic_info_cgrp_admission(void);

static void *ut_topic_info_malloc_denied(size_t size) {
        (void)size;
        return NULL;
}

static int ut_topic_info_checked_layout(void) {
        rd_kafka_metadata_partition_internal_t parts[2] = RD_ZERO_INIT;
        char *empty[1] = {NULL};
        size_t total = 0, preserved, size;
        rd_bool_t racks;
        RD_UT_ASSERT(!rd_kafka_topic_info_size_add(&total, SIZE_MAX, 2) && !total,
                     "topic-info product overflow preserves output");
        RD_UT_ASSERT(!rd_kafka_topic_info_size_add(&total, 1, SIZE_MAX) && !total,
                     "topic-info alignment overflow preserves output");
        total = SIZE_MAX - 7;
        preserved = total;
        RD_UT_ASSERT(!rd_kafka_topic_info_size_add(&total, 8, 1) && total == preserved,
                     "topic-info aggregate overflow preserves output");
        RD_UT_ASSERT(rd_kafka_topic_info_rack_size("first", 2, parts, &size, &racks) &&
                         size == ut_topic_info_empty_size("first") && !racks,
                     "absent racks retain only root and name regardless of partition count");
        parts[1].racks = empty;
        RD_UT_ASSERT(rd_kafka_topic_info_rack_size("first", 2, parts, &size, &racks) && racks &&
                         size == ut_topic_info_empty_size("first") + RD_ROUNDUP(sizeof(parts), 8),
                     "present empty rack list retains the internal partition array");
        parts[1].racks_cnt = SIZE_MAX;
        preserved = size;
        RD_UT_ASSERT(!rd_kafka_topic_info_rack_size("first", 2, parts, &size, &racks) && size == preserved,
                     "rack pointer overflow is rejected before string traversal");
        RD_UT_PASS();
}

static int ut_topic_info_exact_short_and_malloc_failure(void) {
        const size_t size = ut_topic_info_empty_size("first");
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(size);
        rd_kafka_byte_account_t *short_account = rd_kafka_byte_account_new(size - 1);
        rd_kafka_topic_info_t *info, *plain;
        RD_UT_ASSERT(account && short_account, "create exact topic-info accounts");
        RD_UT_ASSERT(!rd_kafka_topic_info_new_with_rack_accounted(short_account, "first", 0, NULL) &&
                         !rd_kafka_byte_account_used(short_account),
                     "one-byte-short topic-info admission retains no storage");
        RD_UT_ASSERT(!rd_kafka_topic_info_new_with_rack0(account, "first", 0, NULL, ut_topic_info_malloc_denied) &&
                         !rd_kafka_byte_account_used(account),
                     "malloc denial returns the newly acquired charge");
        info = rd_kafka_topic_info_new_with_rack_accounted(account, "first", 0, NULL);
        RD_UT_ASSERT(info && info->byte_account == account && info->byte_charge == size &&
                         rd_kafka_byte_account_used(account) == size,
                     "exact charge stays with its independent arena");
        rd_kafka_topic_info_destroy(NULL);
        rd_kafka_topic_info_destroy(info);
        plain = rd_kafka_topic_info_new("plain", 3);
        RD_UT_ASSERT(plain && !plain->byte_account && !plain->byte_charge &&
                         !plain->partitions_internal && plain->partition_cnt == 3 &&
                         !rd_kafka_byte_account_used(account),
                     "legacy plain constructor initializes unaccounted final ownership");
        rd_kafka_topic_info_destroy(plain);
        rd_kafka_byte_account_destroy(short_account);
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

static int ut_topic_info_mixed_racks_outlive_input(void) {
        rd_kafka_metadata_partition_internal_t parts[3] = RD_ZERO_INIT;
        rd_kafka_topic_info_t *info;
        char *rack = rd_strdup("rack-value");
        char *racks[1] = {rack};
        char *empty[1] = {NULL};
        size_t size = ut_topic_info_empty_size("first") + RD_ROUNDUP(sizeof(parts), 8) +
            RD_ROUNDUP(sizeof(char *), 8) + RD_ROUNDUP(strlen(rack) + 1, 8);
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(size);
        parts[0].id = 0;
        parts[1].id = 1;
        parts[2].id = 2;
        parts[1].racks = empty;
        parts[2].racks = racks;
        parts[2].racks_cnt = 1;
        RD_UT_ASSERT(account, "create mixed-rack account");
        info = rd_kafka_topic_info_new_with_rack_accounted(account, "first", 3, parts);
        RD_UT_ASSERT(info && info->byte_charge == size && info->partitions_internal &&
                         !info->partitions_internal[0].racks && !info->partitions_internal[0].racks_cnt &&
                         info->partitions_internal[1].racks && !info->partitions_internal[1].racks_cnt &&
                         info->partitions_internal[2].racks != racks && info->partitions_internal[2].racks[0] != rack,
                     "copied mixed rack geometry is independent and fully admitted");
        memset(rack, 'x', strlen(rack));
        rd_free(rack);
        memset(parts, 0, sizeof(parts));
        RD_UT_ASSERT(!strcmp(info->partitions_internal[2].racks[0], "rack-value") &&
                         rd_kafka_byte_account_used(account) == size,
                     "retained rack payload survives complete source destruction");
        rd_kafka_topic_info_destroy(info);
        RD_UT_ASSERT(!rd_kafka_byte_account_used(account), "final mixed-rack free releases every retained extent");
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

static int ut_topic_info_old_and_new_overlap(void) {
        size_t size = ut_topic_info_empty_size("first");
        int exact;
        for (exact = 0; exact < 2; exact++) {
                rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(2 * size - !exact);
                rd_kafka_topic_info_t *old, *next;
                RD_UT_ASSERT(account, "create topic-info overlap account");
                old = rd_kafka_topic_info_new_with_rack_accounted(account, "first", 0, NULL);
                next = rd_kafka_topic_info_new_with_rack_accounted(account, "first", 0, NULL);
                RD_UT_ASSERT(old && (!!next == !!exact) &&
                                 rd_kafka_byte_account_used(account) == (exact ? 2 * size : size),
                             "candidate admission keeps the retained old arena charged");
                rd_kafka_topic_info_destroy(old);
                RD_UT_ASSERT(rd_kafka_byte_account_used(account) == (exact ? size : 0) &&
                                 (!next || !strcmp(next->topic, "first")),
                             "candidate lifetime does not borrow from the old topic-info arena");
                rd_kafka_topic_info_destroy(next);
                RD_UT_ASSERT(!rd_kafka_byte_account_used(account), "both independent arenas release exactly once");
                rd_kafka_byte_account_destroy(account);
        }
        RD_UT_PASS();
}

int unittest_topic_info_admission(void) {
        return ut_topic_info_checked_layout() + ut_topic_info_exact_short_and_malloc_failure() +
               ut_topic_info_mixed_racks_outlive_input() + ut_topic_info_old_and_new_overlap() +
               unittest_topic_info_metadata_admission() + unittest_topic_info_cgrp_admission();
}
