#include "rdunittest.h"

int unittest_metadata_cache_ownership(void);
extern int unittest_metadata_cache_parsers(void);

struct ut_cache_value {
        rd_kafka_metadata_topic_t topic;
        rd_kafka_metadata_topic_internal_t internal;
};

static void ut_cache_value_init(struct ut_cache_value *value, int count, int epoch) {
        int i;
        memset(value, 0, sizeof(*value));
        value->topic.topic = rd_strdup("cache-owned");
        value->topic.partition_cnt = count;
        value->topic.partitions = rd_calloc(count, sizeof(*value->topic.partitions));
        value->internal.partitions = rd_calloc(count, sizeof(*value->internal.partitions));
        value->internal.topic_id.most_significant_bits = 1;
        for (i = 0; i < count; i++) {
                rd_kafka_metadata_partition_t *part = &value->topic.partitions[i];
                rd_kafka_metadata_partition_internal_t *internal = &value->internal.partitions[i];
                part->id = internal->id = i;
                part->leader = 7;
                internal->leader_epoch = epoch;
                part->replica_cnt = 2;
                part->replicas = rd_malloc(2 * sizeof(*part->replicas));
                part->replicas[0] = 100 + epoch + i;
                part->replicas[1] = 200 + epoch + i;
                part->isr_cnt = 1;
                part->isrs = rd_malloc(sizeof(*part->isrs));
                part->isrs[0] = 100 + epoch + i;
                internal->racks = rd_malloc(sizeof(*internal->racks));
                internal->racks_cnt = 1;
                internal->racks[0] = rd_strdup(epoch == 1 ? "old-rack" : "new-rack");
        }
}

static void ut_cache_value_destroy(struct ut_cache_value *value) {
        int i;
        for (i = 0; i < value->topic.partition_cnt; i++) {
                size_t j;
                rd_free(value->topic.partitions[i].replicas);
                rd_free(value->topic.partitions[i].isrs);
                for (j = 0; j < value->internal.partitions[i].racks_cnt; j++)
                        rd_free(value->internal.partitions[i].racks[j]);
                rd_free(value->internal.partitions[i].racks);
        }
        rd_free(value->topic.partitions);
        rd_free(value->internal.partitions);
        rd_free(value->topic.topic);
}

static int ut_cache_full_owns_present_and_empty_racks(void) {
        struct ut_cache_value input;
        struct rd_kafka_metadata_cache_entry *entry;
        rd_kafka_metadata_partition_internal_t *parts;
        ut_cache_value_init(&input, 3, 1);
        rd_free(input.internal.partitions[1].racks[0]);
        input.internal.partitions[1].racks_cnt = 0;
        rd_free(input.internal.partitions[2].racks[0]);
        rd_free(input.internal.partitions[2].racks);
        input.internal.partitions[2].racks = NULL;
        input.internal.partitions[2].racks_cnt = 0;
        entry = rd_kafka_metadata_cache_entry_new(&input.topic, &input.internal);
        parts = entry->rkmce_metadata_internal_topic.partitions;
        RD_UT_ASSERT(parts[0].racks != input.internal.partitions[0].racks &&
                         parts[0].racks[0] != input.internal.partitions[0].racks[0] &&
                         parts[1].racks && !parts[1].racks_cnt && !parts[2].racks,
                     "full cache owns present rack payload and preserves empty versus absent arrays");
        ut_cache_value_destroy(&input);
        RD_UT_ASSERT(parts[0].racks_cnt == 1 && !strcmp(parts[0].racks[0], "old-rack") &&
                         parts[1].racks_cnt == 0 && parts[1].racks && !parts[2].racks &&
                         !entry->rkmce_mtopic.partitions[0].replicas && !entry->rkmce_mtopic.partitions[0].replica_cnt &&
                         !entry->rkmce_mtopic.partitions[0].isrs && !entry->rkmce_mtopic.partitions[0].isr_cnt,
                     "input destruction leaves retained rack facts and source uncached fields unchanged");
        rd_free(entry);
        RD_UT_PASS();
}

static int ut_cache_merge_owns_selected_pointees(void) {
        struct ut_cache_value seed, first, second;
        struct rd_kafka_metadata_cache_entry *base, *old, *merged;
        rd_kafka_t rk = RD_ZERO_INIT;
        ut_cache_value_init(&seed, 2, 0);
        base = rd_kafka_metadata_cache_entry_new(&seed.topic, &seed.internal);
        ut_cache_value_destroy(&seed);
        ut_cache_value_init(&first, 2, 1);
        old = rd_kafka_metadata_cache_topic_update_merge_partitions(
            &rk, base, &first.topic, &first.internal, rd_true, rd_true);
        RD_UT_ASSERT(old->rkmce_mtopic.partitions[0].replicas != first.topic.partitions[0].replicas &&
                         old->rkmce_mtopic.partitions[0].isrs != first.topic.partitions[0].isrs,
                     "first reliable merge acquires selected replica and ISR arrays");
        ut_cache_value_destroy(&first);
        rd_free(base);
        ut_cache_value_init(&second, 3, 2);
        second.internal.partitions[0].leader_epoch = 0;
        merged = rd_kafka_metadata_cache_topic_update_merge_partitions(
            &rk, old, &second.topic, &second.internal, rd_true, rd_true);
        RD_UT_ASSERT(merged->rkmce_mtopic.partitions[0].replicas != old->rkmce_mtopic.partitions[0].replicas &&
                         merged->rkmce_metadata_internal_topic.partitions[0].racks !=
                             old->rkmce_metadata_internal_topic.partitions[0].racks &&
                         merged->rkmce_mtopic.partitions[1].replicas != second.topic.partitions[1].replicas,
                     "old and new epoch winners have independent retained payloads");
        ut_cache_value_destroy(&second);
        rd_free(old);
        RD_UT_ASSERT(merged->rkmce_mtopic.partition_cnt == 3 &&
                         merged->rkmce_mtopic.partitions[0].replicas[0] == 101 &&
                         merged->rkmce_mtopic.partitions[0].isrs[0] == 101 &&
                         !strcmp(merged->rkmce_metadata_internal_topic.partitions[0].racks[0], "old-rack") &&
                         merged->rkmce_mtopic.partitions[1].replicas[0] == 103 &&
                         merged->rkmce_mtopic.partitions[2].replicas[1] == 204 &&
                         !strcmp(merged->rkmce_metadata_internal_topic.partitions[2].racks[0], "new-rack"),
                     "carried old, replaced and new-only partitions survive both source owners");
        rd_free(merged);
        RD_UT_PASS();
}

static int ut_cache_merge_preserves_empty_arrays_and_logical_count(void) {
        struct ut_cache_value seed, next, smaller;
        struct rd_kafka_metadata_cache_entry *base, *old, *merged;
        rd_kafka_t rk = RD_ZERO_INIT;
        ut_cache_value_init(&seed, 2, 0);
        base = rd_kafka_metadata_cache_entry_new(&seed.topic, &seed.internal);
        ut_cache_value_destroy(&seed);
        ut_cache_value_init(&next, 2, 1);
        next.topic.partitions[0].replica_cnt = 0;
        next.topic.partitions[0].isr_cnt = 0;
        rd_free(next.topic.partitions[1].replicas);
        next.topic.partitions[1].replicas = NULL;
        next.topic.partitions[1].replica_cnt = 0;
        old = rd_kafka_metadata_cache_topic_update_merge_partitions(
            &rk, base, &next.topic, &next.internal, rd_false, rd_true);
        ut_cache_value_destroy(&next);
        rd_free(base);
        RD_UT_ASSERT(old->rkmce_mtopic.partitions[0].replicas && old->rkmce_mtopic.partitions[0].isrs &&
                         !old->rkmce_mtopic.partitions[0].replica_cnt && !old->rkmce_mtopic.partitions[0].isr_cnt &&
                         !old->rkmce_mtopic.partitions[1].replicas &&
                         !old->rkmce_metadata_internal_topic.partitions[0].racks &&
                         !old->rkmce_metadata_internal_topic.partitions[0].racks_cnt,
                     "merge keeps zero-count presence and source rack exclusion");
        ut_cache_value_init(&smaller, 1, 2);
        merged = rd_kafka_metadata_cache_topic_update_merge_partitions(
            &rk, old, &smaller.topic, &smaller.internal, rd_true, rd_true);
        ut_cache_value_destroy(&smaller);
        rd_free(old);
        RD_UT_ASSERT(merged->rkmce_mtopic.partition_cnt == 1 &&
                         merged->rkmce_mtopic.partitions[0].replicas[0] == 102 &&
                         merged->rkmce_mtopic.partitions[1].id == 1 &&
                         merged->rkmce_metadata_internal_topic.partitions[1].racks &&
                         !merged->rkmce_metadata_internal_topic.partitions[1].racks_cnt,
                     "physical old-only extent and present empty rack slot preserve the incoming logical count");
        rd_free(merged);
        RD_UT_PASS();
}

int unittest_metadata_cache_ownership(void) {
        return ut_cache_full_owns_present_and_empty_racks() + ut_cache_merge_owns_selected_pointees() +
               ut_cache_merge_preserves_empty_arrays_and_logical_count() + unittest_metadata_cache_parsers();
}
