#ifndef _RDKAFKA_TOPIC_INFO_ADMISSION_FIXTURE_H_
#define _RDKAFKA_TOPIC_INFO_ADMISSION_FIXTURE_H_

#include "rdkafka_metadata_cache_admission_fixture.h"

static RD_UNUSED size_t ut_topic_info_empty_size(const char *name) {
        return RD_ROUNDUP(sizeof(rd_kafka_topic_info_t), 8) +
               RD_ROUNDUP(strlen(name) + 1, 8);
}

static RD_UNUSED rd_bool_t ut_topic_info_seed_cache(
    struct ut_cache_admission_client *fixture, const char *name) {
        rd_kafka_metadata_topic_t topic = {.topic = (char *)name};
        rd_kafka_metadata_topic_internal_t internal = RD_ZERO_INIT;
        rd_kafka_metadata_cache_update_result_t result;
        rd_kafka_wrlock(&fixture->rk);
        result = rd_kafka_metadata_cache_topic_update(&fixture->rk, &topic,
                                                       &internal, rd_false,
                                                       rd_false, rd_false);
        rd_kafka_wrunlock(&fixture->rk);
        return result == RD_KAFKA_METADATA_CACHE_CHANGED;
}

static RD_UNUSED rd_kafka_topic_partition_list_t *
ut_topic_info_subscription(rd_bool_t wildcard) {
        rd_kafka_topic_partition_list_t *topics = rd_kafka_topic_partition_list_new(2);
        rd_kafka_topic_partition_list_add(topics, wildcard ? "^first$" : "first", RD_KAFKA_PARTITION_UA);
        rd_kafka_topic_partition_list_add(topics, wildcard ? "^second$" : "second", RD_KAFKA_PARTITION_UA);
        return topics;
}

#endif
