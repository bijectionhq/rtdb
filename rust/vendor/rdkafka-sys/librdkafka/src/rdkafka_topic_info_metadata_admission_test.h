#include "rdkafka_topic_info_admission_fixture.h"

int unittest_topic_info_metadata_admission(void);

static int ut_topic_info_metadata_denied_prefix(rd_bool_t wildcard, int accepted) {
        struct ut_cache_admission_client fixture;
        size_t cache_bytes = 2 * ut_cache_admission_empty_size("first");
        size_t copy_bytes = ut_topic_info_empty_size("first");
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(cache_bytes + accepted * copy_bytes);
        rd_kafka_topic_partition_list_t *subscription, *errored;
        rd_list_t infos;
        rd_kafka_resp_err_t error;
        rd_kafka_topic_info_t *prefix, *copy;
        RD_UT_ASSERT(account, "create exact Metadata topic-info boundary");
        ut_cache_admission_client_init(&fixture, account);
        RD_UT_ASSERT(ut_topic_info_seed_cache(&fixture, "first") && ut_topic_info_seed_cache(&fixture, "second"),
                     "install two independently charged cache entries");
        subscription = ut_topic_info_subscription(wildcard);
        rd_kafka_topic_partition_list_add(subscription, "missing", RD_KAFKA_PARTITION_UA);
        errored = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(errored, "prior", RD_KAFKA_PARTITION_UA)->err = RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED;
        rd_list_init(&infos, 3, (void *)rd_kafka_topic_info_destroy);
        prefix = rd_kafka_topic_info_new("prefix", 9);
        rd_list_add(&infos, prefix);
        error = wildcard ? rd_kafka_metadata_topic_match(&fixture.rk, &infos, subscription, errored) :
                           rd_kafka_metadata_topic_filter(&fixture.rk, &infos, subscription, errored);
        RD_UT_ASSERT(error == RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE && rd_list_cnt(&infos) == 1 + accepted &&
                         rd_list_elem(&infos, 0) == prefix && errored->cnt == 1 &&
                         errored->elems[0].err == RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED &&
                         rd_kafka_byte_account_used(account) == cache_bytes + accepted * copy_bytes,
                     "both matchers return a typed denial with owned output prefixes and no fabricated later topic error");
        if (accepted) {
                rd_bool_t independent;
                const struct rd_kafka_metadata_cache_entry *cached;
                copy = rd_list_elem(&infos, 1);
                rd_kafka_rdlock(&fixture.rk);
                cached = rd_kafka_metadata_cache_find(&fixture.rk, "first", 1);
                independent = cached && copy && copy->byte_account == account && !strcmp(copy->topic, "first") &&
                    copy->topic != cached->rkmce_mtopic.topic;
                rd_kafka_rdunlock(&fixture.rk);
                RD_UT_ASSERT(independent,
                             "accepted prefix owns copied name on the actual client's account");
        }
        rd_kafka_wrlock(&fixture.rk);
        rd_kafka_metadata_cache_purge(&fixture.rk, rd_false);
        rd_kafka_wrunlock(&fixture.rk);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == accepted * copy_bytes &&
                         (!accepted || !strcmp(((rd_kafka_topic_info_t *)rd_list_elem(&infos, 1))->topic, "first")),
                     "copied partial output remains live after its complete source cache is freed");
        rd_list_destroy(&infos);
        rd_kafka_topic_partition_list_destroy(errored);
        rd_kafka_topic_partition_list_destroy(subscription);
        ut_cache_admission_client_destroy(&fixture);
        RD_UT_ASSERT(!rd_kafka_byte_account_used(account), "caller cleanup releases every accepted partial copy");
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

static int ut_topic_info_metadata_complete(rd_bool_t wildcard) {
        struct ut_cache_admission_client fixture;
        size_t cache_bytes = 2 * ut_cache_admission_empty_size("first");
        size_t copy_bytes = 2 * ut_topic_info_empty_size("first");
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(cache_bytes + copy_bytes);
        rd_kafka_topic_partition_list_t *subscription, *errored;
        rd_list_t infos;
        rd_kafka_resp_err_t error;
        RD_UT_ASSERT(account, "create complete projection account");
        ut_cache_admission_client_init(&fixture, account);
        RD_UT_ASSERT(ut_topic_info_seed_cache(&fixture, "first") && ut_topic_info_seed_cache(&fixture, "second"),
                     "install complete Metadata source values");
        subscription = ut_topic_info_subscription(wildcard);
        rd_kafka_topic_partition_list_add(subscription, "missing", RD_KAFKA_PARTITION_UA);
        errored = rd_kafka_topic_partition_list_new(0);
        rd_list_init(&infos, 2, (void *)rd_kafka_topic_info_destroy);
        error = wildcard ? rd_kafka_metadata_topic_match(&fixture.rk, &infos, subscription, errored) :
                           rd_kafka_metadata_topic_filter(&fixture.rk, &infos, subscription, errored);
        RD_UT_ASSERT(error == RD_KAFKA_RESP_ERR_NO_ERROR && rd_list_cnt(&infos) == 2 &&
                         errored->cnt == 1 && !strcmp(errored->elems[0].topic, "missing") &&
                         errored->elems[0].err == RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC &&
                         rd_kafka_byte_account_used(account) == cache_bytes + copy_bytes,
                     "successful typed matchers preserve complete output and unknown-topic error behavior");
        rd_list_destroy(&infos);
        rd_kafka_topic_partition_list_destroy(errored);
        rd_kafka_topic_partition_list_destroy(subscription);
        ut_cache_admission_client_destroy(&fixture);
        RD_UT_ASSERT(!rd_kafka_byte_account_used(account), "complete candidate and cache have independent final release");
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

int unittest_topic_info_metadata_admission(void) {
        return ut_topic_info_metadata_denied_prefix(rd_false, 0) +
               ut_topic_info_metadata_denied_prefix(rd_false, 1) +
               ut_topic_info_metadata_denied_prefix(rd_true, 0) +
               ut_topic_info_metadata_denied_prefix(rd_true, 1) +
               ut_topic_info_metadata_complete(rd_false) +
               ut_topic_info_metadata_complete(rd_true);
}
