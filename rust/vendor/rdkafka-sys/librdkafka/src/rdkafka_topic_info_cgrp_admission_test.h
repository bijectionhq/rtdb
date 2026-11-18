#include "rdkafka_topic_info_admission_fixture.h"

int unittest_topic_info_cgrp_admission(void);

struct ut_topic_info_group {
        struct ut_cache_admission_client client;
        rd_kafka_cgrp_t *group;
        rd_kafka_assignor_t assignor;
};

static rd_bool_t ut_topic_info_group_init(struct ut_topic_info_group *fixture,
                                            rd_kafka_byte_account_t *account,
                                            rd_bool_t wildcard) {
        rd_kafka_t *rk;
        rd_kafka_cgrp_t *group;
        const char *names[] = {"first", "second", "removed"};
        size_t i;
        memset(fixture, 0, sizeof(*fixture));
        ut_cache_admission_client_init(&fixture->client, account);
        rk = &fixture->client.rk;
        rk->rk_type = RD_KAFKA_CONSUMER;
        rk->rk_conf.group_protocol = RD_KAFKA_GROUP_PROTOCOL_CLASSIC;
        rk->rk_conf.enabled_assignor_cnt = 1;
        rk->rk_group_id = rd_kafkap_str_new("group", -1);
        rk->rk_ts_full_metadata = rd_clock();
        group = rd_calloc(1, sizeof(*group));
        fixture->group = group;
        rk->rk_cgrp = group;
        group->rkcg_rk = rk;
        group->rkcg_group_id = rk->rk_group_id;
        group->rkcg_group_protocol = RD_KAFKA_GROUP_PROTOCOL_CLASSIC;
        group->rkcg_state = RD_KAFKA_CGRP_STATE_UP;
        group->rkcg_join_state = RD_KAFKA_CGRP_JOIN_STATE_STEADY;
        group->rkcg_wait_resp = -1;
        group->rkcg_q = rd_kafka_q_new(rk);
        group->rkcg_ops = rd_kafka_q_new(rk);
        group->rkcg_wait_coord_q = rd_kafka_q_new(rk);
        TAILQ_INIT(&group->rkcg_topics);
        rd_list_init(&group->rkcg_toppars, 0, NULL);
        group->rkcg_current_assignment = rd_kafka_topic_partition_list_new(0);
        group->rkcg_group_assignment = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(group->rkcg_group_assignment, "removed", 0);
        group->rkcg_client_rack = rd_kafkap_str_new(NULL, -1);
        group->rkcg_subscribed_topics = rd_list_new(3, (void *)rd_kafka_topic_info_destroy);
        group->rkcg_errored_topics = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(group->rkcg_errored_topics, "prior", RD_KAFKA_PARTITION_UA)->err =
            RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED;
        fixture->assignor.rkas_protocol = RD_KAFKA_REBALANCE_PROTOCOL_COOPERATIVE;
        group->rkcg_assignor = &fixture->assignor;
        for (i = 0; i < RD_ARRAYSIZE(names); i++) {
                rd_kafka_topic_info_t *info = rd_kafka_topic_info_new_with_rack_accounted(account, names[i], 0, NULL);
                if (!info)
                        return rd_false;
                rd_list_add(group->rkcg_subscribed_topics, info);
        }
        rd_list_sort(group->rkcg_subscribed_topics, rd_kafka_topic_info_cmp);
        rd_kafka_cgrp_subscription_set(group, ut_topic_info_subscription(wildcard));
        return ut_topic_info_seed_cache(&fixture->client, "first") &&
               ut_topic_info_seed_cache(&fixture->client, "second");
}

static void ut_topic_info_group_destroy(struct ut_topic_info_group *fixture) {
        rd_kafka_t *rk = &fixture->client.rk;
        rd_kafka_cgrp_t *group = fixture->group;
        rd_kafka_cgrp_subscription_set(group, NULL);
        rd_kafka_topic_partition_list_destroy(group->rkcg_group_assignment);
        group->rkcg_group_assignment = NULL;
        rd_kafka_cgrp_destroy_final(group);
        rk->rk_cgrp = NULL;
        rd_kafkap_str_destroy(rk->rk_group_id);
        rk->rk_group_id = NULL;
        ut_cache_admission_client_destroy(&fixture->client);
}

static int ut_topic_info_group_projection_denial(rd_bool_t subscribe,
                                                 rd_bool_t wildcard,
                                                 int accepted) {
        struct ut_topic_info_group fixture;
        size_t info_size = ut_topic_info_empty_size("first");
        size_t retained = 2 * ut_cache_admission_empty_size("first") + 3 * info_size;
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(retained + accepted * info_size);
        rd_list_t *effective;
        rd_kafka_topic_partition_list_t *errors, *assignment, *input = NULL;
        rd_kafka_cgrp_t *group;
        rd_kafka_op_t *notice, *reply = NULL;
        rd_kafka_q_t *replies = NULL;
        int32_t version;
        rd_bool_t reply_owned = rd_true;
        RD_UT_ASSERT(account && ut_topic_info_group_init(&fixture, account, wildcard),
                     "initialize caller-owned group with complete retained list and exact candidate capacity");
        group = fixture.group;
        effective = group->rkcg_subscribed_topics;
        errors = group->rkcg_errored_topics;
        assignment = group->rkcg_group_assignment;
        version = rd_atomic32_get(&group->rkcg_subscription_version);
        if (subscribe) {
                rd_kafka_op_t *op = rd_kafka_op_new(RD_KAFKA_OP_SUBSCRIBE);
                input = ut_topic_info_subscription(wildcard);
                op->rko_u.subscribe.topics = input;
                replies = rd_kafka_q_new(&fixture.client.rk);
                rd_kafka_op_set_replyq(op, replies, NULL);
                rd_kafka_cgrp_op_serve(&fixture.client.rk, group->rkcg_ops, op,
                                        RD_KAFKA_Q_CB_CALLBACK, group);
                reply = rd_kafka_q_pop(replies, RD_POLL_NOWAIT, 0);
                reply_owned = reply && reply->rko_err == RD_KAFKA_RESP_ERR_NO_ERROR &&
                    !reply->rko_u.subscribe.topics && group->rkcg_subscription == input &&
                    rd_atomic32_get(&group->rkcg_subscription_version) == version + 1;
                if (reply)
                        rd_kafka_op_destroy(reply);
                rd_kafka_q_destroy_owner(replies);
        } else {
                rd_kafka_cgrp_metadata_update_check(group, rd_false);
                reply_owned = rd_atomic32_get(&group->rkcg_subscription_version) == version;
        }
        notice = rd_kafka_q_pop(group->rkcg_q, RD_POLL_NOWAIT, 0);
        RD_UT_ASSERT(reply_owned && notice && notice->rko_type == RD_KAFKA_OP_CONSUMER_ERR &&
                         notice->rko_err == RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE &&
                         group->rkcg_subscribed_topics == effective && rd_list_cnt(effective) == 3 &&
                         group->rkcg_errored_topics == errors && errors->cnt == 1 &&
                         group->rkcg_group_assignment == assignment && assignment->cnt == 1 &&
                         !strcmp(assignment->elems[0].topic, "removed") &&
                         group->rkcg_join_state == RD_KAFKA_CGRP_JOIN_STATE_STEADY &&
                         !rd_kafka_q_len(group->rkcg_ops) && !rd_kafka_q_len(group->rkcg_q) &&
                         rd_kafka_byte_account_used(account) == retained,
                     "both group paths discard partial copies, keep complete effective/error/assignment state and publish one asynchronous resource error");
        rd_kafka_op_destroy(notice);
        if (subscribe)
                RD_UT_ASSERT(input->cnt == 2 && input == group->rkcg_subscription,
                             "destroying the successful SUBSCRIBE reply leaves its adopted input alive exactly once");
        ut_topic_info_group_destroy(&fixture);
        RD_UT_ASSERT(!rd_kafka_byte_account_used(account), "actual final cgrp destructor releases retained topic-info before the client account ends");
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

static int ut_topic_info_effective_replacement_and_member_copy(void) {
        struct ut_topic_info_group fixture;
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(1 << 20);
        rd_kafka_cgrp_t *group;
        rd_list_t *next, *old;
        rd_kafkap_bytes_t *metadata;
        const unsigned char prefix[] = {0,3,0,0,0,2,0,5,'f','i','r','s','t',0,6,'s','e','c','o','n','d'};
        const char *names[] = {"first", "second", "removed"};
        size_t i, retained;
        RD_UT_ASSERT(account && ut_topic_info_group_init(&fixture, account, rd_false),
                     "initialize retained group copy lifetime fixture");
        group = fixture.group;
        old = group->rkcg_subscribed_topics;
        retained = rd_kafka_byte_account_used(account);
        next = rd_list_new(3, (void *)rd_kafka_topic_info_destroy);
        for (i = 0; i < RD_ARRAYSIZE(names); i++) {
                rd_kafka_topic_info_t *info = rd_kafka_topic_info_new_with_rack_accounted(account, names[i], 0, NULL);
                RD_UT_ASSERT(info, "admit equal candidate value");
                rd_list_add(next, info);
        }
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == retained + 3 * ut_topic_info_empty_size("first") &&
                         !rd_kafka_cgrp_update_subscribed_topics(group, next) &&
                         group->rkcg_subscribed_topics == old && rd_kafka_byte_account_used(account) == retained,
                     "equal candidate comparison frees its newly admitted arenas while retaining the original list");
        next = rd_list_new(2, (void *)rd_kafka_topic_info_destroy);
        for (i = 0; i < 2; i++) {
                rd_kafka_topic_info_t *info = rd_kafka_topic_info_new_with_rack_accounted(account, names[i], 0, NULL);
                RD_UT_ASSERT(info, "admit changed candidate value");
                rd_list_add(next, info);
        }
        RD_UT_ASSERT(rd_kafka_cgrp_update_subscribed_topics(group, next) &&
                         group->rkcg_subscribed_topics == next &&
                         rd_kafka_byte_account_used(account) == retained - ut_topic_info_empty_size("first"),
                     "changed complete effective list releases its old arenas after new candidate admission");
        rd_kafka_wrlock(&fixture.client.rk);
        rd_kafka_metadata_cache_purge(&fixture.client.rk, rd_false);
        rd_kafka_wrunlock(&fixture.client.rk);
        metadata = rd_kafka_consumer_protocol_member_metadata_new(
            group->rkcg_subscribed_topics, NULL, 0, NULL, -1, group->rkcg_client_rack);
        RD_UT_ASSERT(metadata && metadata->len >= 0 && (size_t)metadata->len >= sizeof(prefix) &&
                         !memcmp(metadata->data, prefix, sizeof(prefix)) &&
                         rd_kafka_byte_account_used(account) == 2 * ut_topic_info_empty_size("first"),
                     "built-in member metadata encoding reads retained copies after their source cache is gone");
        rd_kafka_cgrp_update_subscribed_topics(group, NULL);
        RD_UT_ASSERT(!rd_kafka_byte_account_used(account) && !memcmp(metadata->data, prefix, sizeof(prefix)),
                     "clearing effective topics releases their charge while independent encoded output stays valid");
        rd_kafkap_bytes_destroy(metadata);
        ut_topic_info_group_destroy(&fixture);
        RD_UT_ASSERT(!rd_kafka_byte_account_used(account), "empty retained-list and client teardown release no duplicate charge");
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

int unittest_topic_info_cgrp_admission(void) {
        int failures = ut_topic_info_effective_replacement_and_member_copy();
        int subscribe, wildcard, accepted;
        for (subscribe = 0; subscribe < 2; subscribe++)
                for (wildcard = 0; wildcard < 2; wildcard++)
                        for (accepted = 0; accepted < 2; accepted++)
                                failures += ut_topic_info_group_projection_denial(subscribe, wildcard, accepted);
        return failures;
}
