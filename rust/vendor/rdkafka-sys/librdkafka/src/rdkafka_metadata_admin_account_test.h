#include "rdunittest.h"

int unittest_metadata_admin_account(void);

struct ut_metadata_admin_log {
        rd_kafka_byte_account_t *account;
        size_t used_during_message;
        char message[256];
};

static void ut_metadata_admin_log(const rd_kafka_t *rk, int level,
                                  const char *facility, const char *message) {
        struct ut_metadata_admin_log *record = rk->rk_conf.opaque;
        (void)level;
        (void)facility;
        if (strstr(message, "Broker returned topic")) {
                record->used_during_message = rd_kafka_byte_account_used(record->account);
                rd_strlcpy(record->message, message, sizeof(record->message));
        }
}

static rd_kafka_op_t *ut_metadata_admin_request(rd_kafka_op_type_t type, const char *topic) {
        rd_kafka_op_t *request = rd_kafka_op_new(type);
        rd_list_init(&request->rko_u.admin_request.args, topic ? 1 : 0, rd_free);
        if (topic)
                rd_list_add(&request->rko_u.admin_request.args, rd_strdup(topic));
        rd_kafka_confval_init_ptr(&request->rko_u.admin_request.options.opaque, "opaque");
        return request;
}

static rd_kafka_metadata_internal_t *ut_metadata_admin_arena(
    rd_kafka_byte_account_t *account, int count, const char *topic) {
        const size_t root = RD_ROUNDUP(sizeof(rd_kafka_metadata_internal_t), 8);
        const size_t public_topics = RD_ROUNDUP(count * sizeof(rd_kafka_metadata_topic_t), 8);
        const size_t size = root + public_topics +
            RD_ROUNDUP(count * sizeof(rd_kafka_metadata_topic_internal_t), 8);
        rd_kafka_metadata_internal_t *mdi;
        int i;
        if (!rd_kafka_byte_account_acquire(account, size))
                return NULL;
        mdi = calloc(1, size);
        if (!mdi) {
                rd_kafka_byte_account_release(account, size);
                return NULL;
        }
        mdi->byte_account = account;
        mdi->byte_charge = size;
        mdi->metadata.topic_cnt = count;
        mdi->metadata.topics = (void *)((char *)mdi + root);
        mdi->topics = (void *)((char *)mdi + root + public_topics);
        for (i = 0; i < count; i++) {
                mdi->metadata.topics[i].topic = (char *)topic;
                mdi->metadata.topics[i].err = RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART;
        }
        return mdi;
}

static int ut_metadata_admin_projection_release(void) {
        rd_kafka_t rk = RD_ZERO_INIT;
        rd_kafka_broker_t broker = RD_ZERO_INIT;
        struct ut_metadata_admin_log record = RD_ZERO_INIT;
        rd_kafka_buf_t reply = RD_ZERO_INIT;
        rd_kafka_metadata_internal_t *mdi;
        rd_kafka_op_t *request, *result;
        char message[256];
        int i;
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(4096);
        RD_UT_ASSERT(account && mtx_init(&broker.rkb_logname_lock, mtx_plain) == thrd_success,
                     "initialize synchronous projection diagnostics");
        record.account = account;
        rk.rk_conf.opaque = &record;
        rk.rk_conf.log_level = LOG_ERR;
        rk.rk_conf.log_cb = ut_metadata_admin_log;
        broker.rkb_rk = &rk;
        broker.rkb_logname = "metadata-fixture";
        reply.rkbuf_rkb = &broker;
        for (i = 0; i < 2; i++) {
                const char *topic = i ? "same" : "unexpected";
                request = ut_metadata_admin_request(RD_KAFKA_OP_DESCRIBETOPICS, i ? "same" : "wanted");
                mdi = ut_metadata_admin_arena(account, i ? 2 : 1, topic);
                result = NULL;
                record.used_during_message = 0;
                record.message[0] = 0;
                RD_UT_ASSERT(mdi && rd_kafka_DescribeTopicsResponse_project(
                                 request, &result, &reply, mdi, message, sizeof(message)) ==
                                 RD_KAFKA_RESP_ERR__BAD_MSG && !result &&
                                 strstr(record.message, topic) && record.used_during_message > 0 &&
                                 rd_kafka_byte_account_used(account) == 0,
                             "invalid and duplicate topics log live names then dispose every arena owner");
                rd_kafka_op_destroy(request);
        }
        request = ut_metadata_admin_request(RD_KAFKA_OP_DESCRIBETOPICS, "same");
        mdi = ut_metadata_admin_arena(account, 1, "same");
        result = NULL;
        RD_UT_ASSERT(mdi && rd_kafka_DescribeTopicsResponse_project(
                         request, &result, &reply, mdi, message, sizeof(message)) ==
                         RD_KAFKA_RESP_ERR_NO_ERROR && result &&
                         rd_kafka_byte_account_used(account) == 0 &&
                         !strcmp(((rd_kafka_TopicDescription_t *)rd_list_elem(
                             &result->rko_u.admin_result.results, 0))->topic, "same"),
                     "successful owned projection outlives release of its parsed arena");
        rd_kafka_op_destroy(result);
        rd_kafka_op_destroy(request);
        rd_kafka_byte_account_destroy(account);
        mtx_destroy(&broker.rkb_logname_lock);
        RD_UT_PASS();
}

static int ut_metadata_admin_admission_error(void) {
        const unsigned char body[] = {0, 0, 0, 0};
        rd_kafka_resp_err_t (*parsers[])(rd_kafka_op_t *, rd_kafka_op_t **,
                                       rd_kafka_buf_t *, char *, size_t) = {
            rd_kafka_DescribeTopicsResponse_parse, rd_kafka_DescribeClusterResponse_parse};
        const rd_kafka_op_type_t types[] = {RD_KAFKA_OP_DESCRIBETOPICS, RD_KAFKA_OP_DESCRIBECLUSTER};
        rd_kafka_t rk = RD_ZERO_INIT;
        rd_kafka_broker_t broker = RD_ZERO_INIT;
        rd_kafka_buf_t *reply;
        rd_kafka_op_t *request, *result;
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(0);
        char message[256];
        size_t i;
        RD_UT_ASSERT(account && mtx_init(&broker.rkb_lock, mtx_plain) == thrd_success,
                     "initialize denied admin parser actor");
        rwlock_init(&rk.rk_lock);
#if defined(_TTHREAD_WIN32_)
        RD_UT_ASSERT(DuplicateHandle(GetCurrentProcess(), thrd_current(), GetCurrentProcess(),
                                    &rk.rk_thread, 0, FALSE, DUPLICATE_SAME_ACCESS),
                     "retain main-thread admin fixture identity");
#else
        rk.rk_thread = thrd_current();
#endif
        rk.rk_byte_account = account;
        broker.rkb_rk = &rk;
        broker.rkb_features = RD_KAFKA_FEATURE_UNITTEST;
        rd_strlcpy(broker.rkb_name, "b", sizeof(broker.rkb_name));
        for (i = 0; i < RD_ARRAYSIZE(parsers); i++) {
                request = ut_metadata_admin_request(types[i], NULL);
                result = NULL;
                reply = rd_kafka_buf_new_shadow(body, sizeof(body), NULL);
                reply->rkbuf_reqhdr.ApiVersion = 0;
                reply->rkbuf_rkb = &broker;
                RD_UT_ASSERT(parsers[i](request, &result, reply, message, sizeof(message)) ==
                                 RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE && !result &&
                                 reply->rkbuf_err == RD_KAFKA_RESP_ERR_NO_ERROR &&
                                 rd_kafka_byte_account_used(account) == 0,
                             "admin wrappers preserve the native admission error outside the parser error field");
                reply->rkbuf_rkb = NULL;
                rd_kafka_buf_destroy(reply);
                rd_kafka_op_destroy(request);
        }
        rd_kafka_byte_account_destroy(account);
        rwlock_destroy(&rk.rk_lock);
        mtx_destroy(&broker.rkb_lock);
#if defined(_TTHREAD_WIN32_)
        RD_UT_ASSERT(CloseHandle(rk.rk_thread), "release main-thread admin fixture identity");
#endif
        RD_UT_PASS();
}

int unittest_metadata_admin_account(void) {
        return ut_metadata_admin_projection_release() + ut_metadata_admin_admission_error();
}
