#include "rdkafka_metadata_cache_admission_fixture.h"

int unittest_metadata_cache_admission_admin(void);

static int ut_cache_admission_admin_handler(int cluster, rd_bool_t late) {
        struct ut_cache_admission_client fixture;
        rd_kafka_byte_account_t *account;
        rd_kafka_buf_t *response;
        rd_kafka_op_t *request, *result = NULL, *notice, *pending;
        rd_kafka_q_t *notifications;
        rd_kafka_enq_once_t *observer;
        rd_kafka_resp_err_t error;
        rd_bool_t retained, notified, error_preserved;
        char message[256];
        account = rd_kafka_byte_account_new(ut_cache_admission_parser_size() +
                    (late ? ut_cache_admission_empty_size("first") : 0));
        RD_UT_ASSERT(account, "create admin account with exact cache failure boundary");
        ut_cache_admission_client_init(&fixture, account);
        request = ut_metadata_admin_request(cluster ? RD_KAFKA_OP_DESCRIBECLUSTER : RD_KAFKA_OP_DESCRIBETOPICS, NULL);
        notifications = rd_kafka_q_new(&fixture.rk);
        observer = rd_kafka_enq_once_new(rd_kafka_op_new(RD_KAFKA_OP_WAKEUP), RD_KAFKA_REPLYQ(notifications, 0));
        rd_kafka_wrlock(&fixture.rk);
        rd_kafka_metadata_cache_wait_state_change_async(&fixture.rk, observer);
        rd_kafka_wrunlock(&fixture.rk);
        response = rd_kafka_buf_new_shadow(ut_cache_admission_response,
                                           sizeof(ut_cache_admission_response), NULL);
        response->rkbuf_reqhdr.ApiVersion = 0;
        response->rkbuf_rkb = &fixture.broker;
        error = cluster ? rd_kafka_DescribeClusterResponse_parse(request, &result, response, message, sizeof(message)) :
                          rd_kafka_DescribeTopicsResponse_parse(request, &result, response, message, sizeof(message));
        error_preserved = error == RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE &&
            !result && response->rkbuf_err == RD_KAFKA_RESP_ERR_NO_ERROR;
        response->rkbuf_rkb = NULL;
        retained = ut_cache_admission_after_parse(&fixture, late);
        notice = rd_kafka_q_pop(notifications, RD_POLL_NOWAIT, 0);
        notified = late ? (notice && notice->rko_err == RD_KAFKA_RESP_ERR_NO_ERROR) : !notice;
        if (notice)
                rd_kafka_op_destroy(notice);
        pending = rd_kafka_enq_once_disable(observer);
        if (pending)
                rd_kafka_op_destroy(pending);
        rd_kafka_buf_destroy(response);
        rd_kafka_op_destroy(request);
        rd_kafka_q_destroy_owner(notifications);
        ut_cache_admission_client_destroy(&fixture);
        RD_UT_ASSERT(error_preserved && retained && notified &&
                         rd_kafka_byte_account_used(account) == 0,
                     "both actual admin parsers return cache admission error before projection and notify earlier accepted cache changes");
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

int unittest_metadata_cache_admission_admin(void) {
        return ut_cache_admission_admin_handler(0, rd_false) +
               ut_cache_admission_admin_handler(0, rd_true) +
               ut_cache_admission_admin_handler(1, rd_false) +
               ut_cache_admission_admin_handler(1, rd_true);
}
