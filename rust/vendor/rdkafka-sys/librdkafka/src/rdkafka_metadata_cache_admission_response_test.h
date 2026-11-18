#include "rdkafka_metadata_cache_admission_fixture.h"

int unittest_metadata_cache_admission_responses(void);

static int ut_cache_admission_ordinary_handler(rd_bool_t late) {
        struct ut_cache_admission_client fixture;
        rd_kafka_byte_account_t *account;
        rd_kafka_buf_t request = RD_ZERO_INIT;
        rd_kafka_buf_t *response;
        rd_kafka_q_t *replies;
        rd_kafka_op_t *request_op, *reply;
        rd_list_t topics, hint;
        size_t hint_size = ut_cache_admission_empty_size("hint");
        rd_bool_t error_preserved, retained;
        account = rd_kafka_byte_account_new(ut_cache_admission_parser_size() + hint_size +
                    (late ? ut_cache_admission_empty_size("first") : 0));
        RD_UT_ASSERT(account, "create ordinary parser account with exact cache failure boundary");
        ut_cache_admission_client_init(&fixture, account);
        rd_list_init(&hint, 1, NULL);
        rd_list_add(&hint, "hint");
        rd_kafka_wrlock(&fixture.rk);
        RD_UT_ASSERT(rd_kafka_metadata_cache_hint(&fixture.rk, &hint, NULL, RD_KAFKA_RESP_ERR__NOENT) == 1,
                     "install requested hint before response failure");
        rd_kafka_wrunlock(&fixture.rk);
        rd_list_destroy(&hint);
        rd_list_init(&topics, 3, NULL);
        rd_list_add(&topics, "first");
        rd_list_add(&topics, "second");
        rd_list_add(&topics, "hint");
        request.rkbuf_reqhdr.ApiKey = RD_KAFKAP_Metadata;
        request.rkbuf_u.Metadata.topics = &topics;
        request.rkbuf_u.Metadata.reason = (char *)"cache admission fixture";
        request.rkbuf_u.Metadata.cgrp_subscription_version = -1;
        response = rd_kafka_buf_new_shadow(ut_cache_admission_response,
                                           sizeof(ut_cache_admission_response), NULL);
        response->rkbuf_reqhdr.ApiVersion = 0;
        replies = rd_kafka_q_new(&fixture.rk);
        request_op = rd_kafka_op_new(RD_KAFKA_OP_METADATA);
        rd_kafka_op_set_replyq(request_op, replies, NULL);
        rd_kafka_handle_Metadata(&fixture.rk, &fixture.broker, RD_KAFKA_RESP_ERR_NO_ERROR,
                                 response, &request, request_op);
        reply = rd_kafka_q_pop(replies, RD_POLL_NOWAIT, 0);
        error_preserved = reply == request_op &&
            reply->rko_err == RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE &&
            !reply->rko_u.metadata.md && !reply->rko_u.metadata.mdi &&
            response->rkbuf_err == RD_KAFKA_RESP_ERR_NO_ERROR &&
            rd_kafka_q_len(replies) == 0;
        retained = ut_cache_admission_after_parse(&fixture, late);
        if (reply)
                rd_kafka_op_destroy(reply);
        rd_kafka_q_destroy_owner(replies);
        rd_kafka_buf_destroy(response);
        rd_list_destroy(&topics);
        ut_cache_admission_client_destroy(&fixture);
        RD_UT_ASSERT(error_preserved && retained && rd_kafka_byte_account_used(account) == 0,
                     "ordinary handler returns original resource error, purges requested hints and retains only earlier cache effects");
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

int unittest_metadata_cache_admission_responses(void) {
        return ut_cache_admission_ordinary_handler(rd_false) +
               ut_cache_admission_ordinary_handler(rd_true);
}
