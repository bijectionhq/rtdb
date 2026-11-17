static rd_kafka_resp_err_t ut_decode_accounted_api_versions(
    rd_kafka_broker_t *rkb, const void *body, size_t length,
    struct rd_kafka_ApiVersion **apis, size_t *count) {
        rd_kafka_buf_t request = RD_ZERO_INIT;
        rd_kafka_buf_t *response = rd_kafka_buf_new(1, length);
        rd_kafka_resp_err_t error;
        rd_kafka_buf_write(response, body, length);
        rd_slice_init_full(&response->rkbuf_reader, &response->rkbuf_buf);
        error = rd_kafka_handle_ApiVersion(rkb->rkb_rk, rkb,
                                           RD_KAFKA_RESP_ERR_NO_ERROR,
                                           response, &request, apis, count);
        rd_kafka_buf_destroy(response);
        return error;
}

static int ut_accounted_api_versions_decode(void) {
        const unsigned char success[] = {
                0, 0, 0, 0, 0, 2, 0, 18, 0, 0, 0, 4, 0, 3, 0, 1, 0, 13
        };
        const unsigned char downgrade[] = {0, 35, 0, 0, 0, 1, 0, 18, 0, 0, 0, 2};
        const unsigned char empty[] = {0, 0, 0, 0, 0, 0};
        const unsigned char absent[] = {0, 0, 255, 255, 255, 255};
        const size_t entry = sizeof(struct rd_kafka_ApiVersion);
        rd_kafka_t rk = RD_ZERO_INIT;
        rd_kafka_broker_t broker = RD_ZERO_INIT;
        struct rd_kafka_ApiVersion *apis;
        size_t count;
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(2 * entry);

        RD_UT_ASSERT(account, "create API range account");
        rk.rk_byte_account = account;
        broker.rkb_rk = &rk;
        RD_UT_ASSERT(ut_decode_accounted_api_versions(&broker, absent,
                         sizeof(absent), &apis, &count) == RD_KAFKA_RESP_ERR__BAD_MSG &&
                         !apis && !count && rd_kafka_byte_account_used(account) == 0,
                     "null array fails before retaining an owner");
        RD_UT_ASSERT(ut_decode_accounted_api_versions(&broker, success,
                         sizeof(success) - 1, &apis, &count) == RD_KAFKA_RESP_ERR__UNDERFLOW &&
                         !apis && !count && rd_kafka_byte_account_used(account) == 0,
                     "parse failure releases the admitted array");
        RD_UT_ASSERT(ut_decode_accounted_api_versions(&broker, downgrade,
                         sizeof(downgrade), &apis, &count) == RD_KAFKA_RESP_ERR_UNSUPPORTED_VERSION &&
                         count == 1 && apis && apis[0].MaxVer == 2 &&
                         rd_kafka_byte_account_used(account) == entry,
                     "downgrade result retains the advertised range");
        rd_kafka_ApiVersions_destroy(&rk, apis, count);
        RD_UT_ASSERT(ut_decode_accounted_api_versions(&broker, empty,
                         sizeof(empty), &apis, &count) == RD_KAFKA_RESP_ERR_NO_ERROR &&
                         apis && !count && rd_kafka_byte_account_used(account) == 1,
                     "present empty range table retains its allocation");
        rd_kafka_ApiVersions_destroy(&rk, apis, count);
        RD_UT_ASSERT(ut_decode_accounted_api_versions(&broker, success,
                         sizeof(success), &apis, &count) == RD_KAFKA_RESP_ERR_NO_ERROR &&
                         count == 2 && apis && apis[0].ApiKey == RD_KAFKAP_Metadata &&
                         apis[1].ApiKey == RD_KAFKAP_ApiVersion &&
                         rd_kafka_byte_account_used(account) == 2 * entry,
                     "successful arrays retain sorted payload storage");
        rd_kafka_ApiVersions_destroy(&rk, apis, count);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == 0, "all parse storage released");
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

static int ut_accounted_api_versions_overlap(void) {
        const unsigned char candidate[] = {0, 0, 0, 0, 0, 1, 0, 3, 0, 1, 0, 13};
        const size_t entry = sizeof(struct rd_kafka_ApiVersion);
        rd_kafka_t rk = RD_ZERO_INIT;
        rd_kafka_broker_t broker = RD_ZERO_INIT;
        struct rd_kafka_ApiVersion *old, *apis;
        size_t count;
        int features;
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(2 * entry);

        RD_UT_ASSERT(account, "create overlap account");
        rk.rk_byte_account = account;
        broker.rkb_rk = &rk;
        rk.rk_conf.broker_version_fallback = "3.0.0";
        RD_UT_ASSERT(ut_decode_accounted_api_versions(&broker, candidate,
                         sizeof(candidate), &old, &count) == RD_KAFKA_RESP_ERR_NO_ERROR,
                     "prepare the first admitted generation");
        rd_kafka_broker_set_api_versions(&broker, old, count);
        features = broker.rkb_features;
        RD_UT_ASSERT(rd_kafka_byte_account_acquire(account, 1), "leave less than one entry free");
        RD_UT_ASSERT(ut_decode_accounted_api_versions(&broker, candidate,
                         sizeof(candidate), &apis, &count) == RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE &&
                         !apis && !count, "candidate admission precedes allocation");
        RD_UT_ASSERT(rd_kafka_broker_set_fallback_api_versions(&broker) ==
                         RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE && broker.rkb_ApiVersions == old &&
                         broker.rkb_ApiVersions_cnt == 1 && broker.rkb_features == features &&
                         rd_kafka_byte_account_used(account) == entry + 1,
                     "fallback denial preserves the original generation and features");
        rd_kafka_byte_account_release(account, 1);
        RD_UT_ASSERT(ut_decode_accounted_api_versions(&broker, candidate,
                         sizeof(candidate), &apis, &count) == RD_KAFKA_RESP_ERR_NO_ERROR &&
                         apis != old && rd_kafka_byte_account_used(account) == 2 * entry,
                     "candidate and installed arrays retain separate overlap charges");
        rd_kafka_broker_set_api_versions(&broker, apis, count);
        RD_UT_ASSERT(broker.rkb_ApiVersions == apis && rd_kafka_byte_account_used(account) == entry,
                     "publication transfers ownership and releases the prior array");
        RD_UT_ASSERT(!rd_kafka_broker_set_fallback_api_versions(&broker) &&
                         broker.rkb_ApiVersions_cnt == 1 &&
                         broker.rkb_ApiVersions[0].ApiKey == RD_KAFKAP_ApiVersion &&
                         rd_kafka_byte_account_used(account) == entry,
                     "legacy fallback retains its admitted sorted copy");
        rd_kafka_ApiVersions_destroy(&rk, broker.rkb_ApiVersions, broker.rkb_ApiVersions_cnt);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == 0, "broker final array storage released");
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

int unittest_producer_api_versions_account(void) {
        return ut_accounted_api_versions_decode() + ut_accounted_api_versions_overlap();
}
