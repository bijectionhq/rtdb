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

static int ut_accounted_api_versions_request(void) {
        const unsigned char body[] = {2, 'n', 2, 'v', 0};
        const size_t fixed = sizeof(rd_kafka_buf_t) + 2 * RD_ROUNDUP(sizeof(rd_segment_t), 8);
        const size_t header = RD_KAFKAP_REQHDR_SIZE + 3;
        const size_t flexible_charge = fixed + header + 1 + sizeof(body);
        rd_kafka_t rk = RD_ZERO_INIT;
        rd_kafka_broker_t broker = RD_ZERO_INIT;
        rd_kafka_buf_t *request;
        rd_slice_t reader;
        unsigned char bytes[64];
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(flexible_charge);

        RD_UT_ASSERT(account, "create handshake account");
#if defined(_TTHREAD_WIN32_)
        RD_UT_ASSERT(DuplicateHandle(GetCurrentProcess(), thrd_current(),
                                    GetCurrentProcess(), &broker.rkb_thread,
                                    0, FALSE, DUPLICATE_SAME_ACCESS), "retain broker thread identity");
#else
        broker.rkb_thread = thrd_current();
#endif
        rk.rk_byte_account = account;
        rk.rk_client_id = rd_kafkap_str_new("x", -1);
        rk.rk_conf.sw_name = "n";
        rk.rk_conf.sw_version = "v";
        rk.rk_conf.socket_timeout_ms = 10000;
        rk.rk_conf.api_version_request_timeout_ms = 1000;
        broker.rkb_rk = &rk;
        rd_kafka_bufq_init(&broker.rkb_outbufs);
        rd_refcnt_init(&broker.rkb_refcnt, 1);

        RD_UT_ASSERT(!rd_kafka_ApiVersionRequest(&broker, -1, RD_KAFKA_NO_REPLYQ, NULL, NULL),
                     "highest handshake version admits exact retained storage");
        request = TAILQ_FIRST(&broker.rkb_outbufs.rkbq_bufs);
        RD_UT_ASSERT(request && request->rkbuf_reqhdr.ApiKey == RD_KAFKAP_ApiVersion &&
                         request->rkbuf_reqhdr.ApiVersion == 3 &&
                         request->rkbuf_prio == RD_KAFKA_PRIO_FLASH &&
                         request->rkbuf_max_retries == RD_KAFKA_REQUEST_NO_RETRIES &&
                         request->rkbuf_abs_timeout > 0,
                     "preserve actual handshake request policy");
        reader = request->rkbuf_reader;
        RD_UT_ASSERT(rd_slice_remains(&reader) == header + 1 + sizeof(body) &&
                         rd_slice_read(&reader, bytes, header + 1 + sizeof(body)) == header + 1 + sizeof(body) &&
                         bytes[header] == 0 && !memcmp(bytes + header + 1, body, sizeof(body)),
                     "exact flexible header tags and compact software identity");
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == flexible_charge &&
                         rd_kafka_ApiVersionRequest(&broker, 0, RD_KAFKA_NO_REPLYQ, NULL, NULL) ==
                             RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE &&
                         rd_kafka_bufq_cnt(&broker.rkb_outbufs) == 1,
                     "retained flexible request blocks a second handshake allocation");
        rd_kafka_bufq_deq(&broker.rkb_outbufs, request);
        rd_kafka_buf_destroy(request);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == 0 &&
                         !rd_kafka_ApiVersionRequest(&broker, 0, RD_KAFKA_NO_REPLYQ, NULL, NULL),
                     "legacy handshake can proceed after final flexible release");
        request = TAILQ_FIRST(&broker.rkb_outbufs.rkbq_bufs);
        RD_UT_ASSERT(request && request->rkbuf_reqhdr.ApiVersion == 0 &&
                         rd_slice_remains(&request->rkbuf_reader) == header &&
                         rd_kafka_byte_account_used(account) == fixed + header,
                     "legacy retry has no software fields or flexible tags");
        rd_kafka_bufq_deq(&broker.rkb_outbufs, request);
        rd_kafka_buf_destroy(request);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == 0 && rd_refcnt_sub(&broker.rkb_refcnt) == 0,
                     "handshake queue drains its request and broker references");
        rd_refcnt_destroy(&broker.rkb_refcnt);
        rd_kafkap_str_destroy(rk.rk_client_id);
        rd_kafka_byte_account_destroy(account);
#if defined(_TTHREAD_WIN32_)
        RD_UT_ASSERT(CloseHandle(broker.rkb_thread), "release retained broker thread identity");
#endif
        RD_UT_PASS();
}

int unittest_producer_api_versions_account(void) {
        return ut_accounted_api_versions_decode() + ut_accounted_api_versions_overlap() +
               ut_accounted_api_versions_request();
}
