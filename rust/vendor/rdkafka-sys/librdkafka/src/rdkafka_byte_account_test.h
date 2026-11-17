static const int8_t ut_metadata_body[] = {1, 0, 0};
static const int8_t ut_metadata_wire[] = {
    0, 0, 0, 16, 0, 3, 0, 13, 0, 0, 0, 0, 0, 1, 'x', 0, 1, 0, 0, 0
};

static size_t ut_frame_header_charge(void) {
        return sizeof(rd_kafka_buf_t) + 2 * RD_ROUNDUP(sizeof(rd_segment_t), 8) +
               RD_KAFKAP_RESHDR_SIZE;
}

static size_t ut_request_charge(void) {
        return sizeof(rd_kafka_buf_t) + 2 * RD_ROUNDUP(sizeof(rd_segment_t), 8) +
               sizeof(ut_metadata_wire);
}

static size_t ut_queue_node_charge(void) {
        return sizeof(rd_kafka_op_t) - sizeof(((rd_kafka_op_t *)0)->rko_u) +
               sizeof(((rd_kafka_op_t *)0)->rko_u.xbuf);
}

static int ut_frame_admission_and_late_correlation(void) {
        rd_kafka_t rk = RD_ZERO_INIT;
        rd_kafka_broker_t rkb = RD_ZERO_INIT;
        const size_t header = ut_frame_header_charge();
        const size_t capacity = 2 * header + 513;
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(capacity);
        rd_kafka_buf_t *first, *second;
        int8_t wire_header[RD_KAFKAP_RESHDR_SIZE] = {0};
        RD_UT_ASSERT(account, "create account");
        RD_UT_ASSERT(rd_kafka_byte_account_acquire(account, capacity - header + 1),
                     "reserve to one below frame header");
        RD_UT_ASSERT(!rd_kafka_recv_buf_new(account), "denied frame");
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == capacity - header + 1,
                     "denied header added no charge");
        rd_kafka_byte_account_release(account, capacity - header + 1);
        first = rd_kafka_recv_buf_new(account);
        second = rd_kafka_recv_buf_new(account);
        RD_UT_ASSERT(first && second, "admit simultaneous uncorrelated headers");
        rd_buf_write(&first->rkbuf_buf, wire_header, sizeof(wire_header));
        RD_UT_ASSERT(!rd_kafka_recv_buf_grow(first, 514), "deny growth by one");
        RD_UT_ASSERT(first->rkbuf_buf.rbuf_segment_cnt == 1 &&
                     rd_kafka_byte_account_used(account) == 2 * header,
                     "failed growth retained original segment and charge");
        RD_UT_ASSERT(rd_kafka_recv_buf_grow(first, 513), "admit exact overlap");
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == capacity &&
                     first->rkbuf_buf.rbuf_segment_cnt == 2, "charged external body");
        first->rkbuf_reshdr.CorrId = 17;
        first->rkbuf_totlen = 513;
        rd_buf_write(&first->rkbuf_buf, NULL, 513);
        rkb.rkb_rk = &rk;
        rkb.rkb_thread = thrd_current();
        rkb.rkb_logname = "unit";
        mtx_init(&rkb.rkb_logname_lock, mtx_plain);
        rd_kafka_bufq_init(&rkb.rkb_waitresps);
        rd_atomic64_init(&rkb.rkb_c.rx_corrid_err, 0);
        rd_kafka_buf_keep(first);
        RD_UT_ASSERT(rd_kafka_req_response(&rkb, first) == -1 &&
                     rd_atomic64_get(&rkb.rkb_c.rx_corrid_err) == 1,
                     "late response discarded after correlation lookup");
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == capacity,
                     "late frame remained charged through extra reference");
        rd_kafka_buf_destroy(second);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == header + 513,
                     "only the discarded partial frame released");
        rd_kafka_buf_destroy(first);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == 0, "last frame released");
        mtx_destroy(&rkb.rkb_logname_lock);
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

struct ut_account_completion {
        int calls;
        rd_kafka_resp_err_t error;
        size_t used;
};

static void ut_account_complete(rd_kafka_t *rk, rd_kafka_broker_t *rkb,
                                rd_kafka_resp_err_t err, rd_kafka_buf_t *response,
                                rd_kafka_buf_t *request, void *opaque) {
        struct ut_account_completion *record = opaque;
        (void)rkb;
        (void)response;
        (void)request;
        record->calls++;
        record->error = err;
        record->used = rd_kafka_byte_account_used(rk->rk_byte_account);
}

static int ut_request_frame_and_queue(rd_kafka_broker_t *rkb, rd_bool_t deny_node) {
        rd_kafka_t *rk = rkb->rkb_rk;
        const size_t request_bytes = ut_request_charge();
        const size_t frame_bytes = ut_frame_header_charge();
        const size_t capacity = request_bytes + frame_bytes +
                                (deny_node ? 0 : ut_queue_node_charge());
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(capacity);
        rd_kafka_buf_t *request, *frame;
        rd_kafka_q_t *queue;
        rd_slice_t reader;
        int8_t wire[sizeof(ut_metadata_wire)];
        struct ut_account_completion record = RD_ZERO_INIT;
        RD_UT_ASSERT(account, "create account");
        rk->rk_byte_account = account;
        RD_UT_ASSERT(!rd_kafka_buf_accounted_metadata(rkb, 13, ut_metadata_body,
                                                      sizeof(ut_metadata_body), &request),
                     "admit Metadata v13 request");
        rd_kafka_buf_finalize(rk, request);
        rd_slice_init_full(&reader, &request->rkbuf_buf);
        RD_UT_ASSERT(rd_slice_read(&reader, wire, sizeof(wire)) == sizeof(wire) &&
                     !memcmp(wire, ut_metadata_wire, sizeof(wire)), "complete request bytes");
        RD_UT_ASSERT(rd_buf_len(&request->rkbuf_buf) == sizeof(wire) &&
                     request->rkbuf_buf.rbuf_segment_cnt == 1 &&
                     rd_kafka_byte_account_used(account) == request_bytes,
                     "headers, tags and body fit the one admitted segment");
        frame = rd_kafka_recv_buf_new(account);
        RD_UT_ASSERT(frame, "admit response frame");
        queue = rd_kafka_q_new(rk);
        rd_kafka_q_disable(queue);
        request->rkbuf_cb = ut_account_complete;
        request->rkbuf_opaque = &record;
        request->rkbuf_replyq.q = rd_kafka_q_keep(queue);
        rd_kafka_buf_keep(request);
        rd_kafka_buf_keep(frame);
        rd_kafka_buf_callback(rk, rkb, RD_KAFKA_RESP_ERR_NO_ERROR, frame, request);
        RD_UT_ASSERT(record.calls == 1 && record.used == capacity &&
                     record.error == (deny_node ? RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE
                                                : RD_KAFKA_RESP_ERR__DESTROY),
                     "exactly one terminal callback while its native owners are live");
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == request_bytes + frame_bytes,
                     "only the queue node released after the callback");
        rd_kafka_q_destroy(queue);
        rd_kafka_buf_destroy(request);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == frame_bytes, "held frame");
        rd_kafka_buf_destroy(frame);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == 0, "last owner released");
        rd_kafka_byte_account_destroy(account);
        rk->rk_byte_account = NULL;
        RD_UT_PASS();
}

static int ut_queue_node_outlives_transferred_request(rd_kafka_broker_t *rkb) {
        rd_kafka_t *rk = rkb->rkb_rk;
        size_t capacity = ut_request_charge() + ut_queue_node_charge();
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(capacity);
        rd_kafka_buf_t *request;
        rd_kafka_op_t *operation;
        RD_UT_ASSERT(account, "create account");
        rk->rk_byte_account = account;
        RD_UT_ASSERT(!rd_kafka_buf_accounted_metadata(rkb, 13, ut_metadata_body,
                            sizeof(ut_metadata_body), &request), "create transfer request");
        operation = rd_kafka_op_new_accounted(RD_KAFKA_OP_SOURCE, RD_KAFKA_OP_XMIT_BUF, account);
        RD_UT_ASSERT(operation, "admit transmit node");
        operation->rko_u.xbuf.rkbuf = request;
        request = operation->rko_u.xbuf.rkbuf;
        operation->rko_u.xbuf.rkbuf = NULL;
        rd_kafka_buf_destroy(request);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == ut_queue_node_charge(),
                     "empty transmit node retains its own charge");
        rd_kafka_op_destroy(operation);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == 0, "last node released");
        rd_kafka_byte_account_destroy(account);
        rk->rk_byte_account = NULL;
        RD_UT_PASS();
}

static int ut_transmit_node(rd_kafka_broker_t *rkb, rd_bool_t retry, rd_bool_t deny_node) {
        rd_kafka_t *rk = rkb->rkb_rk;
        const size_t capacity = ut_request_charge() + (deny_node ? 0 : ut_queue_node_charge());
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(capacity);
        struct ut_account_completion record = RD_ZERO_INIT;
        rd_kafka_buf_t *request;
        RD_UT_ASSERT(account, "create account");
        rk->rk_byte_account = account;
        rkb->rkb_ops = rd_kafka_q_new(rk);
        rd_kafka_q_disable(rkb->rkb_ops);
        if (retry) {
                RD_UT_ASSERT(!rd_kafka_buf_accounted_metadata(rkb, 13, ut_metadata_body,
                            sizeof(ut_metadata_body), &request), "build retry");
                request->rkbuf_cb = ut_account_complete;
                request->rkbuf_opaque = &record;
                rd_kafka_broker_buf_retry(rkb, request);
        } else {
                RD_UT_ASSERT(!rd_kafka_broker_accounted_metadata(rkb, 13, ut_metadata_body,
                            sizeof(ut_metadata_body), NULL, ut_account_complete, &record),
                            "accepted request completed inline");
        }
        RD_UT_ASSERT(record.calls == 1 && record.used == capacity &&
                     record.error == (deny_node ? RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE
                                                : RD_KAFKA_RESP_ERR__DESTROY),
                     "transmit or retry operation released after terminal callback");
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == 0, "transmit resources released");
        rd_kafka_q_destroy(rkb->rkb_ops);
        rd_kafka_byte_account_destroy(account);
        rk->rk_byte_account = NULL;
        RD_UT_PASS();
}

static rd_kafka_resp_err_t ut_release_capacity_before_completion(
    rd_kafka_t *rk, int sockfd, const char *brokername, int32_t brokerid,
    int16_t api_key, int16_t api_version, int32_t correlation, size_t size,
    int64_t elapsed, rd_kafka_resp_err_t error, void *opaque) {
        int *calls = opaque;
        (void)sockfd;
        (void)brokername;
        (void)brokerid;
        (void)api_key;
        (void)api_version;
        (void)correlation;
        (void)size;
        (void)elapsed;
        (void)error;
        if ((*calls)++ == 0)
                rd_kafka_byte_account_release(rk->rk_byte_account, ut_queue_node_charge());
        return RD_KAFKA_RESP_ERR_NO_ERROR;
}

static int ut_transmit_denial_after_capacity_returns(rd_kafka_broker_t *rkb,
                                                    rd_bool_t retry) {
        rd_kafka_t *rk = rkb->rkb_rk;
        const size_t capacity = ut_request_charge() + ut_queue_node_charge();
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(capacity);
        struct ut_account_completion record = RD_ZERO_INIT;
        rd_kafka_buf_t *request;
        rd_kafka_q_t *replies;
        int interceptions = 0;
        RD_UT_ASSERT(account, "create account with room for request and one node");
        rk->rk_byte_account = account;
        rkb->rkb_ops = rd_kafka_q_new(rk);
        replies = rd_kafka_q_new(rk);
        rd_kafka_conf_interceptor_ctor(_RK_GLOBAL, &rk->rk_conf);
        RD_UT_ASSERT(!rd_kafka_interceptor_add_on_response_received(
                         rk, "release-node-capacity", ut_release_capacity_before_completion,
                         &interceptions), "install deterministic capacity release");
        RD_UT_ASSERT(rd_kafka_byte_account_acquire(account, ut_queue_node_charge()),
                     "hold competing reservation until the response interceptor");
        if (retry) {
                RD_UT_ASSERT(!rd_kafka_buf_accounted_metadata(
                                 rkb, 13, ut_metadata_body, sizeof(ut_metadata_body), &request),
                             "build retry while competing reservation is held");
                request->rkbuf_cb = ut_account_complete;
                request->rkbuf_opaque = &record;
                request->rkbuf_orig_replyq.q = rd_kafka_q_keep(replies);
                rd_kafka_broker_buf_retry(rkb, request);
        } else {
                RD_UT_ASSERT(!rd_kafka_broker_accounted_metadata(
                                 rkb, 13, ut_metadata_body, sizeof(ut_metadata_body),
                                 replies, ut_account_complete, &record),
                             "accept transmit while competing reservation is held");
        }
        RD_UT_ASSERT(interceptions == 1 && record.calls == 1 &&
                     record.error == RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE &&
                     record.used == ut_request_charge(),
                     "terminal completion ran inline after node capacity became available");
        RD_UT_ASSERT(rd_kafka_q_len(replies) == 0 && rd_kafka_q_len(rkb->rkb_ops) == 0,
                     "ready queues received no operation after transmit denial");
        RD_UT_ASSERT(replies->rkq_refcnt == 1 &&
                     rd_kafka_byte_account_used(account) == 0,
                     "request and reply queue references released exactly once");
        rd_kafka_conf_interceptor_dtor(_RK_GLOBAL, &rk->rk_conf);
        rd_kafka_q_destroy_owner(replies);
        rd_kafka_q_destroy_owner(rkb->rkb_ops);
        rd_kafka_byte_account_destroy(account);
        rk->rk_byte_account = NULL;
        RD_UT_PASS();
}

static int ut_default_buffers_and_existing_error(rd_kafka_broker_t *rkb) {
        rd_kafka_t *rk = rkb->rkb_rk;
        rd_kafka_buf_t *frame = rd_kafka_recv_buf_new(NULL);
        rd_kafka_buf_t *request;
        rd_kafka_q_t *queue;
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(ut_request_charge());
        struct ut_account_completion record = RD_ZERO_INIT;
        RD_UT_ASSERT(frame && account, "create default frame and explicit request account");
        RD_UT_ASSERT(!frame->rkbuf_byte_account && !frame->rkbuf_byte_charge,
                     "default frame has no account");
        rd_buf_write(&frame->rkbuf_buf, NULL, RD_KAFKAP_RESHDR_SIZE);
        RD_UT_ASSERT(rd_kafka_recv_buf_grow(frame, 513), "default growth succeeds");
        rd_kafka_buf_destroy(frame);
        rk->rk_byte_account = account;
        RD_UT_ASSERT(!rd_kafka_buf_accounted_metadata(rkb, 13, ut_metadata_body,
                            sizeof(ut_metadata_body), &request), "create request at capacity");
        queue = rd_kafka_q_new(rk);
        rd_kafka_q_disable(queue);
        request->rkbuf_replyq.q = rd_kafka_q_keep(queue);
        request->rkbuf_cb = ut_account_complete;
        request->rkbuf_opaque = &record;
        rd_kafka_buf_callback(rk, rkb, RD_KAFKA_RESP_ERR__TIMED_OUT, NULL, request);
        RD_UT_ASSERT(record.calls == 1 && record.error == RD_KAFKA_RESP_ERR__TIMED_OUT,
                     "denied reply node preserved the previous terminal error");
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == 0, "request released");
        rd_kafka_q_destroy(queue);
        rd_kafka_byte_account_destroy(account);
        rk->rk_byte_account = NULL;
        RD_UT_PASS();
}

static int ut_admit_all_bytes(void *account) {
        return rd_kafka_byte_account_acquire(account, SIZE_MAX);
}

static int ut_shared_account_at_size_max(void) {
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(SIZE_MAX);
        thrd_t first, second;
        int a, b;
        RD_UT_ASSERT(account, "create shared account");
        RD_UT_ASSERT(thrd_create(&first, ut_admit_all_bytes, account) == thrd_success,
                     "start first admission");
        RD_UT_ASSERT(thrd_create(&second, ut_admit_all_bytes, account) == thrd_success,
                     "start second admission");
        thrd_join(first, &a);
        thrd_join(second, &b);
        RD_UT_ASSERT(a + b == 1 && rd_kafka_byte_account_used(account) == SIZE_MAX,
                     "one complete reservation won");
        RD_UT_ASSERT(!rd_kafka_byte_account_acquire(account, 1),
                     "full size_t account cannot wrap");
        rd_kafka_byte_account_release(account, SIZE_MAX);
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

static int ut_client_id_lengths_before_admission(rd_kafka_broker_t *rkb) {
        static const int oversized[] = {INT16_MAX + 1, INT_MAX - 2, INT_MAX};
        static const int invalid[] = {-2, INT_MIN};
        static const int valid[] = {RD_KAFKAP_STR_LEN_NULL, 0, INT16_MAX};
        rd_kafka_t *rk = rkb->rkb_rk;
        rd_kafkap_str_t *original = rk->rk_client_id;
        rd_kafkap_str_t synthetic = RD_ZERO_INIT;
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(0);
        rd_kafka_buf_t *request = NULL;
        size_t i;
        RD_UT_ASSERT(account, "create zero-capacity account");
        rk->rk_byte_account = account;
        rk->rk_client_id = &synthetic;
        for (i = 0; i < RD_ARRAYSIZE(oversized); i++) {
                synthetic.len = oversized[i];
                RD_UT_ASSERT(rd_kafka_buf_accounted_metadata(
                                 rkb, 13, ut_metadata_body, sizeof(ut_metadata_body), &request) ==
                             RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE && !request,
                             "reject oversized client ID %d before admission or copying",
                             synthetic.len);
        }
        for (i = 0; i < RD_ARRAYSIZE(invalid); i++) {
                synthetic.len = invalid[i];
                RD_UT_ASSERT(rd_kafka_buf_accounted_metadata(
                                 rkb, 13, ut_metadata_body, sizeof(ut_metadata_body), &request) ==
                             RD_KAFKA_RESP_ERR__INVALID_ARG && !request,
                             "reject invalid negative client ID length %d", synthetic.len);
        }
        for (i = 0; i < RD_ARRAYSIZE(valid); i++) {
                synthetic.len = valid[i];
                RD_UT_ASSERT(rd_kafka_buf_accounted_metadata(
                                 rkb, 13, ut_metadata_body, sizeof(ut_metadata_body), &request) ==
                             RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE && !request,
                             "valid client ID length %d reaches byte admission", synthetic.len);
                RD_UT_ASSERT(rd_kafka_buf_accounted_metadata(
                                 rkb, 13, ut_metadata_body, SIZE_MAX, &request) ==
                             RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE && !request,
                             "body size overflow precedes admission for client ID length %d",
                             synthetic.len);
        }
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == 0,
                     "rejected lengths retained no allocation or charge");
        rk->rk_client_id = original;
        rk->rk_byte_account = NULL;
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

static int ut_release_detached_frame(void *opaque) {
        rd_kafka_buf_destroy((rd_kafka_buf_t *)opaque);
        return 0;
}

static int ut_detached_frame_after_producer_destroy(void) {
        const size_t charge = ut_frame_header_charge();
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(charge);
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        rd_kafka_buf_t *frame;
        rd_kafka_t *rk;
        thrd_t releaser;
        char error[256];
        int status;
        RD_UT_ASSERT(account, "create account");
        rk = rd_kafka_new_accounted_producer(&conf, account, error, sizeof(error));
        RD_UT_ASSERT(rk && !conf, "accounted producer creation: %s", error);
        frame = rd_kafka_recv_buf_new(rk->rk_byte_account);
        RD_UT_ASSERT(frame, "hold a native frame across client destruction");
        rd_kafka_destroy(rk);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == charge,
                     "joined client left live native reference charged");
        RD_UT_ASSERT(thrd_create(&releaser, ut_release_detached_frame, frame) == thrd_success,
                     "start independent native owner release");
        rd_kafka_byte_account_destroy(account);
        thrd_join(releaser, &status);
        RD_UT_ASSERT(status == 0, "frame released before account storage destroyed");
        RD_UT_PASS();
}

int unittest_byte_account(void) {
        rd_kafka_t rk = RD_ZERO_INIT;
        rd_kafka_broker_t rkb = RD_ZERO_INIT;
        rd_kafka_byte_account_t *small = rd_kafka_byte_account_new(ut_request_charge() - 1);
        struct ut_account_completion record = RD_ZERO_INIT;
        int fails = 0;
        RD_UT_ASSERT(small, "create small account");
        rk.rk_client_id = rd_kafkap_str_new("x", -1);
        rk.rk_byte_account = small;
        rkb.rkb_rk = &rk;
        rkb.rkb_logname = "unit";
        mtx_init(&rkb.rkb_logname_lock, mtx_plain);
        rd_refcnt_init(&rkb.rkb_refcnt, 1);
        RD_UT_ASSERT(rd_kafka_broker_accounted_metadata(&rkb, 13, ut_metadata_body,
                            sizeof(ut_metadata_body), NULL, ut_account_complete, &record) ==
                     RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE, "one byte below request limit");
        RD_UT_ASSERT(rd_kafka_broker_accounted_metadata(&rkb, 13, ut_metadata_body,
                            SIZE_MAX, NULL, ut_account_complete, &record) ==
                     RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE, "reject size overflow");
        RD_UT_ASSERT(record.calls == 0 && rd_kafka_byte_account_used(small) == 0,
                     "unsent request did not consume quota or publish completion");
        rd_kafka_byte_account_destroy(small);
        fails += ut_client_id_lengths_before_admission(&rkb);
        fails += ut_detached_frame_after_producer_destroy();
        fails += ut_shared_account_at_size_max();
        fails += ut_default_buffers_and_existing_error(&rkb);
        fails += ut_frame_admission_and_late_correlation();
        fails += ut_request_frame_and_queue(&rkb, rd_false);
        fails += ut_request_frame_and_queue(&rkb, rd_true);
        fails += ut_queue_node_outlives_transferred_request(&rkb);
        fails += ut_transmit_node(&rkb, rd_false, rd_false);
        fails += ut_transmit_node(&rkb, rd_false, rd_true);
        fails += ut_transmit_node(&rkb, rd_true, rd_false);
        fails += ut_transmit_node(&rkb, rd_true, rd_true);
        fails += ut_transmit_denial_after_capacity_returns(&rkb, rd_false);
        fails += ut_transmit_denial_after_capacity_returns(&rkb, rd_true);
        RD_UT_ASSERT(rd_refcnt_sub(&rkb.rkb_refcnt) == 0, "last reference to broker");
        rd_refcnt_destroy(&rkb.rkb_refcnt);
        mtx_destroy(&rkb.rkb_logname_lock);
        rd_kafkap_str_destroy(rk.rk_client_id);
        return fails;
}
