int unittest_connection_observation(void);
extern int unittest_connection_op_allocation(void);

struct ut_connection_fixture {
        rd_kafka_t rk;
        rd_kafka_broker_t broker;
        struct rd_kafka_ApiVersion api;
};

static int ut_connection_init_owner(struct ut_connection_fixture *f, size_t capacity,
                                    rd_kafka_confsource_t source, uint64_t owner_serial) {
        memset(f, 0, sizeof(*f));
        rwlock_init(&f->rk.rk_lock);
        if (mtx_init(&f->broker.rkb_lock, mtx_plain) != thrd_success)
                goto fail_lock;
        if (mtx_init(&f->broker.rkb_logname_lock, mtx_plain) != thrd_success)
                goto fail_broker_mutex;
        f->broker.rkb_logname = "connection-unit";
#if defined(_TTHREAD_WIN32_)
        if (!DuplicateHandle(GetCurrentProcess(), thrd_current(), GetCurrentProcess(),
                             &f->broker.rkb_thread, 0, FALSE, DUPLICATE_SAME_ACCESS))
                goto fail_log_mutex;
#else
        f->broker.rkb_thread = thrd_current();
#endif
        f->rk.rk_type = RD_KAFKA_PRODUCER;
        f->rk.rk_byte_account = rd_kafka_byte_account_new(capacity);
        if (!f->rk.rk_byte_account)
                goto fail_thread;
        f->rk.rk_client_id = rd_kafkap_str_new("x", -1);
        f->rk.rk_conf.allow_auto_create_topics = rd_true;
        f->rk.rk_conf.ordered_metadata_bootstrap = rd_true;
        TAILQ_INIT(&f->rk.rk_brokers);
        rd_list_init(&f->rk.rk_broker_by_id, 1, NULL);
        f->broker.rkb_rk = &f->rk;
        f->broker.rkb_source = source;
        f->broker.rkb_nodeid = 7;
        f->broker.rkb_owner_serial = owner_serial;
        f->broker.rkb_connection_generation = 3;
        f->broker.rkb_nodename_epoch = 2;
        f->broker.rkb_connect_epoch = 2;
        f->broker.rkb_state = RD_KAFKA_BROKER_STATE_UP;
        f->broker.rkb_max_inflight = 5;
        f->broker.rkb_ts_connect = 1234;
        f->broker.rkb_ts_reconnect = 5678;
        f->broker.rkb_origname = "host";
        f->broker.rkb_port = 9092;
        rd_strlcpy(f->broker.rkb_nodename, "host:9092", sizeof(f->broker.rkb_nodename));
        f->api.ApiKey = RD_KAFKAP_Metadata;
        f->api.MinVer = 1;
        f->api.MaxVer = 13;
        f->broker.rkb_ApiVersions = &f->api;
        f->broker.rkb_ApiVersions_cnt = 1;
        f->broker.rkb_ops = rd_kafka_q_new(&f->rk);
        rd_refcnt_init(&f->broker.rkb_refcnt, 1);
        rd_kafka_bufq_init(&f->broker.rkb_outbufs);
        rd_kafka_bufq_init(&f->broker.rkb_waitresps);
        rd_kafka_bufq_init(&f->broker.rkb_retrybufs);
        TAILQ_INSERT_TAIL(&f->rk.rk_brokers, &f->broker, rkb_link);
        rd_list_add(&f->rk.rk_broker_by_id, &f->broker);
        return 0;
fail_thread:
#if defined(_TTHREAD_WIN32_)
        CloseHandle(f->broker.rkb_thread);
fail_log_mutex:
#endif
        mtx_destroy(&f->broker.rkb_logname_lock);
fail_broker_mutex:
        mtx_destroy(&f->broker.rkb_lock);
fail_lock:
        rwlock_destroy(&f->rk.rk_lock);
        return 1;
}

static int ut_connection_init_source(struct ut_connection_fixture *f, size_t capacity, rd_kafka_confsource_t source) {
        return ut_connection_init_owner(f, capacity, source, 1);
}

static int ut_connection_init(struct ut_connection_fixture *f, size_t capacity) {
        return ut_connection_init_source(f, capacity, RD_KAFKA_LEARNED);
}

static int ut_connection_finish(struct ut_connection_fixture *f) {
        rd_kafka_q_purge(f->broker.rkb_ops);
        RD_UT_ASSERT(rd_kafka_byte_account_used(f->rk.rk_byte_account) == 0 &&
                         rd_refcnt_sub(&f->broker.rkb_refcnt) == 0,
                     "all control, result and target references released");
        rd_refcnt_destroy(&f->broker.rkb_refcnt);
        rd_kafka_q_destroy(f->broker.rkb_ops);
        rd_list_destroy(&f->rk.rk_broker_by_id);
        rd_kafkap_str_destroy(f->rk.rk_client_id);
        rd_kafka_byte_account_destroy(f->rk.rk_byte_account);
        mtx_destroy(&f->broker.rkb_logname_lock);
        mtx_destroy(&f->broker.rkb_lock);
        rwlock_destroy(&f->rk.rk_lock);
#if defined(_TTHREAD_WIN32_)
        RD_UT_ASSERT(CloseHandle(f->broker.rkb_thread), "release stable actor identity");
#endif
        RD_UT_PASS();
}

static int ut_connection_serve(struct ut_connection_fixture *f) {
        rd_kafka_op_t *op = rd_kafka_q_pop(f->broker.rkb_ops, 0, 0);
        RD_UT_ASSERT(op && op->rko_type == RD_KAFKA_OP_CONNECTION_OBSERVATION,
                     "actual accounted control reaches its owning actor");
        RD_UT_ASSERT(!rd_kafka_broker_op_serve(&f->broker, op), "read-only observation does not initiate a connection");
        RD_UT_PASS();
}

static void *ut_connection_allocate_denied(size_t count, size_t size) {
        (void)count;
        (void)size;
        return NULL;
}
static int ut_connection_mutex_denied(mtx_t *lock, int flags) {
        (void)lock;
        (void)flags;
        return thrd_error;
}

static int ut_connection_admission(void) {
        const size_t result_charge = sizeof(rd_kafka_connection_observation_t);
        const size_t op_charge = sizeof(rd_kafka_op_t) - sizeof(((rd_kafka_op_t *)0)->rko_u) +
            sizeof(((rd_kafka_op_t *)0)->rko_u.connection_observation);
        struct ut_connection_fixture f;
        rd_kafka_connection_observation_t *result;
        int stage;
        for (stage = 0; stage < 2; stage++) {
                RD_UT_ASSERT(!ut_connection_init(&f, result_charge - 1 + (stage ? op_charge : 0)), "create threshold fixture");
                result = NULL;
                RD_UT_ASSERT(rd_kafka_connection_observe_at(&f.rk, 7, &result) ==
                                 RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE && !result &&
                                 rd_kafka_byte_account_used(f.rk.rk_byte_account) == 0 &&
                                 rd_refcnt_get(&f.broker.rkb_refcnt) == 1,
                             "result and op denial undo every earlier owner");
                RD_UT_ASSERT(!ut_connection_finish(&f), "threshold cleanup");
        }
        RD_UT_ASSERT(!ut_connection_init(&f, result_charge), "create fallible allocation fixture");
        RD_UT_ASSERT(!rd_kafka_connection_observation_new(&f.rk, ut_connection_allocate_denied, mtx_init) &&
                         rd_kafka_byte_account_used(f.rk.rk_byte_account) == 0,
                     "record allocation failure releases its prior charge");
        RD_UT_ASSERT(!rd_kafka_connection_observation_new(&f.rk, calloc, ut_connection_mutex_denied) &&
                         rd_kafka_byte_account_used(f.rk.rk_byte_account) == 0,
                     "mutex failure frees allocation before returning its charge");
        RD_UT_ASSERT(!ut_connection_finish(&f), "allocation cleanup");
        RD_UT_PASS();
}

static int ut_connection_publication_and_cancellation(void) {
        struct ut_connection_fixture f;
        rd_kafka_connection_observation_t *result;
        rd_kafka_ordered_metadata_t *request;
        rd_kafka_connection_snapshot_t snapshot = RD_ZERO_INIT, again;
        rd_kafka_resp_err_t error = RD_KAFKA_RESP_ERR_NO_ERROR;
        RD_UT_ASSERT(!ut_connection_init(&f, 65536), "create actor fixture");
        RD_UT_ASSERT(!rd_kafka_connection_observe_at(&f.rk, 7, &result), "queue observation");
        snapshot.owner_serial = 99;
        RD_UT_ASSERT(!rd_kafka_connection_observation_poll(result, &error, &snapshot) && snapshot.owner_serial == 99,
                     "pending poll leaves output unchanged");
        RD_UT_ASSERT(rd_refcnt_get(&f.broker.rkb_refcnt) == 2, "only pending control holds temporary target reference");
        RD_UT_ASSERT(!ut_connection_serve(&f), "serve observation");
        RD_UT_ASSERT(rd_kafka_connection_observation_poll(result, &error, &snapshot) && !error &&
                         snapshot.owner_serial == 1 && snapshot.connection_generation == 3 &&
                         snapshot.state_code == RD_KAFKA_BROKER_STATE_UP && !snapshot.transport_present &&
                         snapshot.last_connect_attempt_us == 1234 && snapshot.reconnect_at_us == 5678 &&
                         snapshot.max_inflight == 5 && snapshot.awaiting_response_count == 0 &&
                         rd_refcnt_get(&f.broker.rkb_refcnt) == 1 &&
                         rd_kafka_byte_account_used(f.rk.rk_byte_account) == sizeof(*result),
                     "immutable native facts retain storage without retaining the broker or claiming channel readiness");
        RD_UT_ASSERT(rd_kafka_ordered_metadata_start_observed(&f.rk, result, NULL, 0, 64, 1000, &request) ==
                         RD_KAFKA_RESP_ERR__TRANSPORT && !request,
                     "native UP without transport cannot qualify a socket");
        rd_kafka_broker_lock(&f.broker);
        f.broker.rkb_connection_generation = 4;
        rd_kafka_broker_unlock(&f.broker);
        rd_kafka_connection_observation_cancel(result);
        RD_UT_ASSERT(rd_kafka_connection_observation_poll(result, &error, &again) && !error &&
                         again.connection_generation == snapshot.connection_generation &&
                         again.observed_at_us == snapshot.observed_at_us,
                     "later state and cancellation cannot rewrite a ready snapshot");
        rd_kafka_connection_observation_destroy(result);
        RD_UT_ASSERT(!rd_kafka_connection_observe_at(&f.rk, 7, &result), "queue abandoned observation");
        rd_kafka_connection_observation_destroy(result);
        rd_kafka_q_purge(f.broker.rkb_ops);
        RD_UT_ASSERT(rd_kafka_byte_account_used(f.rk.rk_byte_account) == 0 &&
                         rd_refcnt_get(&f.broker.rkb_refcnt) == 1, "purge completes abandoned control release");
        rd_kafka_q_disable(f.broker.rkb_ops);
        RD_UT_ASSERT(!rd_kafka_connection_observe_at(&f.rk, 7, &result) &&
                         rd_kafka_connection_observation_poll(result, &error, &snapshot) &&
                         error == RD_KAFKA_RESP_ERR__DESTROY && rd_refcnt_get(&f.broker.rkb_refcnt) == 1,
                     "disabled queue publishes cancellation inline with a live caller reference");
        rd_kafka_connection_observation_destroy(result);
        RD_UT_ASSERT(!ut_connection_finish(&f), "publication cleanup");
        RD_UT_PASS();
}

static int ut_connection_exact_owner_and_socket(void) {
        struct ut_connection_fixture f;
        rd_kafka_connection_observation_t *observation, *queued;
        rd_kafka_ordered_metadata_t *request;
        rd_kafka_connection_snapshot_t snapshot;
        rd_kafka_resp_err_t error;
        rd_kafka_t other = RD_ZERO_INIT;
        rd_kafka_broker_t replacement = RD_ZERO_INIT;
        const void *body;
        size_t length;
        int16_t version;
        RD_UT_ASSERT(!ut_connection_init(&f, 65536), "create target fixture");
        f.broker.rkb_transport = (rd_kafka_transport_t *)&f;
        RD_UT_ASSERT(!rd_kafka_connection_observe_at(&f.rk, 7, &observation), "queue target snapshot");
        RD_UT_ASSERT(rd_kafka_ordered_metadata_start_observed(&f.rk, observation, NULL, 0, 64, 1000, &request) ==
                         RD_KAFKA_RESP_ERR__IN_PROGRESS && !request, "pending authority cannot submit");
        RD_UT_ASSERT(!ut_connection_serve(&f), "capture actual actor fields");
        RD_UT_ASSERT(rd_kafka_connection_observation_poll(observation, &error, &snapshot) && !error, "captured target");
        RD_UT_ASSERT(rd_kafka_ordered_metadata_start_observed(&other, observation, NULL, 0, 64, 1000, &request) ==
                         RD_KAFKA_RESP_ERR__INVALID_ARG && !request, "client domains cannot be substituted");
        replacement.rkb_rk = &f.rk;
        replacement.rkb_owner_serial = 2;
        replacement.rkb_nodeid = 7;
        replacement.rkb_source = RD_KAFKA_LEARNED;
        replacement.rkb_connection_generation = snapshot.connection_generation;
        rd_kafka_wrlock(&f.rk);
        TAILQ_REMOVE(&f.rk.rk_brokers, &f.broker, rkb_link);
        TAILQ_INSERT_TAIL(&f.rk.rk_brokers, &replacement, rkb_link);
        rd_kafka_wrunlock(&f.rk);
        RD_UT_ASSERT(rd_kafka_ordered_metadata_start_observed(&f.rk, observation, NULL, 0, 64, 1000, &request) ==
                         RD_KAFKA_RESP_ERR__TRANSPORT && !request, "same ID and socket number on a replacement owner do not qualify");
        rd_kafka_wrlock(&f.rk);
        TAILQ_REMOVE(&f.rk.rk_brokers, &replacement, rkb_link);
        TAILQ_INSERT_TAIL(&f.rk.rk_brokers, &f.broker, rkb_link);
        rd_kafka_wrunlock(&f.rk);
        rd_kafka_broker_lock(&f.broker);
        f.broker.rkb_nodename_epoch++;
        rd_kafka_broker_unlock(&f.broker);
        RD_UT_ASSERT(rd_kafka_ordered_metadata_start_observed(&f.rk, observation, NULL, 0, 64, 1000, &request) ==
                         RD_KAFKA_RESP_ERR__TRANSPORT && !request, "changed advertised epoch is stale");
        rd_kafka_broker_lock(&f.broker);
        f.broker.rkb_nodename_epoch--;
        rd_kafka_broker_unlock(&f.broker);
        RD_UT_ASSERT(!rd_kafka_ordered_metadata_start_observed(&f.rk, observation, NULL, 0, 64, 1000, &request),
                     "exact observed socket accepts broker-only metadata");
        RD_UT_ASSERT(rd_kafka_bufq_cnt(&f.broker.rkb_outbufs) == 1 &&
                         !rd_slice_seek(&TAILQ_FIRST(&f.broker.rkb_outbufs.rkbq_bufs)->rkbuf_reader, 1),
                     "outgoing head has a concrete partial-send offset");
        RD_UT_ASSERT(!rd_kafka_connection_observe_at(&f.rk, 7, &queued) && !ut_connection_serve(&f) &&
                         rd_kafka_connection_observation_poll(queued, &error, &snapshot) && !error &&
                         snapshot.outgoing_count == 1 && snapshot.partially_sent && snapshot.transport_present,
                     "broker actor samples actual outgoing head and native transport presence");
        rd_kafka_connection_observation_destroy(queued);
        rd_kafka_broker_lock(&f.broker);
        f.broker.rkb_connection_generation++;
        rd_kafka_broker_unlock(&f.broker);
        RD_UT_ASSERT(rd_kafka_send(&f.broker) == 0 && rd_kafka_bufq_cnt(&f.broker.rkb_outbufs) == 0,
                     "actual actor send fence rejects reconnect before transport access");
        RD_UT_ASSERT(rd_kafka_ordered_metadata_poll(request, &error, &version, &body, &length) &&
                         error == RD_KAFKA_RESP_ERR__TRANSPORT && !length,
                     "connection-qualified failure is terminal without retry");
        rd_kafka_ordered_metadata_destroy(request);
        rd_kafka_connection_observation_destroy(observation);
        f.broker.rkb_transport = NULL;
        RD_UT_ASSERT(!ut_connection_finish(&f), "target cleanup");
        RD_UT_PASS();
}

static int ut_connection_serial_and_endpoint(void) {
        struct ut_connection_fixture f;
        rd_kafka_connection_observation_t *observation = NULL;
        rd_kafka_ordered_metadata_t *request;
        int stage;
        RD_UT_ASSERT(!ut_connection_init_owner(&f, 65536, RD_KAFKA_LEARNED, 0), "create unqualified serial fixture");
        rd_kafka_wrlock(&f.rk);
        f.rk.rk_broker_owner_serial = UINT64_MAX - 1;
        RD_UT_ASSERT(rd_kafka_broker_next_owner_serial(&f.rk) == UINT64_MAX &&
                         rd_kafka_broker_next_owner_serial(&f.rk) == 0 &&
                         f.rk.rk_broker_owner_serial == UINT64_MAX,
                     "owner serial exhaustion never wraps or reuses a prior owner");
        rd_kafka_wrunlock(&f.rk);
        RD_UT_ASSERT(rd_kafka_connection_observe_at(&f.rk, 99, &observation) == RD_KAFKA_RESP_ERR__TRANSPORT &&
                         !observation, "missing ID does not observe another ready broker");
        RD_UT_ASSERT(rd_kafka_connection_observe_at(&f.rk, 7, &observation) ==
                         RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE && !observation,
                     "unqualified owner serial is explicit unsupported state");
        RD_UT_ASSERT(!ut_connection_finish(&f), "finish exhausted learned-owner fixture");
        RD_UT_ASSERT(!ut_connection_init_source(&f, 65536, RD_KAFKA_CONFIGURED), "create configured endpoint fixture");
        f.rk.rk_conf.ordered_metadata_bootstrap = rd_false;
        RD_UT_ASSERT(rd_kafka_connection_observe_endpoint(&f.rk, "host", 4, 9092, &observation) ==
                         RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE && !observation,
                     "configured retention is an explicit bootstrap precondition");
        f.rk.rk_conf.ordered_metadata_bootstrap = rd_true;
        RD_UT_ASSERT(rd_kafka_connection_observe_endpoint(&f.rk, "HOST", 4, 9092, &observation) ==
                         RD_KAFKA_RESP_ERR__TRANSPORT && !observation, "endpoint matching has no case or ID fallback");
        for (stage = 0; stage < 2; stage++) {
                rd_kafka_broker_lock(&f.broker);
                f.broker.rkb_connection_generation = stage ? UINT64_MAX : 0;
                rd_kafka_broker_unlock(&f.broker);
                RD_UT_ASSERT(!rd_kafka_connection_observe_endpoint(&f.rk, "host", 4, 9092, &observation) &&
                                 !ut_connection_serve(&f), "capture unqualified native socket state");
                RD_UT_ASSERT(rd_kafka_ordered_metadata_start_observed(&f.rk, observation, NULL, 0, 64, 1000, &request) ==
                                 RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE && !request,
                             "zero and exhausted sockets are never converted to unfenced requests");
                rd_kafka_connection_observation_destroy(observation);
        }
        RD_UT_ASSERT(!ut_connection_finish(&f), "serial cleanup");
        RD_UT_PASS();
}

static int ut_connection_real_decommission(void) {
        rd_kafka_conf_t *conf = rd_kafka_conf_new();
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(1 << 20);
        rd_kafka_t *rk;
        rd_kafka_broker_t *rkb;
        rd_kafka_connection_observation_t *observation;
        rd_kafka_connection_snapshot_t snapshot, again;
        rd_kafka_resp_err_t error;
        rd_ts_t deadline;
        thrd_t broker_thread = RD_ZERO_INIT;
        int owns_thread = 0;
        int present;
        char message[256];
        RD_UT_ASSERT(account && rd_kafka_conf_set(conf, "bootstrap.servers", "127.0.0.1:0", message, sizeof(message)) ==
                         RD_KAFKA_CONF_OK && rd_kafka_conf_set(conf, "enable.ordered.metadata.bootstrap", "true", message, sizeof(message)) ==
                         RD_KAFKA_CONF_OK, "configure real native actor without an external broker");
        rk = rd_kafka_new_accounted_producer(&conf, account, message, sizeof(message));
        RD_UT_ASSERT(rk && !conf, "start actual accounted native producer");
        RD_UT_ASSERT(!rd_kafka_connection_observe_endpoint(rk, "127.0.0.1", 9, 0, &observation), "queue on real configured actor");
        deadline = rd_clock() + 5000000;
        while (!rd_kafka_connection_observation_poll(observation, &error, &snapshot) && rd_clock() < deadline)
                rd_usleep(1000, NULL);
        RD_UT_ASSERT(rd_kafka_connection_observation_poll(observation, &error, &snapshot) && !error && snapshot.owner_serial,
                     "real broker publishes a qualified native owner serial");
        rd_kafka_wrlock(rk);
        TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
                if (rkb->rkb_owner_serial == snapshot.owner_serial) {
                        broker_thread = rkb->rkb_thread;
                        rd_kafka_broker_decommission(rk, rkb, NULL);
                        owns_thread = 1;
                        break;
                }
        }
        rd_kafka_wrunlock(rk);
        deadline = rd_clock() + 5000000;
        do {
                present = 0;
                rd_kafka_rdlock(rk);
                TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link)
                        if (rkb->rkb_owner_serial == snapshot.owner_serial) present = 1;
                rd_kafka_rdunlock(rk);
                if (present) rd_usleep(1000, NULL);
        } while (present && rd_clock() < deadline);
        RD_UT_ASSERT(!present && rd_kafka_connection_observation_poll(observation, &error, &again) && !error &&
                         again.owner_serial == snapshot.owner_serial && again.observed_at_us == snapshot.observed_at_us,
                     "completed retained observation does not prevent actual broker decommission");
        rd_kafka_connection_observation_destroy(observation);
        RD_UT_ASSERT(owns_thread && thrd_join(broker_thread, NULL) == thrd_success,
                     "join the original broker thread before client-owned state is destroyed");
        rd_kafka_destroy(rk);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == 0, "native teardown drains all independent controls and responses");
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}

int unittest_connection_observation(void) {
        return unittest_connection_op_allocation() + ut_connection_admission() +
            ut_connection_publication_and_cancellation() + ut_connection_exact_owner_and_socket() +
            ut_connection_serial_and_endpoint() + ut_connection_real_decommission();
}
