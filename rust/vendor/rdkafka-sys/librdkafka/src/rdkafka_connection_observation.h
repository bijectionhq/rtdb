struct rd_kafka_connection_observation_s {
        mtx_t lock;
        rd_kafka_byte_account_t *account;
        rd_kafka_t *origin;
        size_t charge;
        int references;
        int ready;
        rd_kafka_resp_err_t error;
        rd_kafka_connection_snapshot_t snapshot;
};

static rd_kafka_connection_observation_t *rd_kafka_connection_observation_new(
    rd_kafka_t *rk, void *(*allocate)(size_t, size_t), int (*init)(mtx_t *, int)) {
        rd_kafka_connection_observation_t *result;
        size_t charge = sizeof(*result);
        if (!rd_kafka_byte_account_acquire(rk->rk_byte_account, charge))
                return NULL;
        result = allocate(1, charge);
        if (!result)
                goto fail;
        if (init(&result->lock, mtx_plain) != thrd_success) {
                free(result);
                goto fail;
        }
        result->account = rk->rk_byte_account;
        result->origin = rk;
        result->charge = charge;
        result->references = 1;
        return result;
fail:
        rd_kafka_byte_account_release(rk->rk_byte_account, charge);
        return NULL;
}

static void rd_kafka_connection_observation_keep(rd_kafka_connection_observation_t *result) {
        mtx_lock(&result->lock);
        result->references++;
        mtx_unlock(&result->lock);
}

void rd_kafka_connection_observation_destroy(rd_kafka_connection_observation_t *result) {
        rd_kafka_byte_account_t *account;
        size_t charge;
        int last;
        if (!result)
                return;
        mtx_lock(&result->lock);
        last = --result->references == 0;
        mtx_unlock(&result->lock);
        if (!last)
                return;
        account = result->account;
        charge = result->charge;
        mtx_destroy(&result->lock);
        free(result);
        rd_kafka_byte_account_release(account, charge);
}

static void rd_kafka_connection_observation_publish(
    rd_kafka_connection_observation_t *result, rd_kafka_resp_err_t error,
    const rd_kafka_connection_snapshot_t *snapshot) {
        mtx_lock(&result->lock);
        if (!result->ready) {
                if (snapshot)
                        result->snapshot = *snapshot;
                result->error = error;
                result->ready = 1;
        }
        mtx_unlock(&result->lock);
}

void rd_kafka_connection_observation_cancel(rd_kafka_connection_observation_t *result) {
        rd_kafka_connection_observation_publish(result, RD_KAFKA_RESP_ERR__DESTROY, NULL);
}

int rd_kafka_connection_observation_poll(rd_kafka_connection_observation_t *result,
    rd_kafka_resp_err_t *error, rd_kafka_connection_snapshot_t *snapshot) {
        int ready;
        mtx_lock(&result->lock);
        ready = result->ready;
        if (ready) {
                *error = result->error;
                *snapshot = result->snapshot;
        }
        mtx_unlock(&result->lock);
        return ready;
}

void rd_kafka_connection_observation_capture(
    rd_kafka_broker_t *rkb, rd_kafka_connection_observation_t *result) {
        rd_kafka_connection_snapshot_t snapshot = RD_ZERO_INIT;
        rd_kafka_buf_t *head;
        rd_kafka_assert(rkb->rkb_rk, thrd_is_current(rkb->rkb_thread));
        if (rd_kafka_broker_or_instance_terminating(rkb)) {
                rd_kafka_connection_observation_cancel(result);
                return;
        }
        rd_kafka_broker_lock(rkb);
        snapshot.owner_serial = rkb->rkb_owner_serial;
        snapshot.connection_generation = rkb->rkb_connection_generation;
        snapshot.node_id = rkb->rkb_nodeid;
        snapshot.source_code = rkb->rkb_source;
        snapshot.state_code = rkb->rkb_state;
        snapshot.nodename_epoch = rkb->rkb_nodename_epoch;
        snapshot.connect_epoch = rkb->rkb_connect_epoch;
        rd_kafka_broker_unlock(rkb);
        snapshot.observed_at_us = rd_clock();
        snapshot.last_connect_attempt_us = rkb->rkb_ts_connect;
        snapshot.reconnect_at_us = rkb->rkb_ts_reconnect;
        snapshot.outgoing_count = rd_kafka_bufq_cnt(&rkb->rkb_outbufs);
        snapshot.awaiting_response_count = rd_kafka_bufq_cnt(&rkb->rkb_waitresps);
        snapshot.retry_count = rd_kafka_bufq_cnt(&rkb->rkb_retrybufs);
        snapshot.max_inflight = rkb->rkb_max_inflight;
        snapshot.transport_present = rkb->rkb_transport != NULL;
        head = TAILQ_FIRST(&rkb->rkb_outbufs.rkbq_bufs);
        snapshot.partially_sent = head && rd_slice_offset(&head->rkbuf_reader) != 0;
        rd_kafka_connection_observation_publish(result, RD_KAFKA_RESP_ERR_NO_ERROR, &snapshot);
}

static rd_kafka_resp_err_t rd_kafka_connection_observe_submit(
    rd_kafka_t *rk, rd_kafka_broker_t *rkb, rd_kafka_connection_observation_t **output) {
        rd_kafka_connection_observation_t *result;
        rd_kafka_op_t *op;
        if (!rkb)
                return RD_KAFKA_RESP_ERR__TRANSPORT;
        if (!rkb->rkb_owner_serial) {
                rd_kafka_broker_destroy(rkb);
                return RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE;
        }
        result = rd_kafka_connection_observation_new(rk, calloc, mtx_init);
        if (!result) {
                rd_kafka_broker_destroy(rkb);
                return RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
        }
        op = rd_kafka_op_new_accounted(RD_KAFKA_OP_SOURCE,
                                       RD_KAFKA_OP_CONNECTION_OBSERVATION, rk->rk_byte_account);
        if (!op) {
                rd_kafka_connection_observation_destroy(result);
                rd_kafka_broker_destroy(rkb);
                return RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
        }
        rd_kafka_connection_observation_keep(result);
        op->rko_u.connection_observation.result = result;
        op->rko_u.connection_observation.broker = rkb;
        *output = result;
        rd_kafka_q_enq(rkb->rkb_ops, op);
        return RD_KAFKA_RESP_ERR_NO_ERROR;
}

static rd_kafka_resp_err_t rd_kafka_connection_observe_validate(
    rd_kafka_t *rk, rd_kafka_connection_observation_t **result) {
        if (!result)
                return RD_KAFKA_RESP_ERR__INVALID_ARG;
        *result = NULL;
        if (!rk)
                return RD_KAFKA_RESP_ERR__INVALID_ARG;
        if (rk->rk_type != RD_KAFKA_PRODUCER || !rk->rk_byte_account)
                return RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE;
        return RD_KAFKA_RESP_ERR_NO_ERROR;
}

rd_kafka_resp_err_t rd_kafka_connection_observe_at(
    rd_kafka_t *rk, int32_t node_id, rd_kafka_connection_observation_t **result) {
        rd_kafka_resp_err_t error = rd_kafka_connection_observe_validate(rk, result);
        rd_kafka_broker_t *rkb;
        if (error)
                return error;
        if (node_id < 0)
                return RD_KAFKA_RESP_ERR__INVALID_ARG;
        rd_kafka_rdlock(rk);
        rkb = rd_kafka_broker_find_by_nodeid(rk, node_id);
        rd_kafka_rdunlock(rk);
        if (rkb && (rkb->rkb_source != RD_KAFKA_LEARNED ||
                    rd_kafka_broker_or_instance_terminating(rkb))) {
                rd_kafka_broker_destroy(rkb);
                rkb = NULL;
        }
        return rd_kafka_connection_observe_submit(rk, rkb, result);
}

rd_kafka_resp_err_t rd_kafka_connection_observe_endpoint(
    rd_kafka_t *rk, const char *host_bytes, size_t length, uint16_t port,
    rd_kafka_connection_observation_t **result) {
        rd_kafka_resp_err_t error = rd_kafka_connection_observe_validate(rk, result);
        rd_kafka_broker_t *rkb, *selected = NULL;
        char host[RD_KAFKA_NODENAME_SIZE], nodename[RD_KAFKA_NODENAME_SIZE];
        int formatted;
        if (error)
                return error;
        if (!rk->rk_conf.ordered_metadata_bootstrap)
                return RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE;
        if (!host_bytes || !length || length >= sizeof(host) || memchr(host_bytes, 0, length))
                return RD_KAFKA_RESP_ERR__INVALID_ARG;
        memcpy(host, host_bytes, length);
        host[length] = 0;
        formatted = rd_snprintf(nodename, sizeof(nodename), "%s:%hu", host, port);
        if (formatted < 0 || (size_t)formatted >= sizeof(nodename))
                return RD_KAFKA_RESP_ERR__INVALID_ARG;
        rd_kafka_rdlock(rk);
        TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
                if (rkb->rkb_source != RD_KAFKA_CONFIGURED || rd_kafka_broker_or_instance_terminating(rkb))
                        continue;
                rd_kafka_broker_lock(rkb);
                if (rkb->rkb_port == port && !strcmp(rkb->rkb_origname, host) &&
                    !strcmp(rkb->rkb_nodename, nodename)) {
                        rd_kafka_broker_keep(rkb);
                        selected = rkb;
                }
                rd_kafka_broker_unlock(rkb);
                if (selected)
                        break;
        }
        rd_kafka_rdunlock(rk);
        return rd_kafka_connection_observe_submit(rk, selected, result);
}

rd_kafka_resp_err_t rd_kafka_ordered_metadata_start_observed(
    rd_kafka_t *rk, rd_kafka_connection_observation_t *observation,
    const rd_kafka_ordered_metadata_topic_t *topics, size_t count,
    size_t body_capacity, int timeout_ms, rd_kafka_ordered_metadata_t **output) {
        rd_kafka_connection_snapshot_t snapshot;
        rd_kafka_broker_t *rkb, *selected = NULL;
        rd_kafka_resp_err_t error;
        int16_t version = -1;
        size_t ignored;
        if (!output)
                return RD_KAFKA_RESP_ERR__INVALID_ARG;
        *output = NULL;
        if (!rk || !observation || observation->origin != rk || timeout_ms <= 0 || body_capacity > INT32_MAX)
                return RD_KAFKA_RESP_ERR__INVALID_ARG;
        if (!rd_kafka_connection_observation_poll(observation, &error, &snapshot))
                return RD_KAFKA_RESP_ERR__IN_PROGRESS;
        if (error)
                return error;
        if (!snapshot.owner_serial || !snapshot.connection_generation || snapshot.connection_generation == UINT64_MAX)
                return RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE;
        if (snapshot.state_code != RD_KAFKA_BROKER_STATE_UP || !snapshot.transport_present)
                return RD_KAFKA_RESP_ERR__TRANSPORT;
        error = rd_kafka_ordered_body_size(topics, count, 13, &ignored);
        if (error)
                return error;
        rd_kafka_rdlock(rk);
        TAILQ_FOREACH(rkb, &rk->rk_brokers, rkb_link) {
                if (rkb->rkb_owner_serial != snapshot.owner_serial ||
                    (rkb->rkb_source != RD_KAFKA_LEARNED && rkb->rkb_source != RD_KAFKA_CONFIGURED) ||
                    rd_kafka_broker_or_instance_terminating(rkb))
                        continue;
                rd_kafka_broker_lock(rkb);
                if (rkb->rkb_state == RD_KAFKA_BROKER_STATE_UP &&
                    rkb->rkb_nodeid == snapshot.node_id && rkb->rkb_source == snapshot.source_code &&
                    rkb->rkb_connection_generation == snapshot.connection_generation &&
                    rkb->rkb_nodename_epoch == snapshot.nodename_epoch &&
                    rkb->rkb_connect_epoch == snapshot.connect_epoch &&
                    rkb->rkb_connect_epoch == rkb->rkb_nodename_epoch) {
                        rd_kafka_broker_keep(rkb);
                        selected = rkb;
                        version = rd_kafka_ordered_metadata_version0(rkb, count == 0, rd_false);
                }
                rd_kafka_broker_unlock(rkb);
                break;
        }
        rd_kafka_rdunlock(rk);
        if (!selected)
                return RD_KAFKA_RESP_ERR__TRANSPORT;
        if (version < 0)
                error = RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE;
        else
                error = rd_kafka_ordered_metadata_submit_on_connection(selected, topics, count,
                    version, body_capacity, timeout_ms, snapshot.connection_generation, output);
        rd_kafka_broker_destroy(selected);
        return error;
}
