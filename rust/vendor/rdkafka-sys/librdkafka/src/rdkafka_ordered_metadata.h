struct rd_kafka_ordered_metadata_s {
        mtx_t lock;
        rd_kafka_byte_account_t *account;
        size_t charge;
        size_t capacity;
        size_t length;
        int references;
        int ready;
        int16_t version;
        rd_kafka_resp_err_t error;
};

static rd_kafka_ordered_metadata_t *
rd_kafka_ordered_metadata_new(rd_kafka_byte_account_t *account, size_t capacity) {
        rd_kafka_ordered_metadata_t *result;
        size_t charge;
        if (capacity > SIZE_MAX - sizeof(*result))
                return NULL;
        charge = sizeof(*result) + capacity;
        if (!rd_kafka_byte_account_acquire(account, charge))
                return NULL;
        result = calloc(1, charge);
        if (!result)
                goto fail;
        if (mtx_init(&result->lock, mtx_plain) != thrd_success) {
                free(result);
                goto fail;
        }
        result->account = account;
        result->charge = charge;
        result->capacity = capacity;
        result->references = 1;
        return result;
fail:
        rd_kafka_byte_account_release(account, charge);
        return NULL;
}

static void rd_kafka_ordered_metadata_keep(rd_kafka_ordered_metadata_t *result) {
        mtx_lock(&result->lock);
        result->references++;
        mtx_unlock(&result->lock);
}

void rd_kafka_ordered_metadata_destroy(rd_kafka_ordered_metadata_t *result) {
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

int rd_kafka_ordered_metadata_poll(rd_kafka_ordered_metadata_t *result,
                                   rd_kafka_resp_err_t *error, int16_t *version,
                                   const void **body, size_t *length) {
        int ready;
        mtx_lock(&result->lock);
        ready = result->ready;
        if (ready) {
                *error = result->error;
                *version = result->version;
                *length = result->length;
                *body = result + 1;
        }
        mtx_unlock(&result->lock);
        return ready;
}

static void rd_kafka_ordered_metadata_complete(
    rd_kafka_t *rk, rd_kafka_broker_t *rkb, rd_kafka_resp_err_t error,
    rd_kafka_buf_t *response, rd_kafka_buf_t *request, void *opaque) {
        rd_kafka_ordered_metadata_t *result = opaque;
        size_t length = 0;
        (void)rk;
        if ((error == RD_KAFKA_RESP_ERR__TRANSPORT ||
             error == RD_KAFKA_RESP_ERR__TIMED_OUT ||
             error == RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT) &&
            rd_kafka_buf_retry(rkb, request))
                return;
        if (!error) {
                if (!response) {
                        error = RD_KAFKA_RESP_ERR__BAD_MSG;
                } else {
                        rd_slice_t reader = response->rkbuf_reader;
                        length = rd_slice_remains(&reader);
                        if (length > result->capacity)
                                error = RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE;
                        else if (rd_slice_read(&reader, result + 1, length) != length)
                                error = RD_KAFKA_RESP_ERR__BAD_MSG;
                }
        }
        mtx_lock(&result->lock);
        result->error = error;
        result->version = request->rkbuf_reqhdr.ApiVersion;
        result->length = error ? 0 : length;
        result->ready = 1;
        mtx_unlock(&result->lock);
        rd_kafka_ordered_metadata_destroy(result);
}

static size_t rd_kafka_ordered_varint_size(uint32_t value) {
        size_t size = 1;
        while (value >= 128) {
                value >>= 7;
                size++;
        }
        return size;
}

static unsigned char *rd_kafka_ordered_varint(unsigned char *out, uint32_t value) {
        while (value >= 128) {
                *out++ = (unsigned char)(value | 128);
                value >>= 7;
        }
        *out++ = (unsigned char)value;
        return out;
}

static rd_kafka_resp_err_t rd_kafka_ordered_body_size(
    const rd_kafka_ordered_metadata_topic_t *topics, size_t count, int16_t version,
    size_t *size) {
        size_t i;
        size_t total;
        if (count > INT32_MAX || version < 0 || version > 13 ||
            (count == 0 ? topics != NULL || version == 0 : topics == NULL))
                return RD_KAFKA_RESP_ERR__INVALID_ARG;
        total = version >= 9 ? rd_kafka_ordered_varint_size((uint32_t)count + 1) : 4;
        total += (version >= 4) + (version >= 8) + (version >= 8 && version <= 10);
        for (i = 0; i < count; i++) {
                size_t length = topics[i].length;
                size_t item;
                if (!topics[i].name || !length || length > INT16_MAX)
                        return RD_KAFKA_RESP_ERR__INVALID_ARG;
                item = length + (version >= 9 ? rd_kafka_ordered_varint_size((uint32_t)length + 1) + 1 : 2);
                if (version >= 10)
                        item += 16;
                if (item > (size_t)INT32_MAX - total)
                        return RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE;
                total += item;
        }
        *size = total;
        return RD_KAFKA_RESP_ERR_NO_ERROR;
}

static void rd_kafka_ordered_body_write(unsigned char *out,
                                         const rd_kafka_ordered_metadata_topic_t *topics,
                                         size_t count, int16_t version,
                                         rd_bool_t auto_create) {
        size_t i;
        if (version >= 9)
                out = rd_kafka_ordered_varint(out, (uint32_t)count + 1);
        else {
                uint32_t n = (uint32_t)count;
                *out++ = (unsigned char)(n >> 24);
                *out++ = (unsigned char)(n >> 16);
                *out++ = (unsigned char)(n >> 8);
                *out++ = (unsigned char)n;
        }
        for (i = 0; i < count; i++) {
                size_t length = topics[i].length;
                if (version >= 10) {
                        memset(out, 0, 16);
                        out += 16;
                }
                if (version >= 9)
                        out = rd_kafka_ordered_varint(out, (uint32_t)length + 1);
                else {
                        *out++ = (unsigned char)(length >> 8);
                        *out++ = (unsigned char)length;
                }
                memcpy(out, topics[i].name, length);
                out += length;
                if (version >= 9)
                        *out++ = 0;
        }
        if (version >= 4)
                *out++ = (unsigned char)auto_create;
        if (version >= 8 && version <= 10)
                *out++ = 0;
        if (version >= 8)
                *out = 0;
}

static rd_kafka_resp_err_t rd_kafka_ordered_metadata_submit(
    rd_kafka_broker_t *rkb, const rd_kafka_ordered_metadata_topic_t *topics, size_t count,
    int16_t version, size_t body_capacity, int timeout_ms,
    rd_kafka_ordered_metadata_t **output) {
        rd_kafka_byte_account_t *account = rkb->rkb_rk->rk_byte_account;
        rd_kafka_ordered_metadata_t *result;
        rd_kafka_resp_err_t error;
        size_t size;
        size_t request_charge;
        unsigned char *body;
        *output = NULL;
        error = rd_kafka_ordered_body_size(topics, count, version, &size);
        if (error)
                return error;
        error = rd_kafka_buf_accounted_metadata_size(rkb, version, size, &request_charge);
        if (error)
                return error;
        result = rd_kafka_ordered_metadata_new(account, body_capacity);
        if (!result)
                return RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
        if (!rd_kafka_byte_account_acquire(account, size)) {
                rd_kafka_ordered_metadata_destroy(result);
                return RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
        }
        body = malloc(size);
        if (!body) {
                error = RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
                goto release;
        }
        rd_kafka_ordered_body_write(body, topics, count, version,
                                   rkb->rkb_rk->rk_conf.allow_auto_create_topics);
        rd_kafka_ordered_metadata_keep(result);
        error = rd_kafka_broker_accounted_metadata_timeout(
            rkb, version, body, size, rkb->rkb_rk->rk_ops,
            rd_kafka_ordered_metadata_complete, result, timeout_ms);
        if (error)
                rd_kafka_ordered_metadata_destroy(result);
        free(body);
release:
        rd_kafka_byte_account_release(account, size);
        if (error)
                rd_kafka_ordered_metadata_destroy(result);
        else
                *output = result;
        return error;
}

static int16_t rd_kafka_ordered_metadata_version(rd_kafka_broker_t *rkb,
                                                  rd_bool_t brokers_only) {
        int16_t minimum = rkb->rkb_rk->rk_conf.allow_auto_create_topics
                              ? (brokers_only ? 1 : 0)
                              : 4;
        return rd_kafka_broker_ApiVersion_supported(
            rkb, RD_KAFKAP_Metadata, minimum, 13, NULL);
}

static rd_kafka_broker_t *rd_kafka_ordered_metadata_find_broker(
    rd_kafka_t *rk, int32_t node_id) {
        rd_kafka_broker_t *rkb;
        rd_kafka_rdlock(rk);
        rkb = rd_kafka_broker_find_by_nodeid0(rk, node_id,
                                             RD_KAFKA_BROKER_STATE_UP, rd_true);
        rd_kafka_rdunlock(rk);
        if (rkb && rd_kafka_broker_or_instance_terminating(rkb)) {
                rd_kafka_broker_destroy(rkb);
                return NULL;
        }
        return rkb;
}

static rd_kafka_resp_err_t rd_kafka_ordered_metadata_start0(
    rd_kafka_t *rk, const rd_kafka_ordered_metadata_topic_t *topics, size_t count,
    rd_bool_t brokers_only, const int32_t *node_id, size_t body_capacity, int timeout_ms,
    rd_kafka_ordered_metadata_t **result) {
        rd_kafka_broker_t *rkb;
        rd_kafka_resp_err_t error;
        int16_t version;
        size_t ignored;
        if (!result)
                return RD_KAFKA_RESP_ERR__INVALID_ARG;
        *result = NULL;
        if (!rk || timeout_ms <= 0 || body_capacity > INT32_MAX)
                return RD_KAFKA_RESP_ERR__INVALID_ARG;
        if (rk->rk_type != RD_KAFKA_PRODUCER || !rk->rk_byte_account)
                return RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE;
        if ((!brokers_only && count == 0) || (node_id && *node_id < 0))
                return RD_KAFKA_RESP_ERR__INVALID_ARG;
        error = rd_kafka_ordered_body_size(topics, count, 13, &ignored);
        if (error)
                return error;
        if (node_id)
                rkb = rd_kafka_ordered_metadata_find_broker(rk, *node_id);
        else
                rkb = rd_kafka_broker_any_usable(rk, RD_POLL_NOWAIT, rd_true, 0,
                                                "ordered Metadata request");
        if (!rkb)
                return RD_KAFKA_RESP_ERR__TRANSPORT;
        version = rd_kafka_ordered_metadata_version(rkb, brokers_only);
        if (version < 0)
                error = RD_KAFKA_RESP_ERR__UNSUPPORTED_FEATURE;
        else
                error = rd_kafka_ordered_metadata_submit(
                    rkb, topics, count, version, body_capacity, timeout_ms, result);
        rd_kafka_broker_destroy(rkb);
        return error;
}

rd_kafka_resp_err_t rd_kafka_ordered_metadata_start(
    rd_kafka_t *rk, const rd_kafka_ordered_metadata_topic_t *topics, size_t count,
    size_t body_capacity, int timeout_ms, rd_kafka_ordered_metadata_t **result) {
        return rd_kafka_ordered_metadata_start0(
            rk, topics, count, rd_false, NULL, body_capacity, timeout_ms, result);
}

rd_kafka_resp_err_t rd_kafka_ordered_metadata_start_brokers(
    rd_kafka_t *rk, size_t body_capacity, int timeout_ms,
    rd_kafka_ordered_metadata_t **result) {
        return rd_kafka_ordered_metadata_start0(
            rk, NULL, 0, rd_true, NULL, body_capacity, timeout_ms, result);
}

rd_kafka_resp_err_t rd_kafka_ordered_metadata_start_at(
    rd_kafka_t *rk, int32_t node_id, const rd_kafka_ordered_metadata_topic_t *topics,
    size_t count, size_t body_capacity, int timeout_ms,
    rd_kafka_ordered_metadata_t **result) {
        return rd_kafka_ordered_metadata_start0(
            rk, topics, count, count == 0, &node_id, body_capacity, timeout_ms, result);
}
