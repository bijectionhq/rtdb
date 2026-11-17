static void rd_kafka_ordered_metadata_complete(
    rd_kafka_t *rk, rd_kafka_broker_t *rkb, rd_kafka_resp_err_t error,
    rd_kafka_buf_t *response, rd_kafka_buf_t *request, void *opaque);

static rd_bool_t rd_kafka_ordered_header_uvarint(rd_slice_t *reader,
                                                uint32_t *value) {
        unsigned int shift;
        uint32_t bits = 0;
        for (shift = 0; shift <= 28; shift += 7) {
                unsigned char byte;
                if (rd_slice_read(reader, &byte, 1) != 1)
                        return rd_false;
                bits |= (uint32_t)(byte & 127) << shift;
                if (!(byte & 128)) {
                        *value = bits;
                        return rd_true;
                }
        }
        return rd_false;
}

static rd_bool_t rd_kafka_ordered_metadata_header(rd_kafka_buf_t *response) {
        rd_slice_t reader = response->rkbuf_reader;
        uint32_t count, i;
        if (!(response->rkbuf_flags & RD_KAFKA_OP_F_FLEXVER))
                return rd_true;
        if (!rd_kafka_ordered_header_uvarint(&reader, &count) ||
            count > INT32_MAX || count > rd_slice_remains(&reader) ||
            count > 10000)
                return rd_false;
        for (i = 0; i < count; i++) {
                uint32_t tag, size;
                if (!rd_kafka_ordered_header_uvarint(&reader, &tag) ||
                    !rd_kafka_ordered_header_uvarint(&reader, &size) ||
                    size > INT32_MAX || size > rd_slice_remains(&reader) ||
                    rd_slice_read(&reader, NULL, size) != size)
                        return rd_false;
        }
        response->rkbuf_reader = reader;
        return rd_true;
}
