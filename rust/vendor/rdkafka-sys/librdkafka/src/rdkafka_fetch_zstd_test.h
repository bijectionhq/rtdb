#if WITH_ZSTD
static char *ut_fetch_zstd_frame(const char *payload,
                                 size_t size,
                                 rd_bool_t content_size,
                                 size_t *frame_size) {
        size_t capacity = ZSTD_compressBound(size);
        ZSTD_CCtx *context = ZSTD_createCCtx();
        char *frame = malloc(capacity);
        if (!context || !frame ||
            ZSTD_isError(ZSTD_CCtx_setParameter(context, ZSTD_c_contentSizeFlag,
                                                content_size ? 1 : 0))) {
                ZSTD_freeCCtx(context);
                free(frame);
                return NULL;
        }
        *frame_size = ZSTD_compress2(context, frame, capacity, payload, size);
        ZSTD_freeCCtx(context);
        if (ZSTD_isError(*frame_size)) {
                free(frame);
                return NULL;
        }
        return frame;
}

static char *ut_fetch_zstd_payload(size_t size, rd_bool_t repetitive) {
        char *payload = malloc(size);
        size_t i;
        if (!payload)
                return NULL;
        for (i = 0; i < size; i++)
                payload[i] = repetitive ? 'x' : (char)((i * 7919) ^ (i >> 5));
        return payload;
}

static int ut_fetch_zstd_allocator_boundaries(void) {
        size_t bytes, unchanged = 71;
        rd_kafka_byte_account_t *exact, *short_account;
        rd_kafka_fetch_zstd_allocator_t allocator;
        void *block;
        RD_UT_ASSERT(rd_kafka_fetch_zstd_size(16, &bytes), "measure a sixteen-byte context block");
        RD_UT_ASSERT(!rd_kafka_fetch_zstd_size(SIZE_MAX, &unchanged) && unchanged == 71,
                     "context size overflow leaves its size result untouched");
        exact = rd_kafka_byte_account_new(bytes);
        short_account = rd_kafka_byte_account_new(bytes - 1);
        RD_UT_ASSERT(exact && short_account, "create bounded context accounts");

        allocator = (rd_kafka_fetch_zstd_allocator_t){short_account, malloc, rd_false};
        RD_UT_ASSERT(!rd_kafka_fetch_zstd_alloc(&allocator, 16) && allocator.allocation_failed &&
                         !rd_kafka_byte_account_used(short_account),
                     "one-byte-short context admission returns no block or charge");

        allocator = (rd_kafka_fetch_zstd_allocator_t){exact, ut_fetch_decode_no_malloc, rd_false};
        RD_UT_ASSERT(!rd_kafka_fetch_zstd_alloc(&allocator, 16) && allocator.allocation_failed &&
                         !rd_kafka_byte_account_used(exact),
                     "context malloc failure returns the acquired charge");

        allocator = (rd_kafka_fetch_zstd_allocator_t){exact, malloc, rd_false};
        block = rd_kafka_fetch_zstd_alloc(&allocator, 16);
        RD_UT_ASSERT(block && !allocator.allocation_failed &&
                         rd_kafka_byte_account_used(exact) == bytes,
                     "exact context admission charges prefix and request");
        RD_UT_ASSERT((uintptr_t)block % sizeof(rd_kafka_fetch_zstd_owner_t) == 0,
                     "context blocks keep the owner's alignment");
        memset(block, 0x5a, 16);
        rd_kafka_fetch_zstd_free(&allocator, NULL);
        rd_kafka_fetch_zstd_free(&allocator, block);
        RD_UT_ASSERT(!rd_kafka_byte_account_used(exact), "context free returns its charge");
        rd_kafka_byte_account_destroy(exact);
        rd_kafka_byte_account_destroy(short_account);
        RD_UT_PASS();
}

static int ut_fetch_zstd_round_trip(rd_bool_t content_size,
                                    rd_bool_t repetitive,
                                    rd_bool_t expect_growth) {
        const size_t size = 256 << 10;
        char *payload = ut_fetch_zstd_payload(size, repetitive);
        size_t frame_size, output_size = 0;
        char *frame = payload ? ut_fetch_zstd_frame(payload, size, content_size, &frame_size) : NULL;
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(16 << 20);
        rd_atomic64_t growths;
        void *output = NULL;
        RD_UT_ASSERT(payload && frame && account, "create zstd fixture");
        rd_atomic64_init(&growths, 0);
        RD_UT_ASSERT(!rd_kafka_fetch_zstd(account, frame, frame_size, 4 << 20, &growths,
                                          &output, &output_size, malloc),
                     "accounted zstd decodes the frame");
        RD_UT_ASSERT(output_size == size && !memcmp(output, payload, size),
                     "accounted zstd output matches the producer payload");
        RD_UT_ASSERT((rd_atomic64_get(&growths) > 0) == expect_growth,
                     "growth follows the frame's declared content size");
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) >= size,
                     "the retained output stays charged after the context is freed");
        rd_kafka_fetch_decode_free(output);
        RD_UT_ASSERT(!rd_kafka_byte_account_used(account), "output free returns every charge");
        rd_kafka_byte_account_destroy(account);
        free(frame);
        free(payload);
        RD_UT_PASS();
}

static int ut_fetch_zstd_each_denial_returns_every_charge(void) {
        const size_t size = 256 << 10;
        char *payload = ut_fetch_zstd_payload(size, rd_true);
        size_t frame_size, capacity, output_size;
        char *frame = payload ? ut_fetch_zstd_frame(payload, size, rd_false, &frame_size) : NULL;
        rd_atomic64_t growths;
        rd_bool_t decoded = rd_false;
        RD_UT_ASSERT(payload && frame, "create zstd denial fixture");
        rd_atomic64_init(&growths, 0);
        for (capacity = 0; capacity <= (4 << 20) && !decoded; capacity += 4096) {
                rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(capacity);
                void *output = NULL;
                rd_kafka_resp_err_t err;
                RD_UT_ASSERT(account, "create zstd denial account");
                err = rd_kafka_fetch_zstd(account, frame, frame_size, 4 << 20, &growths,
                                          &output, &output_size, malloc);
                if (err) {
                        RD_UT_ASSERT(err == RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE && !output,
                                     "zstd denial at %" PRIusz " bytes is a resource error", capacity);
                } else {
                        RD_UT_ASSERT(output_size == size && !memcmp(output, payload, size),
                                     "the first admitted capacity decodes the frame");
                        rd_kafka_fetch_decode_free(output);
                        decoded = rd_true;
                }
                RD_UT_ASSERT(!rd_kafka_byte_account_used(account),
                             "zstd at %" PRIusz " bytes returns every charge", capacity);
                rd_kafka_byte_account_destroy(account);
        }
        RD_UT_ASSERT(decoded, "a bounded account eventually decodes the frame");
        free(frame);
        free(payload);
        RD_UT_PASS();
}

static int ut_fetch_zstd_rejects_invalid_and_oversized_frames(void) {
        const size_t size = 64 << 10;
        char *payload = ut_fetch_zstd_payload(size, rd_false);
        size_t frame_size, output_size;
        char *frame = payload ? ut_fetch_zstd_frame(payload, size, rd_true, &frame_size) : NULL;
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(4 << 20);
        rd_atomic64_t growths;
        void *output = NULL;
        RD_UT_ASSERT(payload && frame && account, "create invalid zstd fixture");
        rd_atomic64_init(&growths, 0);
        RD_UT_ASSERT(rd_kafka_fetch_zstd(account, frame, frame_size, size - 1, &growths,
                                         &output, &output_size, malloc) ==
                             RD_KAFKA_RESP_ERR__BAD_COMPRESSION &&
                         !output,
                     "a declared size above the receive limit is rejected");
        RD_UT_ASSERT(rd_kafka_fetch_zstd(account, frame, frame_size - 1, 4 << 20, &growths,
                                         &output, &output_size, malloc) ==
                             RD_KAFKA_RESP_ERR__BAD_COMPRESSION &&
                         !output,
                     "a truncated frame is rejected");
        frame[0] ^= 0xff;
        RD_UT_ASSERT(rd_kafka_fetch_zstd(account, frame, frame_size, 4 << 20, &growths,
                                         &output, &output_size, malloc) ==
                         RD_KAFKA_RESP_ERR__BAD_COMPRESSION,
                     "a bad magic number is rejected");
        RD_UT_ASSERT(!rd_kafka_byte_account_used(account), "rejected frames return every charge");
        rd_kafka_byte_account_destroy(account);
        free(frame);
        free(payload);
        RD_UT_PASS();
}
#endif
