static int ut_slice_uvarint_case(const unsigned char *bytes,
                                 size_t length,
                                 size_t expected_size,
                                 uint64_t expected_value) {
        size_t split;
        for (split = 0; split <= length; split++) {
                rd_buf_t buffer;
                rd_slice_t slice;
                uint64_t value = UINT64_C(0x123456789abcdef0);
                size_t result;
                rd_buf_init(&buffer, 2, 0);
                if (split)
                        rd_buf_push(&buffer, bytes, split, NULL);
                if (split < length)
                        rd_buf_push(&buffer, bytes + split, length - split, NULL);
                rd_slice_init_full(&slice, &buffer);
                result = rd_slice_read_uvarint(&slice, &value);
                RD_UT_ASSERT(result == expected_size,
                             "length %" PRIusz " split %" PRIusz " result %" PRIusz,
                             length, split, result);
                RD_UT_ASSERT(value == (expected_size ? expected_value :
                                                   UINT64_C(0x123456789abcdef0)),
                             "unexpected value for length %" PRIusz
                             " split %" PRIusz, length, split);
                RD_UT_ASSERT(rd_slice_offset(&slice) == expected_size,
                             "unexpected cursor for length %" PRIusz
                             " split %" PRIusz, length, split);
                if (!expected_size) {
                        int64_t signed_value = INT64_C(0x123456789abcdef0);
                        RD_UT_ASSERT(rd_slice_read_varint(&slice, &signed_value) == 0 &&
                                         signed_value == INT64_C(0x123456789abcdef0) &&
                                         rd_slice_offset(&slice) == 0,
                                     "signed wrapper must preserve failed input/output");
                }
                rd_buf_destroy(&buffer);
        }
        RD_UT_PASS();
}

static int do_unittest_slice_uvarint_bounds(void) {
        unsigned char bytes[11];
        unsigned int terminal;
        size_t length;
        int fails = 0;
        memset(bytes, 0xff, 9);
        for (terminal = 0; terminal <= 255; terminal++) {
                bytes[9] = (unsigned char)terminal;
                fails += ut_slice_uvarint_case(bytes, 10, terminal <= 1 ? 10 : 0,
                                              terminal ? UINT64_MAX :
                                                         UINT64_MAX >> 1);
        }
        memset(bytes, 0x80, sizeof(bytes));
        bytes[10] = 0;
        fails += ut_slice_uvarint_case(bytes, sizeof(bytes), 0, 0);
        for (length = 1; length <= 10; length++) {
                memset(bytes, 0x80, length);
                fails += ut_slice_uvarint_case(bytes, length, 0, 0);
                bytes[length - 1] = 0;
                fails += ut_slice_uvarint_case(bytes, length, length, 0);
        }
        bytes[0] = 0xac;
        bytes[1] = 0x02;
        bytes[2] = 0xff;
        fails += ut_slice_uvarint_case(bytes, 3, 2, 300);
        return fails;
}
