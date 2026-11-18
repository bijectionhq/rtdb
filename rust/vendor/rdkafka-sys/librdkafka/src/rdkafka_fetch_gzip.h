/*
 * librd - Rapid Development C library
 *
 * Copyright (c) 2012-2022, Magnus Edenhill
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef _RDKAFKA_FETCH_GZIP_H_
#define _RDKAFKA_FETCH_GZIP_H_

#if WITH_ZLIB
#include <zlib.h>

typedef union rd_kafka_fetch_zlib_owner_u {
        struct {
                rd_kafka_byte_account_t *account;
                size_t charge;
        } allocation;
        long double floating_alignment;
        uint64_t integer_alignment;
        void *pointer_alignment;
} rd_kafka_fetch_zlib_owner_t;

typedef struct rd_kafka_fetch_zlib_allocator_s {
        rd_kafka_byte_account_t *account;
        void *(*allocate)(size_t);
        rd_bool_t allocation_failed;
} rd_kafka_fetch_zlib_allocator_t;

static rd_bool_t rd_kafka_fetch_zlib_size(size_t items,
                                          size_t item_size,
                                          size_t *bytes) {
        size_t payload;
        if (item_size && items > SIZE_MAX / item_size)
                return rd_false;
        payload = items * item_size;
        if (payload > SIZE_MAX - sizeof(rd_kafka_fetch_zlib_owner_t))
                return rd_false;
        *bytes = sizeof(rd_kafka_fetch_zlib_owner_t) + payload;
        return rd_true;
}

static voidpf rd_kafka_fetch_zlib_alloc(voidpf opaque, uInt items, uInt size) {
        rd_kafka_fetch_zlib_allocator_t *allocator = opaque;
        rd_kafka_fetch_zlib_owner_t *owner;
        size_t bytes;
        if (!rd_kafka_fetch_zlib_size(items, size, &bytes) ||
            !rd_kafka_byte_account_acquire(allocator->account, bytes)) {
                allocator->allocation_failed = rd_true;
                return Z_NULL;
        }
        owner = allocator->allocate(bytes);
        if (!owner) {
                rd_kafka_byte_account_release(allocator->account, bytes);
                allocator->allocation_failed = rd_true;
                return Z_NULL;
        }
        owner->allocation.account = allocator->account;
        owner->allocation.charge = bytes;
        return owner + 1;
}

static void rd_kafka_fetch_zlib_free(voidpf opaque, voidpf allocation) {
        rd_kafka_fetch_zlib_owner_t *owner;
        rd_kafka_byte_account_t *account;
        size_t charge;
        (void)opaque;
        if (!allocation)
                return;
        owner = (rd_kafka_fetch_zlib_owner_t *)allocation - 1;
        account = owner->allocation.account;
        charge = owner->allocation.charge;
        rd_free(owner);
        rd_kafka_byte_account_release(account, charge);
}

static rd_bool_t rd_kafka_fetch_gzip_output_size(size_t decoded_size,
                                                 size_t *capacity) {
        if (decoded_size > INT_MAX || decoded_size == SIZE_MAX)
                return rd_false;
        *capacity = decoded_size + 1;
        return rd_true;
}

static rd_kafka_resp_err_t rd_kafka_fetch_gzip(
    rd_kafka_byte_account_t *account,
    const void *input,
    size_t input_size,
    void **output,
    size_t *output_size,
    void *(*allocate)(size_t)) {
        rd_kafka_fetch_zlib_allocator_t allocator = {account, allocate, rd_false};
        char *decoded = NULL;
        size_t decoded_size = 0;
        int pass;
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
        if (input_size > INT_MAX)
                return RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
        for (pass = 1; pass <= 2; pass++) {
                z_stream stream = RD_ZERO_INIT;
                char scratch[512];
                char *cursor;
                int remaining;
                int result;
                size_t measured = 0;
                stream.zalloc = rd_kafka_fetch_zlib_alloc;
                stream.zfree = rd_kafka_fetch_zlib_free;
                stream.opaque = &allocator;
                result = inflateInit2(&stream, 15 + 32);
                if (result != Z_OK) {
                        if (result == Z_MEM_ERROR || allocator.allocation_failed)
                                err = RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
                        goto failed;
                }
                stream.next_in = (void *)input;
                stream.avail_in = (uInt)input_size;
                if (pass == 1) {
                        cursor = scratch;
                        remaining = sizeof(scratch);
                } else {
                        cursor = decoded;
                        remaining = (int)decoded_size;
                }
                do {
                        uInt input_before = stream.avail_in;
                        size_t produced;
                        stream.next_out = (unsigned char *)cursor;
                        stream.avail_out = (uInt)remaining;
                        result = inflate(&stream, Z_NO_FLUSH);
                        switch (result) {
                        case Z_STREAM_ERROR:
                        case Z_NEED_DICT:
                        case Z_DATA_ERROR:
                        case Z_MEM_ERROR:
                                if (result == Z_MEM_ERROR ||
                                    allocator.allocation_failed)
                                        err = RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
                                inflateEnd(&stream);
                                goto failed;
                        }
                        produced = (size_t)remaining - stream.avail_out;
                        if (pass == 1) {
                                if (produced > SIZE_MAX - measured) {
                                        err = RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
                                        inflateEnd(&stream);
                                        goto failed;
                                }
                                measured += produced;
                        } else {
                                cursor += produced;
                                remaining -= (int)produced;
                        }
                        if (!stream.avail_out && result == Z_BUF_ERROR &&
                            input_before == stream.avail_in && !produced) {
                                inflateEnd(&stream);
                                goto failed;
                        }
                } while (!stream.avail_out && result != Z_STREAM_END);
                if (pass == 1) {
                        size_t capacity;
                        if (!rd_kafka_fetch_gzip_output_size(measured, &capacity)) {
                                err = RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
                                inflateEnd(&stream);
                                goto failed;
                        }
                        decoded_size = measured;
                        decoded = rd_kafka_fetch_decode_alloc(account, capacity,
                                                               allocate);
                        if (!decoded) {
                                err = RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
                                inflateEnd(&stream);
                                goto failed;
                        }
                        decoded[decoded_size] = '\0';
                }
                inflateEnd(&stream);
        }
        *output = decoded;
        *output_size = decoded_size;
        return RD_KAFKA_RESP_ERR_NO_ERROR;
failed:
        if (decoded)
                rd_kafka_fetch_decode_free(decoded);
        return err;
}
#endif

#endif
