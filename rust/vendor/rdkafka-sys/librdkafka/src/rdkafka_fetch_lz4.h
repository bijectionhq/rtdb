/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2017-2022, Magnus Edenhill
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

#ifndef _RDKAFKA_FETCH_LZ4_H_
#define _RDKAFKA_FETCH_LZ4_H_

#if !WITH_LZ4_EXT
#ifndef LZ4F_STATIC_LINKING_ONLY
#define LZ4F_STATIC_LINKING_ONLY
#define RD_KAFKA_FETCH_LZ4_UNDEF_STATIC_LINKING
#endif
#include "lz4frame.h"
#ifdef RD_KAFKA_FETCH_LZ4_UNDEF_STATIC_LINKING
#undef LZ4F_STATIC_LINKING_ONLY
#undef RD_KAFKA_FETCH_LZ4_UNDEF_STATIC_LINKING
#endif
#include "rdxxhash.h"

typedef union rd_kafka_fetch_lz4_owner_u {
        struct {
                rd_kafka_byte_account_t *account;
                size_t charge;
        } allocation;
        long double floating_alignment;
        uint64_t integer_alignment;
        void *pointer_alignment;
} rd_kafka_fetch_lz4_owner_t;

typedef struct rd_kafka_fetch_lz4_allocator_s {
        rd_kafka_byte_account_t *account;
        void *(*allocate)(size_t);
        rd_bool_t allocation_failed;
} rd_kafka_fetch_lz4_allocator_t;

static rd_bool_t rd_kafka_fetch_lz4_size(size_t capacity, size_t *bytes) {
        if (capacity > SIZE_MAX - sizeof(rd_kafka_fetch_lz4_owner_t))
                return rd_false;
        *bytes = sizeof(rd_kafka_fetch_lz4_owner_t) + capacity;
        return rd_true;
}

static void *rd_kafka_fetch_lz4_alloc(void *opaque, size_t capacity) {
        rd_kafka_fetch_lz4_allocator_t *allocator = opaque;
        rd_kafka_fetch_lz4_owner_t *owner;
        size_t bytes;
        if (!rd_kafka_fetch_lz4_size(capacity, &bytes) ||
            !rd_kafka_byte_account_acquire(allocator->account, bytes)) {
                allocator->allocation_failed = rd_true;
                return NULL;
        }
        owner = allocator->allocate(bytes);
        if (!owner) {
                rd_kafka_byte_account_release(allocator->account, bytes);
                allocator->allocation_failed = rd_true;
                return NULL;
        }
        owner->allocation.account = allocator->account;
        owner->allocation.charge = bytes;
        return owner + 1;
}

static void rd_kafka_fetch_lz4_free(void *opaque, void *allocation) {
        rd_kafka_fetch_lz4_owner_t *owner;
        rd_kafka_byte_account_t *account;
        size_t charge;
        (void)opaque;
        if (!allocation)
                return;
        owner = (rd_kafka_fetch_lz4_owner_t *)allocation - 1;
        account = owner->allocation.account;
        charge = owner->allocation.charge;
        rd_free(owner);
        rd_kafka_byte_account_release(account, charge);
}

static rd_kafka_resp_err_t rd_kafka_fetch_lz4_fix_hc(char *input,
                                                    size_t input_size) {
        static const unsigned char magic[] = {0x04, 0x22, 0x4d, 0x18};
        size_t offset = 6;
        uint8_t checksum;
        if (input_size < 7 || memcmp(input, magic, sizeof(magic)))
                return RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
        if (((uint8_t)input[4] >> 3) & 1)
                offset += 8;
        if (offset >= input_size)
                return RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
        checksum = (XXH32(input + 4, offset - 4, 0) >> 8) & 0xff;
        if ((uint8_t)input[offset] != checksum)
                input[offset] = (char)checksum;
        return RD_KAFKA_RESP_ERR_NO_ERROR;
}

static rd_bool_t rd_kafka_fetch_lz4_estimate(unsigned long long content_size,
                                             size_t input_size,
                                             size_t max_message_size,
                                             size_t *capacity) {
        rd_bool_t unknown = !content_size;
        if (!unknown) {
                if (input_size > SIZE_MAX / 255)
                        return rd_false;
                unknown = content_size > input_size * 255;
        }
        if (unknown) {
                if (input_size > SIZE_MAX / 4)
                        return rd_false;
                *capacity = RD_MIN(input_size * 4, max_message_size);
        } else {
                if (content_size > SIZE_MAX)
                        return rd_false;
                *capacity = (size_t)content_size;
        }
        return rd_true;
}

static rd_bool_t rd_kafka_fetch_lz4_growth(size_t capacity,
                                           size_t *next_capacity) {
        size_t extra;
        if (capacity > SIZE_MAX / 3)
                return rd_false;
        extra = RD_MAX(capacity * 3 / 4, 1024);
        if (capacity > SIZE_MAX - extra)
                return rd_false;
        *next_capacity = capacity + extra;
        return rd_true;
}

static rd_kafka_resp_err_t rd_kafka_fetch_lz4(
    rd_kafka_byte_account_t *account,
    int proper_hc,
    char *input,
    size_t input_size,
    size_t max_message_size,
    rd_atomic64_t *growths,
    void **output,
    size_t *output_size,
    void *(*allocate)(size_t)) {
        rd_kafka_fetch_lz4_allocator_t allocator = {account, allocate, rd_false};
        LZ4F_CustomMem memory = {rd_kafka_fetch_lz4_alloc, NULL,
                                 rd_kafka_fetch_lz4_free, &allocator};
        LZ4F_dctx *context;
        LZ4F_frameInfo_t info;
        LZ4F_errorCode_t close_result;
        size_t consumed, produced, result;
        size_t input_offset, output_offset;
        size_t capacity;
        char *decoded = NULL;
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR_NO_ERROR;
        *output = NULL;
        context = LZ4F_createDecompressionContext_advanced(memory, LZ4F_VERSION);
        if (!context)
                return RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
        if (!proper_hc && (err = rd_kafka_fetch_lz4_fix_hc(input, input_size)))
                goto done;
        consumed = input_size;
        result = LZ4F_getFrameInfo(context, &info, input, &consumed);
        if (LZ4F_isError(result)) {
                err = allocator.allocation_failed ? RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE
                                                  : RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                goto done;
        }
        if (!rd_kafka_fetch_lz4_estimate(info.contentSize, input_size,
                                         max_message_size, &capacity)) {
                err = RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
                goto done;
        }
        decoded = rd_kafka_fetch_decode_alloc(account, capacity, allocate);
        if (!decoded) {
                err = RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
                goto done;
        }
        input_offset = consumed;
        output_offset = 0;
        while (input_offset < input_size) {
                consumed = input_size - input_offset;
                produced = capacity - output_offset;
                result = LZ4F_decompress(context, decoded + output_offset,
                                         &produced, input + input_offset,
                                         &consumed, NULL);
                if (LZ4F_isError(result)) {
                        err = allocator.allocation_failed ? RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE
                                                          : RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                        goto done;
                }
                rd_kafka_assert(NULL, produced <= capacity - output_offset &&
                                          consumed <= input_size - input_offset);
                output_offset += produced;
                input_offset += consumed;
                if (!result)
                        break;
                if (output_offset == capacity) {
                        char *replacement;
                        size_t next_capacity;
                        rd_atomic64_add(growths, 1);
                        if (!rd_kafka_fetch_lz4_growth(capacity, &next_capacity)) {
                                err = RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
                                goto done;
                        }
                        replacement = rd_kafka_fetch_decode_alloc(account,
                                                                    next_capacity,
                                                                    allocate);
                        if (!replacement) {
                                err = RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
                                goto done;
                        }
                        memcpy(replacement, decoded, output_offset);
                        rd_kafka_fetch_decode_free(decoded);
                        decoded = replacement;
                        capacity = next_capacity;
                }
        }
        if (input_offset < input_size)
                err = RD_KAFKA_RESP_ERR__BAD_MSG;
done:
        close_result = LZ4F_freeDecompressionContext(context);
        if (LZ4F_isError(close_result))
                err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
        if (err) {
                if (decoded)
                        rd_kafka_fetch_decode_free(decoded);
        } else {
                *output = decoded;
                *output_size = output_offset;
        }
        return err;
}
#endif

#endif
