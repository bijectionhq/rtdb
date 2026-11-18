/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * Copyright (c) 2018-2022, Magnus Edenhill
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

#ifndef _RDKAFKA_FETCH_ZSTD_H_
#define _RDKAFKA_FETCH_ZSTD_H_

#if WITH_ZSTD
#ifndef ZSTD_STATIC_LINKING_ONLY
#define ZSTD_STATIC_LINKING_ONLY
#define RD_KAFKA_FETCH_ZSTD_UNDEF_STATIC_LINKING
#endif
#include <zstd.h>
#include <zstd_errors.h>
#ifdef RD_KAFKA_FETCH_ZSTD_UNDEF_STATIC_LINKING
#undef ZSTD_STATIC_LINKING_ONLY
#undef RD_KAFKA_FETCH_ZSTD_UNDEF_STATIC_LINKING
#endif

typedef union rd_kafka_fetch_zstd_owner_u {
        struct {
                rd_kafka_byte_account_t *account;
                size_t charge;
        } allocation;
        long double floating_alignment;
        uint64_t integer_alignment;
        void *pointer_alignment;
} rd_kafka_fetch_zstd_owner_t;

typedef struct rd_kafka_fetch_zstd_allocator_s {
        rd_kafka_byte_account_t *account;
        void *(*allocate)(size_t);
        rd_bool_t allocation_failed;
} rd_kafka_fetch_zstd_allocator_t;

static rd_bool_t rd_kafka_fetch_zstd_size(size_t capacity, size_t *bytes) {
        if (capacity > SIZE_MAX - sizeof(rd_kafka_fetch_zstd_owner_t))
                return rd_false;
        *bytes = sizeof(rd_kafka_fetch_zstd_owner_t) + capacity;
        return rd_true;
}

static void *rd_kafka_fetch_zstd_alloc(void *opaque, size_t capacity) {
        rd_kafka_fetch_zstd_allocator_t *allocator = opaque;
        rd_kafka_fetch_zstd_owner_t *owner;
        size_t bytes;
        if (!rd_kafka_fetch_zstd_size(capacity, &bytes) ||
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

static void rd_kafka_fetch_zstd_free(void *opaque, void *allocation) {
        rd_kafka_fetch_zstd_owner_t *owner;
        rd_kafka_byte_account_t *account;
        size_t charge;
        (void)opaque;
        if (!allocation)
                return;
        owner = (rd_kafka_fetch_zstd_owner_t *)allocation - 1;
        account = owner->allocation.account;
        charge = owner->allocation.charge;
        rd_free(owner);
        rd_kafka_byte_account_release(account, charge);
}

static rd_kafka_resp_err_t rd_kafka_fetch_zstd(
    rd_kafka_byte_account_t *account,
    const char *input,
    size_t input_size,
    unsigned long long max_message_size,
    rd_atomic64_t *growths,
    void **output,
    size_t *output_size,
    void *(*allocate)(size_t)) {
        rd_kafka_fetch_zstd_allocator_t allocator = {account, allocate, rd_false};
        ZSTD_customMem memory = {rd_kafka_fetch_zstd_alloc,
                                 rd_kafka_fetch_zstd_free, &allocator};
        unsigned long long capacity = ZSTD_getFrameContentSize(input, input_size);
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
        ZSTD_DCtx *context;
        *output = NULL;
        if (capacity == ZSTD_CONTENTSIZE_ERROR)
                return RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
        if (capacity == ZSTD_CONTENTSIZE_UNKNOWN) {
                if (input_size > SIZE_MAX / 2)
                        return RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
                capacity = (unsigned long long)input_size * 2;
        }
        context = ZSTD_createDCtx_advanced(memory);
        if (!context)
                return RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
        while (capacity <= max_message_size) {
                char *decoded;
                size_t result;
                decoded = rd_kafka_fetch_decode_alloc(account, (size_t)capacity,
                                                      allocate);
                if (!decoded) {
                        err = RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
                        break;
                }
                result = ZSTD_decompressDCtx(context, decoded, (size_t)capacity,
                                             input, input_size);
                if (!ZSTD_isError(result)) {
                        *output = decoded;
                        *output_size = result;
                        err = RD_KAFKA_RESP_ERR_NO_ERROR;
                        break;
                }
                rd_kafka_fetch_decode_free(decoded);
                if (allocator.allocation_failed) {
                        err = RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
                        break;
                }
                if (ZSTD_getErrorCode(result) != ZSTD_error_dstSize_tooSmall) {
                        err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
                        break;
                }
                capacity += RD_MAX(capacity * 2, 4000);
                rd_atomic64_add(growths, 1);
        }
        ZSTD_freeDCtx(context);
        return err;
}
#endif

#endif
