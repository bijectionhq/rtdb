/*
 * librdkafka - Apache Kafka C library
 *
 * Copyright (c) 2017-2022, Magnus Edenhill
 *               2023, Confluent Inc.
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

/*
 * C port of the snappy compressor from Google.
 * This is a very fast compressor with comparable compression to lzo.
 * Works best on 64bit little-endian, but should be good on others too.
 * Ported by Andi Kleen.
 * Uptodate with snappy 1.1.0
 */

/*
 * Copyright 2005 Google Inc. All Rights Reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef _RDKAFKA_FETCH_DECODE_H_
#define _RDKAFKA_FETCH_DECODE_H_

#include "rdkafka_buf.h"

typedef struct rd_kafka_fetch_decode_owner_s {
        rd_kafka_byte_account_t *account;
        size_t charge;
} rd_kafka_fetch_decode_owner_t;

static rd_bool_t rd_kafka_fetch_decode_size(size_t capacity, size_t *bytes) {
        if (capacity > SIZE_MAX - sizeof(rd_kafka_fetch_decode_owner_t))
                return rd_false;
        *bytes = sizeof(rd_kafka_fetch_decode_owner_t) + capacity;
        return rd_true;
}

static void *rd_kafka_fetch_decode_alloc(rd_kafka_byte_account_t *account,
                                         size_t capacity,
                                         void *(*allocate)(size_t)) {
        rd_kafka_fetch_decode_owner_t *owner;
        size_t bytes;
        if (!account || !rd_kafka_fetch_decode_size(capacity, &bytes) ||
            !rd_kafka_byte_account_acquire(account, bytes))
                return NULL;
        owner = allocate(bytes);
        if (!owner) {
                rd_kafka_byte_account_release(account, bytes);
                return NULL;
        }
        owner->account = account;
        owner->charge = bytes;
        return owner + 1;
}

static void rd_kafka_fetch_decode_free(void *payload) {
        rd_kafka_fetch_decode_owner_t *owner =
            (rd_kafka_fetch_decode_owner_t *)payload - 1;
        rd_kafka_byte_account_t *account = owner->account;
        size_t charge = owner->charge;
        rd_free(owner);
        rd_kafka_byte_account_release(account, charge);
}

static size_t rd_kafka_fetch_decode_shadow_size(void) {
        return sizeof(rd_kafka_buf_t) + RD_ROUNDUP(sizeof(rd_segment_t), 8);
}

static rd_kafka_buf_t *rd_kafka_fetch_decode_shadow(
    void *payload,
    size_t size,
    void *(*allocate)(size_t)) {
        rd_kafka_fetch_decode_owner_t *owner =
            (rd_kafka_fetch_decode_owner_t *)payload - 1;
        rd_kafka_byte_account_t *account = owner->account;
        rd_kafka_buf_t *shadow;
        char *extra;
        size_t extra_size = RD_ROUNDUP(sizeof(rd_segment_t), 8);
        size_t charge = rd_kafka_fetch_decode_shadow_size();
        if (!rd_kafka_byte_account_acquire(account, charge))
                return NULL;
        shadow = allocate(sizeof(*shadow));
        if (!shadow) {
                rd_kafka_byte_account_release(account, charge);
                return NULL;
        }
        extra = allocate(extra_size);
        if (!extra) {
                rd_free(shadow);
                rd_kafka_byte_account_release(account, charge);
                return NULL;
        }
        memset(shadow, 0, sizeof(*shadow));
        shadow->rkbuf_reqhdr.ApiKey = RD_KAFKAP_None;
        rd_buf_init(&shadow->rkbuf_buf, 0, 0);
        shadow->rkbuf_buf.rbuf_extra = extra;
        shadow->rkbuf_buf.rbuf_extra_size = extra_size;
        rd_buf_push(&shadow->rkbuf_buf, payload, size,
                    rd_kafka_fetch_decode_free);
        shadow->rkbuf_totlen = size;
        rd_slice_init_full(&shadow->rkbuf_reader, &shadow->rkbuf_buf);
        rd_refcnt_init(&shadow->rkbuf_refcnt, 1);
        shadow->rkbuf_byte_account = account;
        shadow->rkbuf_byte_charge = charge;
        return shadow;
}

#if WITH_SNAPPY
static rd_kafka_resp_err_t rd_kafka_fetch_snappy_java(
    rd_kafka_byte_account_t *account,
    const char *input,
    size_t input_size,
    void **output,
    size_t *output_size,
    char *errstr,
    size_t errstr_size,
    void *(*allocate)(size_t)) {
        char *decoded = NULL;
        int pass;
        rd_kafka_resp_err_t err = RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
        for (pass = 1; pass <= 2; pass++) {
                size_t offset = 0;
                size_t decoded_offset = 0;
                while (input_size - offset >= 4) {
                        uint32_t chunk_size;
                        size_t decoded_size;
                        int result;
                        memcpy(&chunk_size, input + offset, 4);
                        chunk_size = be32toh(chunk_size);
                        offset += 4;
                        if (chunk_size > input_size - offset) {
                                rd_snprintf(errstr, errstr_size,
                                            "Invalid snappy-java chunk length "
                                            "%" PRId32 " > %" PRIdsz
                                            " available bytes",
                                            chunk_size,
                                            (ssize_t)(input_size - offset));
                                goto failed;
                        }
                        if (!rd_kafka_snappy_uncompressed_length(
                                input + offset, chunk_size, &decoded_size)) {
                                rd_snprintf(errstr, errstr_size,
                                            "Failed to get length of "
                                            "(snappy-java framed) Snappy "
                                            "compressed payload "
                                            "(clen %" PRId32 ")",
                                            chunk_size);
                                goto failed;
                        }
                        if (decoded_size > SIZE_MAX - decoded_offset) {
                                err = RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
                                rd_snprintf(errstr, errstr_size,
                                            "Snappy decoded size overflow");
                                goto failed;
                        }
                        if (pass == 2 &&
                            (result = rd_kafka_snappy_uncompress(
                                 input + offset, chunk_size,
                                 decoded + decoded_offset))) {
                                rd_snprintf(errstr, errstr_size,
                                            "Failed to decompress Snappy-java "
                                            "framed payload of size %" PRId32
                                            ": %s",
                                            chunk_size, rd_strerror(-result));
                                goto failed;
                        }
                        offset += chunk_size;
                        decoded_offset += decoded_size;
                }
                if (offset != input_size) {
                        rd_snprintf(errstr, errstr_size,
                                    "%" PRIusz " trailing bytes in Snappy-java "
                                    "framed compressed data",
                                    input_size - offset);
                        goto failed;
                }
                if (pass == 1) {
                        if (!decoded_offset) {
                                rd_snprintf(errstr, errstr_size,
                                            "Empty Snappy-java framed data");
                                goto failed;
                        }
                        decoded = rd_kafka_fetch_decode_alloc(
                            account, decoded_offset, allocate);
                        if (!decoded) {
                                err = RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
                                rd_snprintf(errstr, errstr_size,
                                            "Failed to admit Snappy output "
                                            "of size %" PRIusz,
                                            decoded_offset);
                                goto failed;
                        }
                } else {
                        *output = decoded;
                        *output_size = decoded_offset;
                }
        }
        return RD_KAFKA_RESP_ERR_NO_ERROR;
failed:
        if (decoded)
                rd_kafka_fetch_decode_free(decoded);
        return err;
}

static rd_kafka_resp_err_t rd_kafka_fetch_snappy(
    rd_kafka_byte_account_t *account,
    const void *input,
    size_t input_size,
    void **output,
    size_t *output_size,
    char *errstr,
    size_t errstr_size,
    void *(*allocate)(size_t)) {
        static const unsigned char java_magic[] = {
            0x82, 'S', 'N', 'A', 'P', 'P', 'Y', 0};
        size_t decoded_size;
        void *decoded;
        int result;
        if (!input) {
                rd_snprintf(errstr, errstr_size, "Missing Snappy input");
                return RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
        }
        if (input_size > 20 && !memcmp(input, java_magic, sizeof(java_magic)))
                return rd_kafka_fetch_snappy_java(
                    account, (const char *)input + 16, input_size - 16,
                    output, output_size, errstr, errstr_size, allocate);
        if (!rd_kafka_snappy_uncompressed_length(input, input_size,
                                                 &decoded_size)) {
                rd_snprintf(errstr, errstr_size,
                            "Failed to get length of Snappy compressed payload");
                return RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
        }
        decoded = rd_kafka_fetch_decode_alloc(account, decoded_size, allocate);
        if (!decoded) {
                rd_snprintf(errstr, errstr_size,
                            "Failed to admit Snappy output of size %" PRIusz,
                            decoded_size);
                return RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE;
        }
        result = rd_kafka_snappy_uncompress(input, input_size, decoded);
        if (result) {
                rd_kafka_fetch_decode_free(decoded);
                rd_snprintf(errstr, errstr_size,
                            "Failed to decompress Snappy payload: %s",
                            rd_strerror(-result));
                return RD_KAFKA_RESP_ERR__BAD_COMPRESSION;
        }
        *output = decoded;
        *output_size = decoded_size;
        return RD_KAFKA_RESP_ERR_NO_ERROR;
}
#endif

#endif
