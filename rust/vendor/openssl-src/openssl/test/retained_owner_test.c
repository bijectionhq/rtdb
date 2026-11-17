/*
 * Licensed under the Apache License 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You can obtain a copy in the file LICENSE in the source distribution.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <openssl/asn1t.h>
#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/x509.h>
#include "crypto/x509.h"
#include "../crypto/x509/x509_local.h"

typedef struct {
    uintptr_t address;
    int freed;
} WATCH;

typedef struct {
    int first;
    int last;
    int calls;
} OWNER;

typedef struct {
    EVP_PKEY *key;
    X509 *cert;
    X509_CRL *crl;
} FIXTURE;

static WATCH watched[64];
static int watched_count;
static int failures;

#define CHECK(condition) do { \
    if (!(condition)) { \
        fprintf(stderr, "%s:%d: %s\n", __FILE__, __LINE__, #condition); \
        failures++; \
    } \
} while (0)

static void *tracked_malloc(size_t size, const char *file, int line)
{
    (void)file;
    (void)line;
    return malloc(size);
}

static void *tracked_realloc(void *ptr, size_t size, const char *file, int line)
{
    (void)file;
    (void)line;
    return realloc(ptr, size);
}

static void tracked_free(void *ptr, const char *file, int line)
{
    uintptr_t address = (uintptr_t)ptr;
    int i;

    (void)file;
    (void)line;
    free(ptr);
    for (i = 0; i < watched_count; i++) {
        if (watched[i].address == address)
            watched[i].freed = 1;
    }
}

static void watch(void *ptr)
{
    CHECK(ptr != NULL);
    CHECK(watched_count < (int)(sizeof(watched) / sizeof(watched[0])));
    if (ptr != NULL && watched_count < (int)(sizeof(watched) / sizeof(watched[0]))) {
        watched[watched_count].address = (uintptr_t)ptr;
        watched[watched_count++].freed = 0;
    }
}

static void release_owner(void *opaque)
{
    OWNER *owner = opaque;
    int i;

    owner->calls++;
    for (i = owner->first; i < owner->last; i++)
        CHECK(watched[i].freed);
}

static void free_fixture(FIXTURE *fixture)
{
    X509_free(fixture->cert);
    X509_CRL_free(fixture->crl);
    EVP_PKEY_free(fixture->key);
    fixture->cert = NULL;
    fixture->crl = NULL;
    fixture->key = NULL;
}

static int make_fixture(FIXTURE *fixture)
{
    static const unsigned char private_key[32] = { 1 };
    X509_NAME *name;

    fixture->key = EVP_PKEY_new_raw_private_key_ex(NULL, "ED25519", NULL,
        private_key, sizeof(private_key));
    fixture->cert = X509_new();
    fixture->crl = X509_CRL_new();
    if (fixture->key == NULL || fixture->cert == NULL || fixture->crl == NULL)
        goto err;
    name = X509_get_subject_name(fixture->cert);
    if (!X509_set_version(fixture->cert, 2)
        || !ASN1_INTEGER_set(X509_get_serialNumber(fixture->cert), 1)
        || !X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
            (const unsigned char *)"retained-owner", -1, -1, 0)
        || !X509_set_issuer_name(fixture->cert, name)
        || !X509_set_pubkey(fixture->cert, fixture->key)
        || !ASN1_TIME_set_string_X509(X509_getm_notBefore(fixture->cert), "20200101000000Z")
        || !ASN1_TIME_set_string_X509(X509_getm_notAfter(fixture->cert), "20400101000000Z")
        || X509_sign(fixture->cert, fixture->key, NULL) <= 0
        || !X509_CRL_set_version(fixture->crl, 1)
        || !X509_CRL_set_issuer_name(fixture->crl, name)
        || !X509_CRL_set1_lastUpdate(fixture->crl, X509_get0_notBefore(fixture->cert))
        || !X509_CRL_set1_nextUpdate(fixture->crl, X509_get0_notAfter(fixture->cert))
        || X509_CRL_sign(fixture->crl, fixture->key, NULL) <= 0)
        goto err;
    return 1;
err:
    ERR_print_errors_fp(stderr);
    free_fixture(fixture);
    return 0;
}

static void duplication_does_not_inherit_retained_owners(void)
{
    FIXTURE fixture = { NULL, NULL, NULL };
    X509 *cert;
    X509_CRL *crl;
    OWNER original_cert = { 0, 0, 0 }, original_crl = { 0, 0, 0 };
    OWNER copied_cert = { 0, 0, 0 }, copied_crl = { 0, 0, 0 };

    CHECK(make_fixture(&fixture));
    if (fixture.cert == NULL)
        return;
    CHECK(X509_set0_retained_owner(fixture.cert, &original_cert, release_owner) == 1);
    CHECK(X509_CRL_set0_retained_owner(fixture.crl, &original_crl, release_owner) == 1);
    cert = X509_dup(fixture.cert);
    crl = X509_CRL_dup(fixture.crl);
    CHECK(cert != NULL && crl != NULL);
    CHECK(original_cert.calls == 0 && original_crl.calls == 0);
    CHECK(X509_set0_retained_owner(cert, &copied_cert, release_owner) == 1);
    CHECK(X509_CRL_set0_retained_owner(crl, &copied_crl, release_owner) == 1);
    X509_free(cert);
    X509_CRL_free(crl);
    CHECK(copied_cert.calls == 1 && copied_crl.calls == 1);
    CHECK(original_cert.calls == 0 && original_crl.calls == 0);
    free_fixture(&fixture);
    CHECK(original_cert.calls == 1 && original_crl.calls == 1);
}

static void decode_into_existing_preserves_owners(void)
{
    FIXTURE fixture = { NULL, NULL, NULL };
    X509 *cert = NULL, *original_cert;
    X509_CRL *crl = NULL, *original_crl;
    unsigned char *cert_der = NULL, *crl_der = NULL;
    const unsigned char *input;
    int cert_len, crl_len;
    OWNER cert_owner = { 0, 0, 0 }, crl_owner = { 0, 0, 0 };

    CHECK(make_fixture(&fixture));
    if (fixture.cert == NULL)
        return;
    cert_len = i2d_X509(fixture.cert, &cert_der);
    crl_len = i2d_X509_CRL(fixture.crl, &crl_der);
    cert = X509_new_ex(NULL, "provider=default");
    crl = X509_CRL_new_ex(NULL, "provider=default");
    CHECK(cert_len > 0 && crl_len > 0 && cert != NULL && crl != NULL);
    if (cert_len <= 0 || crl_len <= 0 || cert == NULL || crl == NULL)
        goto end;
    original_cert = cert;
    original_crl = crl;
    watched_count = 0;
    watch(cert);
    watch(cert->propq);
    cert_owner.last = watched_count;
    crl_owner.first = watched_count;
    watch(crl);
    watch(crl->propq);
    crl_owner.last = watched_count;
    CHECK(X509_set0_retained_owner(cert, &cert_owner, release_owner) == 1);
    CHECK(X509_CRL_set0_retained_owner(crl, &crl_owner, release_owner) == 1);
    input = cert_der;
    CHECK(d2i_X509(&cert, &input, cert_len) == original_cert);
    CHECK(cert == original_cert && input == cert_der + cert_len);
    input = crl_der;
    CHECK(d2i_X509_CRL(&crl, &input, crl_len) == original_crl);
    CHECK(crl == original_crl && input == crl_der + crl_len);
    CHECK(cert_owner.calls == 0 && crl_owner.calls == 0);
    X509_free(cert);
    cert = NULL;
    X509_CRL_free(crl);
    crl = NULL;
    CHECK(cert_owner.calls == 1 && crl_owner.calls == 1);
    watched_count = 0;
end:
    X509_free(cert);
    X509_CRL_free(crl);
    OPENSSL_free(cert_der);
    OPENSSL_free(crl_der);
    free_fixture(&fixture);
}

static void failed_decode_releases_owner_at_actual_last_reference(void)
{
    static const unsigned char malformed[2][2] = { { 0x05, 0 }, { 0x30, 0 } };
    int index, keep_alias;

    for (index = 0; index < 2; index++) {
        for (keep_alias = 0; keep_alias < 2; keep_alias++) {
            X509 *cert = X509_new(), *cert_alias = cert;
            X509_CRL *crl = X509_CRL_new(), *crl_alias = crl;
            OWNER cert_owner = { 0, 0, 0 }, crl_owner = { 1, 0, 0 };
            const unsigned char *input;

            CHECK(cert != NULL && crl != NULL);
            if (cert == NULL || crl == NULL) {
                X509_free(cert);
                X509_CRL_free(crl);
                return;
            }
            watched_count = 0;
            watch(cert);
            cert_owner.last = watched_count;
            watch(crl);
            crl_owner.last = watched_count;
            CHECK(X509_set0_retained_owner(cert, &cert_owner, release_owner) == 1);
            CHECK(X509_CRL_set0_retained_owner(crl, &crl_owner, release_owner) == 1);
            if (keep_alias) {
                CHECK(X509_up_ref(cert) == 1);
                CHECK(X509_CRL_up_ref(crl) == 1);
            }
            input = malformed[index];
            CHECK(d2i_X509(&cert, &input, sizeof(malformed[index])) == NULL);
            CHECK(cert == NULL);
            input = malformed[index];
            CHECK(d2i_X509_CRL(&crl, &input, sizeof(malformed[index])) == NULL);
            CHECK(crl == NULL);
            CHECK(cert_owner.calls == !keep_alias && crl_owner.calls == !keep_alias);
            if (keep_alias) {
                X509_free(cert_alias);
                X509_CRL_free(crl_alias);
            }
            CHECK(cert_owner.calls == 1 && crl_owner.calls == 1);
            watched_count = 0;
            X509_free(cert);
            X509_CRL_free(crl);
            ERR_clear_error();
        }
    }
}

static void failed_aux_decode_keeps_existing_certificate_owner(void)
{
    FIXTURE fixture = { NULL, NULL, NULL };
    X509 *cert = NULL, *original;
    unsigned char *der = NULL, *output;
    const unsigned char *input;
    int length;
    OWNER owner = { 0, 0, 0 };

    CHECK(make_fixture(&fixture));
    if (fixture.cert == NULL)
        return;
    length = i2d_X509(fixture.cert, NULL);
    CHECK(length > 0);
    if (length <= 0)
        goto end;
    der = OPENSSL_malloc((size_t)length + 2);
    cert = X509_new();
    CHECK(der != NULL && cert != NULL);
    if (der == NULL || cert == NULL)
        goto end;
    output = der;
    CHECK(i2d_X509(fixture.cert, &output) == length);
    output[0] = 0x05;
    output[1] = 0;
    original = cert;
    watched_count = 0;
    watch(cert);
    owner.last = watched_count;
    CHECK(X509_set0_retained_owner(cert, &owner, release_owner) == 1);
    input = der;
    CHECK(d2i_X509_AUX(&cert, &input, (long)length + 2) == NULL);
    CHECK(cert == original && input == der && owner.calls == 0);
    X509_free(cert);
    cert = NULL;
    CHECK(owner.calls == 1);
    watched_count = 0;
end:
    X509_free(cert);
    OPENSSL_free(der);
    free_fixture(&fixture);
    ERR_clear_error();
}

static void certificate_owner_follows_generic_asn1_and_last_reference(void)
{
    X509 *cert = X509_new_ex(NULL, "retained-owner=yes");
    OWNER owner = { 0, 0, 0 }, rejected = { 0, 0, 0 };

    CHECK(cert != NULL);
    if (cert == NULL)
        return;
    cert->skid = ASN1_OCTET_STRING_new();
    CHECK(cert->skid != NULL);
    watched_count = 0;
    watch(cert);
    watch(cert->propq);
    watch(cert->skid);
    owner.last = watched_count;
    CHECK(X509_set0_retained_owner(NULL, &rejected, release_owner) == 0);
    CHECK(X509_set0_retained_owner(cert, &rejected, NULL) == 0);
    CHECK(X509_set0_retained_owner(cert, &owner, release_owner) == 1);
    CHECK(X509_set0_retained_owner(cert, &rejected, release_owner) == 0);
    CHECK(X509_up_ref(cert) == 1);
    X509_free(cert);
    CHECK(owner.calls == 0);
    ASN1_item_free((ASN1_VALUE *)cert, ASN1_ITEM_rptr(X509));
    CHECK(owner.calls == 1);
    CHECK(rejected.calls == 0);
    watched_count = 0;
}

static void crl_owner_follows_generic_asn1_and_last_reference(void)
{
    X509_CRL *crl = X509_CRL_new_ex(NULL, "retained-owner=yes");
    OWNER owner = { 0, 0, 0 }, rejected = { 0, 0, 0 };

    CHECK(crl != NULL);
    if (crl == NULL)
        return;
    crl->crl_number = ASN1_INTEGER_new();
    CHECK(crl->crl_number != NULL);
    watched_count = 0;
    watch(crl);
    watch(crl->propq);
    watch(crl->crl_number);
    owner.last = watched_count;
    CHECK(X509_CRL_set0_retained_owner(NULL, &rejected, release_owner) == 0);
    CHECK(X509_CRL_set0_retained_owner(crl, &rejected, NULL) == 0);
    CHECK(X509_CRL_set0_retained_owner(crl, &owner, release_owner) == 1);
    CHECK(X509_CRL_set0_retained_owner(crl, &rejected, release_owner) == 0);
    CHECK(X509_CRL_up_ref(crl) == 1);
    X509_CRL_free(crl);
    CHECK(owner.calls == 0);
    ASN1_item_free((ASN1_VALUE *)crl, ASN1_ITEM_rptr(X509_CRL));
    CHECK(owner.calls == 1);
    CHECK(rejected.calls == 0);
    watched_count = 0;
}

static void store_owner_follows_lookup_cached_objects_and_storage(void)
{
    X509_STORE *store = X509_STORE_new();
    X509 *cert = X509_new();
    X509_CRL *crl = X509_CRL_new();
    X509_LOOKUP *lookup;
    OWNER owner = { 0, 0, 0 }, rejected = { 0, 0, 0 };

    CHECK(store != NULL && cert != NULL && crl != NULL);
    if (store == NULL || cert == NULL || crl == NULL)
        goto end;
    lookup = X509_STORE_add_lookup(store, X509_LOOKUP_hash_dir());
    CHECK(lookup != NULL);
    if (lookup == NULL)
        goto end;
    CHECK(X509_LOOKUP_add_dir(lookup, ".", X509_FILETYPE_PEM) == 1);
    CHECK(X509_STORE_add_cert(store, cert) == 1);
    CHECK(X509_STORE_add_crl(store, crl) == 1);
    watched_count = 0;
    watch(store);
    watch(store->objs);
    watch(store->get_cert_methods);
    watch(lookup);
    watch(lookup->method_data);
    watch(cert);
    watch(crl);
    owner.last = watched_count;
    CHECK(X509_STORE_set0_retained_owner(NULL, &rejected, release_owner) == 0);
    CHECK(X509_STORE_set0_retained_owner(store, &rejected, NULL) == 0);
    CHECK(X509_STORE_set0_retained_owner(store, &owner, release_owner) == 1);
    CHECK(X509_STORE_set0_retained_owner(store, &rejected, release_owner) == 0);
    CHECK(X509_STORE_up_ref(store) == 1);
    X509_STORE_free(store);
    CHECK(owner.calls == 0);
    X509_free(cert);
    cert = NULL;
    X509_CRL_free(crl);
    crl = NULL;
    X509_STORE_free(store);
    store = NULL;
    CHECK(owner.calls == 1);
    CHECK(rejected.calls == 0);
    watched_count = 0;
end:
    X509_free(cert);
    X509_CRL_free(crl);
    X509_STORE_free(store);
}

static void cached_object_owners_outlive_store_when_referenced_elsewhere(void)
{
    X509_STORE *store = X509_STORE_new();
    X509 *cert = X509_new();
    X509_CRL *crl = X509_CRL_new();
    OWNER cert_owner = { 0, 0, 0 }, crl_owner = { 1, 0, 0 };
    OWNER store_owner = { 2, 0, 0 };

    CHECK(store != NULL && cert != NULL && crl != NULL);
    if (store == NULL || cert == NULL || crl == NULL)
        goto end;
    CHECK(X509_STORE_add_cert(store, cert) == 1);
    CHECK(X509_STORE_add_crl(store, crl) == 1);
    watched_count = 0;
    watch(cert);
    cert_owner.last = watched_count;
    watch(crl);
    crl_owner.last = watched_count;
    watch(store);
    store_owner.last = watched_count;
    CHECK(X509_set0_retained_owner(cert, &cert_owner, release_owner) == 1);
    CHECK(X509_CRL_set0_retained_owner(crl, &crl_owner, release_owner) == 1);
    CHECK(X509_STORE_set0_retained_owner(store, &store_owner, release_owner) == 1);
    X509_STORE_free(store);
    store = NULL;
    CHECK(store_owner.calls == 1);
    CHECK(cert_owner.calls == 0 && crl_owner.calls == 0);
    X509_free(cert);
    cert = NULL;
    X509_CRL_free(crl);
    crl = NULL;
    CHECK(cert_owner.calls == 1 && crl_owner.calls == 1);
    watched_count = 0;
end:
    X509_free(cert);
    X509_CRL_free(crl);
    X509_STORE_free(store);
}

static int abort_crl_payload_cleanup(X509_CRL *crl)
{
    (void)crl;
    return 0;
}

static void aborted_crl_payload_cleanup_retains_owner(void)
{
    X509_CRL_METHOD method = { 0, NULL, abort_crl_payload_cleanup, NULL, NULL };
    X509_CRL *crl = X509_CRL_new_ex(NULL, "retained-owner=yes");
    OWNER owner = { 0, 0, 0 };
    char *propq;

    CHECK(crl != NULL);
    if (crl == NULL)
        return;
    crl->meth = &method;
    propq = crl->propq;
    watched_count = 0;
    watch(crl);
    watch(propq);
    owner.last = watched_count;
    CHECK(X509_CRL_set0_retained_owner(crl, &owner, release_owner) == 1);
    X509_CRL_free(crl);
    CHECK(watched[0].freed && !watched[1].freed);
    CHECK(owner.calls == 0);
    OPENSSL_free(propq);
    release_owner(&owner);
    CHECK(owner.calls == 1);
    watched_count = 0;
}

int main(void)
{
    if (!CRYPTO_set_mem_functions(tracked_malloc, tracked_realloc, tracked_free))
        return EXIT_FAILURE;
    certificate_owner_follows_generic_asn1_and_last_reference();
    crl_owner_follows_generic_asn1_and_last_reference();
    store_owner_follows_lookup_cached_objects_and_storage();
    cached_object_owners_outlive_store_when_referenced_elsewhere();
    aborted_crl_payload_cleanup_retains_owner();
    duplication_does_not_inherit_retained_owners();
    decode_into_existing_preserves_owners();
    failed_decode_releases_owner_at_actual_last_reference();
    failed_aux_decode_keeps_existing_certificate_owner();
    OPENSSL_cleanup();
    return failures == 0 ? EXIT_SUCCESS : EXIT_FAILURE;
}
