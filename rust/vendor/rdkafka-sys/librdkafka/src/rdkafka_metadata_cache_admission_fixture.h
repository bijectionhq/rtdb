#ifndef _RDKAFKA_METADATA_CACHE_ADMISSION_FIXTURE_H_
#define _RDKAFKA_METADATA_CACHE_ADMISSION_FIXTURE_H_

#include "rdunittest.h"

struct ut_cache_admission_client {
        rd_kafka_t rk;
        rd_kafka_broker_t broker;
};

static RD_UNUSED void ut_cache_admission_client_init(
    struct ut_cache_admission_client *fixture,
    rd_kafka_byte_account_t *account) {
        rd_kafka_t *rk = &fixture->rk;
        rd_kafka_broker_t *broker = &fixture->broker;
        memset(fixture, 0, sizeof(*fixture));
        rwlock_init(&rk->rk_lock);
        TAILQ_INIT(&rk->rk_brokers);
        TAILQ_INIT(&rk->rk_topics);
        rk->rk_type = RD_KAFKA_PRODUCER;
        rk->rk_byte_account = account;
        rk->rk_conf.metadata_max_age_ms = 60000;
        rk->rk_conf.socket_timeout_ms = 60000;
        rk->rk_ts_metadata = 123;
        rk->rk_ts_full_metadata = 456;
#if defined(_TTHREAD_WIN32_)
        {
                int duplicated = DuplicateHandle(
                    GetCurrentProcess(), thrd_current(), GetCurrentProcess(),
                    &rk->rk_thread, 0, FALSE, DUPLICATE_SAME_ACCESS);
                rd_assert(duplicated);
                (void)duplicated;
        }
#else
        rk->rk_thread = thrd_current();
#endif
        rd_kafka_timers_init(&rk->rk_timers, rk, NULL);
        rd_kafka_metadata_cache_init(rk);
        mtx_init(&broker->rkb_lock, mtx_plain);
        mtx_init(&broker->rkb_logname_lock, mtx_plain);
        broker->rkb_logname = (char *)"cache-admission-fixture";
        broker->rkb_rk = rk;
        broker->rkb_features = RD_KAFKA_FEATURE_UNITTEST;
        rd_strlcpy(broker->rkb_name, "b", sizeof(broker->rkb_name));
}

static RD_UNUSED void ut_cache_admission_client_destroy(
    struct ut_cache_admission_client *fixture) {
        rd_kafka_t *rk = &fixture->rk;
        rd_kafka_wrlock(rk);
        rd_kafka_metadata_cache_destroy(rk);
        rd_kafka_wrunlock(rk);
        rd_kafka_timers_destroy(&rk->rk_timers);
        rwlock_destroy(&rk->rk_lock);
        mtx_destroy(&fixture->broker.rkb_logname_lock);
        mtx_destroy(&fixture->broker.rkb_lock);
#if defined(_TTHREAD_WIN32_)
        {
                int closed = CloseHandle(rk->rk_thread);
                rd_assert(closed);
                (void)closed;
        }
#endif
}

static RD_UNUSED size_t ut_cache_admission_empty_size(const char *name) {
        return RD_ROUNDUP(sizeof(struct rd_kafka_metadata_cache_entry), 8) +
               RD_ROUNDUP(strlen(name) + 1, 8);
}

static const unsigned char ut_cache_admission_response[] = {
        0, 0, 0, 0, 0, 0, 0, 2,
        0, 0, 0, 5, 'f', 'i', 'r', 's', 't', 0, 0, 0, 0,
        0, 0, 0, 6, 's', 'e', 'c', 'o', 'n', 'd', 0, 0, 0, 0
};

static RD_UNUSED size_t ut_cache_admission_parser_size(void) {
        return RD_ROUNDUP(sizeof(rd_kafka_metadata_internal_t), 8) +
               RD_ROUNDUP(2, 8) +
               RD_ROUNDUP(4 * sizeof(ut_cache_admission_response), 8);
}

static RD_UNUSED rd_bool_t ut_cache_admission_after_parse(
    struct ut_cache_admission_client *fixture, rd_bool_t late) {
        rd_kafka_t *rk = &fixture->rk;
        rd_bool_t matches;
        rd_kafka_rdlock(rk);
        matches = rk->rk_metadata_cache.rkmc_cnt == (late ? 1 : 0) &&
            !!rd_kafka_metadata_cache_find(rk, "first", 1) == !!late &&
            !rd_kafka_metadata_cache_find(rk, "second", 0) &&
            rk->rk_ts_metadata == 123 && rk->rk_ts_full_metadata == 456 &&
            rd_kafka_byte_account_used(rk->rk_byte_account) ==
                (late ? ut_cache_admission_empty_size("first") : 0) &&
            !!rd_kafka_timer_is_started(&rk->rk_timers,
                &rk->rk_metadata_cache.rkmc_expiry_tmr) == !!late;
        rd_kafka_rdunlock(rk);
        return matches;
}

#endif
