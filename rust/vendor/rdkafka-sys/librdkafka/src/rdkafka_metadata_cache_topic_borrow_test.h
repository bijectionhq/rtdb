#include "rdunittest.h"

int unittest_metadata_cache_topic_borrow(void);

struct ut_cache_topic_borrow_fixture {
        mtx_t lock;
        cnd_t condition;
        rd_bool_t done;
        rd_bool_t actor_seen;
        rd_bool_t topic_verified;
        int lock_observations;
        rd_bool_t client_lock_held;
};

static const char ut_cache_topic_borrow_name[] = "cache-owned";

static const unsigned char ut_cache_topic_borrow_response[] = {
        0, 0, 0, 0, /* Brokers */
        0, 0, 0, 1, /* Topics */
        0, 0,       /* Topic error */
        0, 11, 'c', 'a', 'c', 'h', 'e', '-', 'o', 'w', 'n', 'e', 'd',
        0, 0, 0, 1,                 /* Partitions */
        0, 0,                       /* Partition error */
        0, 0, 0, 0,                 /* Partition ID */
        0xff, 0xff, 0xff, 0xff,     /* Unknown leader */
        0, 0, 0, 0,                 /* Replicas */
        0, 0, 0, 0,                 /* In-sync replicas */
};

static void ut_cache_topic_borrow_log(const rd_kafka_t *rk,
                                      int level,
                                      const char *facility,
                                      const char *message) {
        struct ut_cache_topic_borrow_fixture *fixture;
        rwlock_t *lock;
        rd_bool_t blocked;

        if (level != LOG_DEBUG || strcmp(facility, "TOPIC") ||
            strcmp(message, "New local topic: cache-owned"))
                return;

        fixture = rk->rk_conf.opaque;
        lock    = (rwlock_t *)&rk->rk_lock;
#if defined(_TTHREAD_WIN32_)
        blocked = !TryAcquireSRWLockShared(&lock->lock);
        if (!blocked)
                ReleaseSRWLockShared(&lock->lock);
#else
        int error = pthread_rwlock_tryrdlock(lock);
        blocked   = error == EBUSY || error == EDEADLK;
        if (!error)
                pthread_rwlock_unlock(lock);
#endif
        fixture->lock_observations++;
        fixture->client_lock_held = blocked;
}

static rd_bool_t ut_cache_topic_borrow_seed(rd_kafka_t *rk) {
        rd_kafka_broker_t *broker = rd_kafka_broker_internal(rk);
        rd_kafka_buf_t request    = RD_ZERO_INIT;
        rd_kafka_buf_t *response;
        rd_kafka_metadata_internal_t *parsed = NULL;
        rd_kafka_resp_err_t error;
        rd_list_t topics;

        if (!broker)
                return rd_false;

        rd_list_init(&topics, 1, NULL);
        rd_list_add(&topics, (void *)ut_cache_topic_borrow_name);
        request.rkbuf_u.Metadata.topics = &topics;
        response = rd_kafka_buf_new_shadow(ut_cache_topic_borrow_response,
                                          sizeof(ut_cache_topic_borrow_response),
                                          NULL);
        response->rkbuf_reqhdr.ApiVersion = 0;
        error = rd_kafka_parse_Metadata(broker, &request, response, &parsed);
        if (parsed)
                rd_kafka_metadata_destroy(&parsed->metadata);
        rd_kafka_buf_destroy(response);
        rd_list_destroy(&topics);
        rd_kafka_broker_destroy(broker);
        return error == RD_KAFKA_RESP_ERR_NO_ERROR;
}

static rd_bool_t ut_cache_topic_borrow_probe(rd_kafka_t *rk) {
        const struct rd_kafka_metadata_cache_entry *entry;
        rd_kafka_lwtopic_t *lightweight;
        rd_kafka_topic_t *proper, *existing = NULL;
        rd_ts_t cached_at;
        rd_bool_t verified = rd_false;

        if (!ut_cache_topic_borrow_seed(rk))
                return rd_false;

        rd_kafka_rdlock(rk);
        entry = rd_kafka_metadata_cache_find(rk, ut_cache_topic_borrow_name, 1);
        if (!entry || entry->rkmce_mtopic.err ||
            entry->rkmce_mtopic.partition_cnt != 1 ||
            entry->rkmce_mtopic.partitions[0].id != 0 || rk->rk_topic_cnt != 0) {
                rd_kafka_rdunlock(rk);
                return rd_false;
        }
        cached_at = entry->rkmce_ts_insert;
        rd_kafka_rdunlock(rk);

        lightweight = rd_kafka_lwtopic_new(rk, ut_cache_topic_borrow_name);
        proper      = rd_kafka_topic_proper((rd_kafka_topic_t *)lightweight);
        if (proper) {
                rd_bool_t proper_passthrough = rd_kafka_topic_proper(proper) == proper;
                existing = rd_kafka_topic_proper((rd_kafka_topic_t *)lightweight);

                rd_kafka_rdlock(rk);
                rd_kafka_topic_rdlock(proper);
                verified = existing == proper && proper_passthrough &&
                           rk->rk_topic_cnt == 1 &&
                           TAILQ_FIRST(&rk->rk_topics) == proper &&
                           proper->rkt_state == RD_KAFKA_TOPIC_S_EXISTS &&
                           proper->rkt_partition_cnt == 1 &&
                           proper->rkt_ts_metadata == cached_at;
                rd_kafka_topic_rdunlock(proper);
                rd_kafka_rdunlock(rk);

                if (existing)
                        rd_kafka_topic_destroy0(existing);
                rd_kafka_topic_destroy0(proper);
        }
        rd_kafka_lwtopic_destroy(lightweight);
        return verified;
}

static rd_kafka_op_res_t ut_cache_topic_borrow_job(rd_kafka_t *rk,
                                                   rd_kafka_q_t *queue,
                                                   rd_kafka_op_t *op) {
        struct ut_cache_topic_borrow_fixture *fixture = rk->rk_conf.opaque;
        rd_bool_t actor_seen = op->rko_err == RD_KAFKA_RESP_ERR_NO_ERROR &&
                               thrd_is_current(rk->rk_thread);
        rd_bool_t verified = actor_seen && ut_cache_topic_borrow_probe(rk);

        (void)queue;
        mtx_lock(&fixture->lock);
        fixture->actor_seen     = actor_seen;
        fixture->topic_verified = verified;
        fixture->done           = rd_true;
        cnd_broadcast(&fixture->condition);
        mtx_unlock(&fixture->lock);
        return RD_KAFKA_OP_RES_HANDLED;
}

int unittest_metadata_cache_topic_borrow(void) {
        static const struct {
                const char *name;
                const char *value;
        } settings[] = {
            {"topic.metadata.refresh.interval.ms", "-1"},
            {"metadata.max.age.ms", "600000"},
            {"debug", "topic"},
            {"log.queue", "false"},
            {"log.thread.name", "false"},
        };
        struct ut_cache_topic_borrow_fixture fixture = RD_ZERO_INIT;
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        rd_ts_t deadline;
        rd_bool_t completed, actor_seen, verified, lock_held;
        char error[256];
        size_t i;

        RD_UT_ASSERT(mtx_init(&fixture.lock, mtx_plain) == thrd_success,
                     "initialize lightweight topic fixture lock");
        if (cnd_init(&fixture.condition) != thrd_success) {
                mtx_destroy(&fixture.lock);
                RD_UT_FAIL("initialize lightweight topic fixture condition");
        }
        conf = rd_kafka_conf_new();
        rd_kafka_conf_set_log_cb(conf, ut_cache_topic_borrow_log);
        rd_kafka_conf_set_opaque(conf, &fixture);
        for (i = 0; i < RD_ARRAYSIZE(settings); i++) {
                if (rd_kafka_conf_set(conf, settings[i].name, settings[i].value,
                                      error, sizeof(error)) != RD_KAFKA_CONF_OK) {
                        rd_kafka_conf_destroy(conf);
                        cnd_destroy(&fixture.condition);
                        mtx_destroy(&fixture.lock);
                        RD_UT_FAIL("set %s: %s", settings[i].name, error);
                }
        }
        rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, error, sizeof(error));
        if (!rk) {
                rd_kafka_conf_destroy(conf);
                cnd_destroy(&fixture.condition);
                mtx_destroy(&fixture.lock);
                RD_UT_FAIL("create native client for lightweight topic: %s", error);
        }

        deadline = rd_timeout_init(5000);
        rd_kafka_q_enq(rk->rk_ops,
                      rd_kafka_op_new_cb(rk, RD_KAFKA_OP_WAKEUP,
                                         ut_cache_topic_borrow_job));
        mtx_lock(&fixture.lock);
        while (!fixture.done) {
                if (cnd_timedwait_abs(&fixture.condition, &fixture.lock,
                                      deadline) != thrd_success)
                        break;
        }
        completed  = fixture.done;
        actor_seen = completed && fixture.actor_seen;
        verified   = completed && fixture.topic_verified;
        lock_held  = completed && fixture.lock_observations == 1 &&
                    fixture.client_lock_held;
        mtx_unlock(&fixture.lock);

        rd_kafka_destroy(rk);
        cnd_destroy(&fixture.condition);
        mtx_destroy(&fixture.lock);

        RD_UT_ASSERT(completed && actor_seen,
                     "native main actor completed lightweight topic conversion");
        RD_UT_ASSERT(verified, "local topic inherited independently cached metadata");
        RD_UT_ASSERT(lock_held,
                     "client write lock held at local topic creation diagnostic");
        RD_UT_PASS();
}
