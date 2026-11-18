int unittest_cgrp_metadata_hint_lock(void);

struct ut_cgrp_metadata_hint_fixture {
        mtx_t lock;
        cnd_t condition;
        rd_bool_t done;
        rd_bool_t actor_seen;
        rd_bool_t hint_verified;
        int hint_logs;
        int blocked_hint_logs;
};

static void ut_cgrp_metadata_hint_log(const rd_kafka_t *rk,
                                      int level,
                                      const char *facility,
                                      const char *message) {
        struct ut_cgrp_metadata_hint_fixture *fixture;
        rwlock_t *lock;
        rd_bool_t blocked;

        if (level != LOG_DEBUG || strcmp(facility, "METADATA") ||
            strcmp(message, "Hinted cache of 1/1 topic(s) being queried"))
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
        fixture->hint_logs++;
        if (blocked)
                fixture->blocked_hint_logs++;
}

static rd_bool_t ut_cgrp_metadata_hint_probe(rd_kafka_t *rk) {
        static const char name[] = "hint-owned";
        rd_kafka_cgrp_t *group    = rk->rk_cgrp;
        rd_kafka_topic_partition_list_t *subscription;
        const struct rd_kafka_metadata_cache_entry *entry;
        int32_t version;
        int result, metadata_age = 0;
        rd_bool_t retry_hint, listed, expiry_armed;
        rd_list_t topics;

        if (!group)
                return rd_false;

        subscription = rd_kafka_topic_partition_list_new(1);
        rd_kafka_topic_partition_list_add(subscription, name,
                                           RD_KAFKA_PARTITION_UA);
        version = rd_kafka_cgrp_subscription_set(group, subscription);
        result = rd_kafka_cgrp_metadata_refresh(group, &metadata_age, version,
                                                "metadata hint lock fixture");

        rd_list_init(&topics, 1, rd_free);
        rd_kafka_rdlock(rk);
        entry = rd_kafka_metadata_cache_find(rk, name, 0);
        retry_hint = entry &&
                     entry->rkmce_mtopic.err == RD_KAFKA_RESP_ERR__NOENT &&
                     entry->rkmce_ts_expires - entry->rkmce_ts_insert ==
                         (rd_ts_t)rk->rk_conf.socket_timeout_ms * 1000 &&
                     rk->rk_metadata_cache.rkmc_cnt == 1 &&
                     !rd_kafka_metadata_cache_find(rk, name, 1);
        listed = rd_kafka_metadata_cache_topics_to_list(rk, &topics, rd_true) == 1;
        rd_kafka_rdunlock(rk);

        listed = listed && !strcmp(rd_list_elem(&topics, 0), name);
        rd_list_destroy(&topics);
        expiry_armed = rd_kafka_timer_is_started(
            &rk->rk_timers, &rk->rk_metadata_cache.rkmc_expiry_tmr);
        rd_kafka_cgrp_subscription_set(group, NULL);
        return result == -1 && metadata_age == -1 && retry_hint && listed &&
               !expiry_armed;
}

static rd_kafka_op_res_t ut_cgrp_metadata_hint_job(rd_kafka_t *rk,
                                                   rd_kafka_q_t *queue,
                                                   rd_kafka_op_t *op) {
        struct ut_cgrp_metadata_hint_fixture *fixture = rk->rk_conf.opaque;
        rd_bool_t actor_seen = op->rko_err == RD_KAFKA_RESP_ERR_NO_ERROR &&
                               thrd_is_current(rk->rk_thread);
        rd_bool_t verified = actor_seen && ut_cgrp_metadata_hint_probe(rk);

        (void)queue;
        mtx_lock(&fixture->lock);
        fixture->actor_seen    = actor_seen;
        fixture->hint_verified = verified;
        fixture->done          = rd_true;
        cnd_broadcast(&fixture->condition);
        mtx_unlock(&fixture->lock);
        return RD_KAFKA_OP_RES_HANDLED;
}

int unittest_cgrp_metadata_hint_lock(void) {
        static const struct {
                const char *name;
                const char *value;
        } settings[] = {
            {"group.id", "metadata-hint-lock-fixture"},
            {"group.protocol", "classic"},
            {"topic.metadata.refresh.interval.ms", "-1"},
            {"enable.metrics.push", "false"},
            {"debug", "metadata"},
            {"log.queue", "false"},
            {"log.thread.name", "false"},
        };
        struct ut_cgrp_metadata_hint_fixture fixture = RD_ZERO_INIT;
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        rd_ts_t deadline;
        rd_bool_t actor_seen, hint_verified, lock_held;
        char error[256];
        size_t i;

        RD_UT_ASSERT(mtx_init(&fixture.lock, mtx_plain) == thrd_success,
                     "initialize group Metadata hint fixture lock");
        if (cnd_init(&fixture.condition) != thrd_success) {
                mtx_destroy(&fixture.lock);
                RD_UT_FAIL("initialize group Metadata hint fixture condition");
        }
        conf = rd_kafka_conf_new();
        rd_kafka_conf_set_log_cb(conf, ut_cgrp_metadata_hint_log);
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
        rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, error, sizeof(error));
        if (!rk) {
                rd_kafka_conf_destroy(conf);
                cnd_destroy(&fixture.condition);
                mtx_destroy(&fixture.lock);
                RD_UT_FAIL("create native classic consumer group: %s", error);
        }

        deadline = rd_timeout_init(5000);
        rd_kafka_q_enq(rk->rk_ops,
                      rd_kafka_op_new_cb(rk, RD_KAFKA_OP_WAKEUP,
                                         ut_cgrp_metadata_hint_job));
        mtx_lock(&fixture.lock);
        while (!fixture.done) {
                if (cnd_timedwait_abs(&fixture.condition, &fixture.lock,
                                      deadline) != thrd_success)
                        break;
        }
        actor_seen = fixture.done && fixture.actor_seen;
        hint_verified = fixture.done && fixture.hint_verified;
        lock_held = fixture.done && fixture.hint_logs == 2 &&
                    fixture.blocked_hint_logs == 2;
        mtx_unlock(&fixture.lock);

        rd_kafka_destroy(rk);
        cnd_destroy(&fixture.condition);
        mtx_destroy(&fixture.lock);

        RD_UT_ASSERT(actor_seen, "native main actor completed group refresh");
        RD_UT_ASSERT(lock_held, "client write lock held at both hint diagnostics");
        RD_UT_ASSERT(hint_verified, "failed refresh retains one retry hint");
        RD_UT_PASS();
}
