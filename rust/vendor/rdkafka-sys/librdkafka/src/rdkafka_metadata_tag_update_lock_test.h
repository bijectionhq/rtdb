#include "rdkafka_partition.h"
#include "rdunittest.h"

int unittest_metadata_tag_update_lock(void);

struct ut_metadata_tag_update_fixture {
        mtx_t lock;
        cnd_t condition;
        rd_bool_t done;
        rd_bool_t actor_seen;
        rd_bool_t updates_verified;
        int lock_observations;
        rd_bool_t client_lock_held;
};

struct ut_metadata_tag_cache_times {
        rd_ts_t inserted;
        rd_ts_t expires;
};

static char ut_metadata_tag_name[] = "tag-owned";
static const rd_kafka_Uuid_t ut_metadata_tag_id = { .most_significant_bits = 17 };

static void ut_metadata_tag_update_log(const rd_kafka_t *rk,
                                       int level,
                                       const char *facility,
                                       const char *message) {
        static const char partition_prefix[] = "Partition tag-owned(";
        struct ut_metadata_tag_update_fixture *fixture;
        rwlock_t *lock;
        rd_bool_t blocked;

        if (level != LOG_WARNING || strcmp(facility, "METADATAUPDATE") ||
            strncmp(message, partition_prefix, sizeof(partition_prefix) - 1) ||
            !strstr(message, "[0]: new leader7 not found in cache"))
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

static rd_bool_t ut_metadata_tag_update_seed(
    rd_kafka_t *rk, struct ut_metadata_tag_cache_times *times) {
        rd_kafka_metadata_partition_t partition = {.id = 0, .leader = -1};
        rd_kafka_metadata_partition_internal_t internal_partition = {
            .id = 0, .leader_epoch = 3};
        rd_kafka_metadata_topic_t topic = {
            .topic = ut_metadata_tag_name,
            .partition_cnt = 1,
            .partitions = &partition,
        };
        rd_kafka_metadata_topic_internal_t internal_topic = {
            .partitions = &internal_partition,
            .topic_id = ut_metadata_tag_id,
        };
        const struct rd_kafka_metadata_cache_entry *entry;

        rd_kafka_wrlock(rk);
        rd_kafka_metadata_cache_topic_update(rk, &topic, &internal_topic,
                                             rd_false, rd_false, rd_false);
        entry = rd_kafka_metadata_cache_find(rk, ut_metadata_tag_name, 1);
        if (!entry) {
                rd_kafka_wrunlock(rk);
                return rd_false;
        }
        times->inserted = entry->rkmce_ts_insert;
        times->expires  = entry->rkmce_ts_expires;
        rd_kafka_wrunlock(rk);
        return rd_true;
}

static rd_bool_t ut_metadata_tag_update_dispatch(rd_kafka_t *rk,
                                                rd_bool_t by_id,
                                                rd_bool_t announce_broker,
                                                int32_t epoch) {
        rd_kafkap_NodeEndpoint_t endpoint = {
            .NodeId = 7,
            .Host = { .len = 9, .str = "127.0.0.1" },
            .Port = 0,
            .Rack = RD_KAFKAP_STR_INITIALIZER,
        };
        rd_kafkap_NodeEndpoints_t endpoints = {
            .NodeEndpointCnt = announce_broker ? 1 : 0,
            .NodeEndpoints = &endpoint,
        };
        rd_kafkap_CurrentLeader_t leader = {.LeaderId = 7, .LeaderEpoch = epoch};
        char *name = by_id ? NULL : ut_metadata_tag_name;
        rd_kafka_Uuid_t id = by_id ? ut_metadata_tag_id : RD_KAFKA_UUID_ZERO;
        rd_kafka_metadata_internal_t *metadata;
        rd_kafka_op_t *op;
        rd_tmpabuf_t arena;

        rd_tmpabuf_new(&arena, 0, rd_true);
        rd_tmpabuf_add_alloc(&arena, sizeof(*metadata));
        rd_kafkap_leader_discovery_tmpabuf_add_alloc_brokers(&arena, &endpoints);
        rd_kafkap_leader_discovery_tmpabuf_add_alloc_topics(&arena, 1);
        rd_kafkap_leader_discovery_tmpabuf_add_alloc_topic(&arena, name, 1);
        rd_tmpabuf_finalize(&arena);

        metadata = rd_tmpabuf_alloc(&arena, sizeof(*metadata));
        rd_kafkap_leader_discovery_metadata_init(metadata, -1);
        rd_kafkap_leader_discovery_set_brokers(&arena, metadata, &endpoints);
        rd_kafkap_leader_discovery_set_topic_cnt(&arena, metadata, 1);
        rd_kafkap_leader_discovery_set_topic(&arena, metadata, 0, id, name, 1);
        rd_kafkap_leader_discovery_set_CurrentLeader(&arena, metadata, 0, 0, 0,
                                                     &leader);

        op = rd_kafka_op_new(RD_KAFKA_OP_METADATA_UPDATE);
        op->rko_u.metadata.md  = &metadata->metadata;
        op->rko_u.metadata.mdi = metadata;
        return rd_kafka_poll_cb(rk, rk->rk_ops, op, RD_KAFKA_Q_CB_CALLBACK,
                                NULL) == RD_KAFKA_OP_RES_HANDLED;
}

static rd_bool_t ut_metadata_tag_update_matches(
    rd_kafka_t *rk,
    rd_kafka_topic_t *local,
    const struct ut_metadata_tag_cache_times *times,
    int32_t leader,
    int32_t epoch,
    int observers) {
        const struct rd_kafka_metadata_cache_entry *entry;
        rd_bool_t cache_matches, local_matches = rd_false;

        rd_kafka_rdlock(rk);
        entry = rd_kafka_metadata_cache_find(rk, ut_metadata_tag_name, 1);
        cache_matches = entry &&
                        entry == rd_kafka_metadata_cache_find_by_id(
                            rk, ut_metadata_tag_id, 1) &&
                        entry->rkmce_mtopic.partition_cnt == 1 &&
                        entry->rkmce_mtopic.partitions[0].leader == leader &&
                        entry->rkmce_metadata_internal_topic.partitions[0]
                                .leader_epoch == epoch &&
                        entry->rkmce_ts_insert == times->inserted &&
                        entry->rkmce_ts_expires == times->expires &&
                        rd_list_cnt(&rk->rk_metadata_cache.rkmc_observers) ==
                            observers;
        rd_kafka_rdunlock(rk);

        rd_kafka_topic_rdlock(local);
        if (local->rkt_partition_cnt == 1) {
                rd_kafka_toppar_t *partition = local->rkt_p[0];
                rd_kafka_toppar_lock(partition);
                local_matches = partition->rktp_leader_id == leader &&
                                partition->rktp_leader_epoch == epoch;
                rd_kafka_toppar_unlock(partition);
        }
        rd_kafka_topic_rdunlock(local);
        return cache_matches && local_matches;
}

static rd_ts_t ut_metadata_tag_update_expiry_at(rd_kafka_t *rk) {
        rd_ts_t next;
        mtx_lock(&rk->rk_timers.rkts_lock);
        next = rk->rk_metadata_cache.rkmc_expiry_tmr.rtmr_next;
        mtx_unlock(&rk->rk_timers.rkts_lock);
        return next;
}

static rd_bool_t ut_metadata_tag_update_probe(rd_kafka_t *rk) {
        struct ut_metadata_tag_cache_times times;
        rd_kafka_timer_t *expiry = &rk->rk_metadata_cache.rkmc_expiry_tmr;
        rd_kafka_topic_t *local;
        rd_kafka_q_t *notifications;
        rd_kafka_op_t *wakeup, *notice, *pending;
        rd_kafka_enq_once_t *observer;
        rd_ts_t expiry_at;
        rd_bool_t missing, updated, stale, notified;

        if (!ut_metadata_tag_update_seed(rk, &times))
                return rd_false;
        local = rd_kafka_topic_new0(rk, ut_metadata_tag_name, NULL, NULL,
                                    RD_DO_LOCK);
        if (!local)
                return rd_false;

        notifications = rd_kafka_q_new(rk);
        wakeup        = rd_kafka_op_new(RD_KAFKA_OP_WAKEUP);
        observer = rd_kafka_enq_once_new(wakeup, RD_KAFKA_REPLYQ(notifications, 0));
        rd_kafka_wrlock(rk);
        rd_kafka_metadata_cache_wait_state_change_async(rk, observer);
        rd_kafka_wrunlock(rk);

        missing = ut_metadata_tag_update_dispatch(rk, rd_false, rd_false, 4) &&
                  ut_metadata_tag_update_matches(rk, local, &times, -1, 3, 1) &&
                  rd_kafka_q_len(notifications) == 0 &&
                  !rd_kafka_timer_is_started(&rk->rk_timers, expiry);

        updated = ut_metadata_tag_update_dispatch(rk, rd_true, rd_true, 4) &&
                  ut_metadata_tag_update_matches(rk, local, &times, 7, 4, 0) &&
                  rd_kafka_timer_is_started(&rk->rk_timers, expiry);
        expiry_at = ut_metadata_tag_update_expiry_at(rk);
        notice    = rd_kafka_q_pop(notifications, RD_POLL_NOWAIT, 0);
        notified  = notice && notice == wakeup &&
                   notice->rko_err == RD_KAFKA_RESP_ERR_NO_ERROR &&
                   rd_kafka_q_len(notifications) == 0;
        if (notice)
                rd_kafka_op_destroy(notice);

        stale = rd_false;
        if (updated && notified) {
                rd_kafka_enq_once_reenable(observer,
                                            rd_kafka_op_new(RD_KAFKA_OP_WAKEUP),
                                            RD_KAFKA_REPLYQ(notifications, 0));
                rd_kafka_wrlock(rk);
                rd_kafka_metadata_cache_wait_state_change_async(rk, observer);
                rd_kafka_wrunlock(rk);

                stale =
                    ut_metadata_tag_update_dispatch(rk, rd_false, rd_false, 3) &&
                    ut_metadata_tag_update_matches(rk, local, &times, 7, 4, 1) &&
                    ut_metadata_tag_update_expiry_at(rk) == expiry_at &&
                    rd_kafka_q_len(notifications) == 0;
        }

        pending = rd_kafka_enq_once_disable(observer);
        if (pending)
                rd_kafka_op_destroy(pending);
        rd_kafka_q_destroy_owner(notifications);
        rd_kafka_topic_destroy0(local);
        return missing && updated && notified && stale;
}

static rd_kafka_op_res_t ut_metadata_tag_update_job(rd_kafka_t *rk,
                                                   rd_kafka_q_t *queue,
                                                   rd_kafka_op_t *op) {
        struct ut_metadata_tag_update_fixture *fixture = rk->rk_conf.opaque;
        rd_bool_t actor_seen = op->rko_err == RD_KAFKA_RESP_ERR_NO_ERROR &&
                               thrd_is_current(rk->rk_thread);
        rd_bool_t verified = actor_seen && ut_metadata_tag_update_probe(rk);

        (void)queue;
        mtx_lock(&fixture->lock);
        fixture->actor_seen       = actor_seen;
        fixture->updates_verified = verified;
        fixture->done             = rd_true;
        cnd_broadcast(&fixture->condition);
        mtx_unlock(&fixture->lock);
        return RD_KAFKA_OP_RES_HANDLED;
}

int unittest_metadata_tag_update_lock(void) {
        static const struct {
                const char *name;
                const char *value;
        } settings[] = {
            {"topic.metadata.refresh.interval.ms", "-1"},
            {"metadata.max.age.ms", "600000"},
            {"enable.sparse.connections", "true"},
            {"log.queue", "false"},
            {"log.thread.name", "false"},
        };
        struct ut_metadata_tag_update_fixture fixture = RD_ZERO_INIT;
        rd_kafka_conf_t *conf;
        rd_kafka_t *rk;
        rd_ts_t deadline;
        rd_bool_t actor_seen, verified, lock_held;
        char error[256];
        size_t i;

        RD_UT_ASSERT(mtx_init(&fixture.lock, mtx_plain) == thrd_success,
                     "initialize tag update fixture lock");
        if (cnd_init(&fixture.condition) != thrd_success) {
                mtx_destroy(&fixture.lock);
                RD_UT_FAIL("initialize tag update fixture condition");
        }
        conf = rd_kafka_conf_new();
        rd_kafka_conf_set_log_cb(conf, ut_metadata_tag_update_log);
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
                RD_UT_FAIL("create native client for tag update: %s", error);
        }

        deadline = rd_timeout_init(5000);
        rd_kafka_q_enq(rk->rk_ops,
                      rd_kafka_op_new_cb(rk, RD_KAFKA_OP_WAKEUP,
                                         ut_metadata_tag_update_job));
        mtx_lock(&fixture.lock);
        while (!fixture.done) {
                if (cnd_timedwait_abs(&fixture.condition, &fixture.lock,
                                      deadline) != thrd_success)
                        break;
        }
        actor_seen = fixture.done && fixture.actor_seen;
        verified   = fixture.done && fixture.updates_verified;
        lock_held  = fixture.done && fixture.lock_observations == 1 &&
                    fixture.client_lock_held;
        mtx_unlock(&fixture.lock);

        rd_kafka_destroy(rk);
        cnd_destroy(&fixture.condition);
        mtx_destroy(&fixture.lock);

        RD_UT_ASSERT(actor_seen, "native main actor completed tag updates");
        RD_UT_ASSERT(lock_held,
                     "client write lock held at the missing-broker diagnostic");
        RD_UT_ASSERT(verified,
                     "the accepted tag updates the cache, local topic, observer and expiry");
        RD_UT_PASS();
}
