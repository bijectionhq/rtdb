#include "rdunittest.h"

int unittest_metadata_cache_parsers(void);

struct ut_cache_parser_fixture {
        mtx_t lock;
        cnd_t condition;
        rd_kafka_t *rk;
        int admin, done, actor_seen;
        rd_kafka_resp_err_t error;
        rd_kafka_metadata_internal_t *parsed;
};

static const unsigned char ut_cache_rack_response[] = {
        0,0,0,1,
        0,0,0,7, 0,9,'1','2','7','.','0','.','0','.','1', 0,0,0,0, 0,4,'r','a','c','k',
        0,0,0,7, 0,0,0,1,
        0,0, 0,11,'c','a','c','h','e','-','o','w','n','e','d', 0, 0,0,0,2,
        0,0, 0,0,0,0, 0,0,0,7, 0,0,0,1, 0,0,0,7, 0,0,0,1, 0,0,0,7,
        0,0, 0,0,0,1, 0,0,0,7, 0,0,0,1, 0,0,0,8, 0,0,0,1, 0,0,0,8
};

static rd_kafka_op_res_t ut_cache_parser_job(rd_kafka_t *rk, rd_kafka_q_t *queue,
                                             rd_kafka_op_t *op) {
        struct ut_cache_parser_fixture *f = rk->rk_conf.opaque;
        (void)queue;
        if (op->rko_err == RD_KAFKA_RESP_ERR__DESTROY) {
                f->error = RD_KAFKA_RESP_ERR__DESTROY;
        } else {
                rd_kafka_buf_t request = RD_ZERO_INIT;
                rd_list_t topics;
                rd_kafka_broker_t *broker = rd_kafka_broker_internal(rk);
                rd_kafka_buf_t *response = rd_kafka_buf_new_shadow(
                    ut_cache_rack_response, sizeof(ut_cache_rack_response), NULL);
                f->actor_seen = thrd_is_current(rk->rk_thread);
                rd_list_init(&topics, 1, NULL);
                rd_list_add(&topics, "cache-owned");
                response->rkbuf_reqhdr.ApiVersion = 1;
                request.rkbuf_u.Metadata.topics = &topics;
                request.rkbuf_u.Metadata.force_racks = rd_true;
                if (!broker)
                        f->error = RD_KAFKA_RESP_ERR__TRANSPORT;
                else if (f->admin)
                        f->error = rd_kafka_parse_Metadata_admin(broker, response, &topics, &f->parsed);
                else
                        f->error = rd_kafka_parse_Metadata(broker, &request, response, &f->parsed);
                rd_kafka_buf_destroy(response);
                rd_list_destroy(&topics);
                if (broker)
                        rd_kafka_broker_destroy(broker);
        }
        mtx_lock(&f->lock);
        f->done = 1;
        cnd_broadcast(&f->condition);
        mtx_unlock(&f->lock);
        return RD_KAFKA_OP_RES_HANDLED;
}

static int ut_cache_parser_init(struct ut_cache_parser_fixture *f, int admin) {
        rd_kafka_conf_t *conf;
        char error[256];
        memset(f, 0, sizeof(*f));
        f->admin = admin;
        if (mtx_init(&f->lock, mtx_plain) != thrd_success)
                return 1;
        if (cnd_init(&f->condition) != thrd_success) {
                mtx_destroy(&f->lock);
                return 1;
        }
        conf = rd_kafka_conf_new();
        rd_kafka_conf_set_opaque(conf, f);
        rd_kafka_conf_set_log_cb(conf, NULL);
        if (admin && rd_kafka_conf_set(conf, "client.rack", "client", error, sizeof(error)) != RD_KAFKA_CONF_OK) {
                rd_kafka_conf_destroy(conf);
                cnd_destroy(&f->condition);
                mtx_destroy(&f->lock);
                return 1;
        }
        f->rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, error, sizeof(error));
        if (!f->rk) {
                rd_kafka_conf_destroy(conf);
                cnd_destroy(&f->condition);
                mtx_destroy(&f->lock);
                return 1;
        }
        return 0;
}

static void ut_cache_parser_finish(struct ut_cache_parser_fixture *f) {
        rd_kafka_destroy(f->rk);
        if (f->parsed)
                rd_kafka_metadata_destroy(&f->parsed->metadata);
        cnd_destroy(&f->condition);
        mtx_destroy(&f->lock);
}

static int ut_cache_parser_owners(int admin) {
        struct ut_cache_parser_fixture f;
        struct rd_kafka_metadata_cache_entry *entry;
        rd_kafka_metadata_partition_internal_t *parts;
        rd_ts_t deadline;
        int completed, independent = 0, retained = 0;
        RD_UT_ASSERT(!ut_cache_parser_init(&f, admin), "create actual parser main actor");
        rd_kafka_q_enq(f.rk->rk_ops, rd_kafka_op_new_cb(f.rk, RD_KAFKA_OP_WAKEUP, ut_cache_parser_job));
        deadline = rd_clock() + 5000000;
        mtx_lock(&f.lock);
        while (!f.done && rd_clock() < deadline)
                cnd_timedwait_ms(&f.condition, &f.lock, 100);
        completed = f.done;
        mtx_unlock(&f.lock);
        if (completed && !f.error && f.parsed &&
            f.parsed->metadata.topic_cnt == 1 &&
            f.parsed->metadata.topics[0].partition_cnt == 2) {
                rd_kafka_rdlock(f.rk);
                entry = rd_kafka_metadata_cache_find(f.rk, "cache-owned", 1);
                if (entry && entry->rkmce_mtopic.partition_cnt == 2) {
                        rd_kafka_metadata_partition_internal_t *parsed =
                            f.parsed->topics[0].partitions;
                        parts = entry->rkmce_metadata_internal_topic.partitions;
                        independent = parts[0].racks && parts[0].racks_cnt == 1 &&
                            parsed[0].racks && parsed[0].racks_cnt == 1 &&
                            parts[0].racks != parsed[0].racks &&
                            parts[0].racks[0] != parsed[0].racks[0] &&
                            parts[1].racks && !parts[1].racks_cnt &&
                            parsed[1].racks && !parsed[1].racks_cnt;
                }
                rd_kafka_rdunlock(f.rk);
                rd_kafka_metadata_destroy(&f.parsed->metadata);
                f.parsed = NULL;
                rd_kafka_rdlock(f.rk);
                entry = rd_kafka_metadata_cache_find(f.rk, "cache-owned", 1);
                if (entry && entry->rkmce_mtopic.partition_cnt == 2) {
                        parts = entry->rkmce_metadata_internal_topic.partitions;
                        retained = parts[0].racks && parts[0].racks_cnt == 1 &&
                            parts[0].racks[0] && !strcmp(parts[0].racks[0], "rack") &&
                            parts[1].racks && !parts[1].racks_cnt;
                }
                rd_kafka_rdunlock(f.rk);
        }
        ut_cache_parser_finish(&f);
        RD_UT_ASSERT(completed && !f.error && f.actor_seen && independent && retained,
                     "ordinary and admin parser cache values outlive their independent parsed arena");
        RD_UT_PASS();
}

int unittest_metadata_cache_parsers(void) {
        return ut_cache_parser_owners(0) + ut_cache_parser_owners(1);
}
