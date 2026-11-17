static const rd_kafka_ordered_metadata_topic_t ut_ordered_topic = {"z", 1};
static const unsigned char ut_ordered_v13_body[] = {
        2, 0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0, 2,'z',0,1,0
};
static const unsigned char ut_ordered_response[] = {
        0,0,0,0, 1,0, 255,255,255,255, 1,0,0,0
};

static int ut_ordered_brokers_only(rd_kafka_broker_t *rkb) {
        struct rd_kafka_ApiVersion api = {
                .ApiKey = RD_KAFKAP_Metadata, .MinVer = 0, .MaxVer = 0
        };
        unsigned char bytes[16];
        const unsigned char legacy[] = {0, 0, 0, 0};
        const unsigned char flexible[] = {1, 1, 0, 0};
        size_t size;
        int16_t v0_named, v0_brokers, v1_brokers, v1_disabled, v4_disabled;

        rkb->rkb_ApiVersions = &api;
        rkb->rkb_ApiVersions_cnt = 1;
        v0_named = rd_kafka_ordered_metadata_version(rkb, rd_false);
        v0_brokers = rd_kafka_ordered_metadata_version(rkb, rd_true);
        api.MaxVer = 1;
        v1_brokers = rd_kafka_ordered_metadata_version(rkb, rd_true);
        rkb->rkb_rk->rk_conf.allow_auto_create_topics = rd_false;
        v1_disabled = rd_kafka_ordered_metadata_version(rkb, rd_true);
        api.MaxVer = 4;
        v4_disabled = rd_kafka_ordered_metadata_version(rkb, rd_true);
        rkb->rkb_rk->rk_conf.allow_auto_create_topics = rd_true;
        rkb->rkb_ApiVersions = NULL;
        rkb->rkb_ApiVersions_cnt = 0;

        RD_UT_ASSERT(v0_named == 0 && v0_brokers == -1, "v0 empty means all topics");
        RD_UT_ASSERT(v1_brokers == 1, "v1 supports empty requests");
        RD_UT_ASSERT(v1_disabled == -1 && v4_disabled == 4,
                     "disabled auto creation requires v4");
        RD_UT_ASSERT(rd_kafka_ordered_body_size(NULL, 0, 0, &size) ==
                         RD_KAFKA_RESP_ERR__INVALID_ARG,
                     "forbid all-topic v0 empty array");
        RD_UT_ASSERT(!rd_kafka_ordered_body_size(NULL, 0, 1, &size) &&
                         size == sizeof(legacy), "v1 empty body size");
        rd_kafka_ordered_body_write(bytes, NULL, 0, 1, rd_true);
        RD_UT_ASSERT(!memcmp(bytes, legacy, size), "v1 empty topic array");
        RD_UT_ASSERT(!rd_kafka_ordered_body_size(NULL, 0, 9, &size) &&
                         size == sizeof(flexible), "v9 empty body size");
        rd_kafka_ordered_body_write(bytes, NULL, 0, 9, rd_true);
        RD_UT_ASSERT(!memcmp(bytes, flexible, size), "v9 non-null empty compact array and flags");
        RD_UT_PASS();
}

static int ut_ordered_target_selection(void) {
        rd_kafka_t rk = RD_ZERO_INIT;
        rd_kafka_broker_t brokers[2] = {RD_ZERO_INIT, RD_ZERO_INIT};
        rd_kafka_broker_t *first, *second, *absent, *down, *terminating;
        int first_ref, second_ref;
        size_t i;

        rwlock_init(&rk.rk_lock);
        rd_atomic32_init(&rk.rk_terminate, 0);
        rd_list_init(&rk.rk_broker_by_id, 2, NULL);
        for (i = 0; i < RD_ARRAYSIZE(brokers); i++) {
                rd_kafka_broker_t *broker = &brokers[i];
                broker->rkb_rk = &rk;
                broker->rkb_nodeid = (int32_t)i + 1;
                broker->rkb_state = RD_KAFKA_BROKER_STATE_UP;
                mtx_init(&broker->rkb_lock, mtx_plain);
                rd_refcnt_init(&broker->rkb_refcnt, 1);
                rd_atomic32_init(&broker->termination_in_progress, 0);
                rd_list_add(&rk.rk_broker_by_id, broker);
        }
        rd_list_sort(&rk.rk_broker_by_id, rd_kafka_broker_cmp_by_id);

        first = rd_kafka_ordered_metadata_find_broker(&rk, 1);
        second = rd_kafka_ordered_metadata_find_broker(&rk, 2);
        absent = rd_kafka_ordered_metadata_find_broker(&rk, 3);
        brokers[1].rkb_state = RD_KAFKA_BROKER_STATE_DOWN;
        down = rd_kafka_ordered_metadata_find_broker(&rk, 2);
        brokers[1].rkb_state = RD_KAFKA_BROKER_STATE_UP;
        rd_atomic32_set(&brokers[1].termination_in_progress, 1);
        terminating = rd_kafka_ordered_metadata_find_broker(&rk, 2);

        if (first)
                rd_kafka_broker_destroy(first);
        if (second)
                rd_kafka_broker_destroy(second);
        if (absent)
                rd_kafka_broker_destroy(absent);
        if (down)
                rd_kafka_broker_destroy(down);
        if (terminating)
                rd_kafka_broker_destroy(terminating);
        first_ref = rd_refcnt_sub(&brokers[0].rkb_refcnt);
        second_ref = rd_refcnt_sub(&brokers[1].rkb_refcnt);
        rd_list_destroy(&rk.rk_broker_by_id);
        for (i = 0; i < RD_ARRAYSIZE(brokers); i++) {
                rd_refcnt_destroy(&brokers[i].rkb_refcnt);
                mtx_destroy(&brokers[i].rkb_lock);
        }
        rwlock_destroy(&rk.rk_lock);

        RD_UT_ASSERT(first == &brokers[0] && second == &brokers[1],
                     "targeted lookup retains the exact broker owner");
        RD_UT_ASSERT(!absent && !down && !terminating,
                     "missing, non-Up and terminating targets are unavailable");
        RD_UT_ASSERT(first_ref == 0 && second_ref == 0,
                     "targeted lookup balances temporary broker references");
        RD_UT_PASS();
}

static int ut_ordered_endpoint_selection(void) {
        rd_kafka_t rk = RD_ZERO_INIT;
        rd_kafka_broker_t brokers[2] = {RD_ZERO_INIT, RD_ZERO_INIT};
        struct rd_kafka_ApiVersion api = {
                .ApiKey = RD_KAFKAP_Metadata, .MinVer = 1, .MaxVer = 12
        };
        struct rd_kafka_ordered_endpoint selected, alias, wrong_port, moved;
        char host[] = "host";
        size_t index;

        rwlock_init(&rk.rk_lock);
        rd_atomic32_init(&rk.rk_terminate, 0);
        TAILQ_INIT(&rk.rk_brokers);
        rk.rk_conf.allow_auto_create_topics = rd_true;
        for (index = 0; index < RD_ARRAYSIZE(brokers); index++) {
                rd_kafka_broker_t *broker = &brokers[index];
                broker->rkb_rk = &rk;
                broker->rkb_source = index == 0 ? RD_KAFKA_LEARNED : RD_KAFKA_CONFIGURED;
                broker->rkb_state = RD_KAFKA_BROKER_STATE_UP;
                broker->rkb_origname = host;
                rd_strlcpy(broker->rkb_nodename, "host:9092", sizeof(broker->rkb_nodename));
                broker->rkb_port = 9092;
                broker->rkb_connection_generation = index + 1;
                broker->rkb_ApiVersions = &api;
                broker->rkb_ApiVersions_cnt = 1;
                mtx_init(&broker->rkb_lock, mtx_plain);
                rd_refcnt_init(&broker->rkb_refcnt, 1);
                rd_atomic32_init(&broker->termination_in_progress, 0);
                TAILQ_INSERT_TAIL(&rk.rk_brokers, broker, rkb_link);
        }
        selected = rd_kafka_ordered_metadata_find_endpoint(&rk, "host", "host:9092", 9092, rd_true);
        if (selected.broker)
                rd_kafka_broker_destroy(selected.broker);
        alias = rd_kafka_ordered_metadata_find_endpoint(&rk, "HOST", "HOST:9092", 9092, rd_true);
        wrong_port = rd_kafka_ordered_metadata_find_endpoint(&rk, "host", "host:9093", 9093, rd_true);
        brokers[1].rkb_nodename_epoch++;
        moved = rd_kafka_ordered_metadata_find_endpoint(&rk, "host", "host:9092", 9092, rd_true);
        RD_UT_ASSERT(selected.broker == &brokers[1] && selected.generation == 2 &&
                         selected.version == 12, "configured connection and its negotiated version");
        RD_UT_ASSERT(!alias.broker && !wrong_port.broker && !moved.broker,
                     "exact configured endpoint and connected address epoch");
        for (index = 0; index < RD_ARRAYSIZE(brokers); index++) {
                RD_UT_ASSERT(rd_refcnt_sub(&brokers[index].rkb_refcnt) == 0,
                             "endpoint lookup balances broker references");
                rd_refcnt_destroy(&brokers[index].rkb_refcnt);
                mtx_destroy(&brokers[index].rkb_lock);
        }
        rwlock_destroy(&rk.rk_lock);
        RD_UT_PASS();
}

static int ut_ordered_stale_connection(rd_kafka_broker_t *rkb) {
        rd_kafka_t *rk = rkb->rkb_rk;
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(65536);
        rd_kafka_ordered_metadata_t *result;
        rd_kafka_resp_err_t error;
        int16_t version;
        const void *body;
        size_t length;

        RD_UT_ASSERT(account, "allocate native account");
        rk->rk_ops = NULL;
        rk->rk_byte_account = account;
        rkb->rkb_source = RD_KAFKA_CONFIGURED;
        rkb->rkb_state = RD_KAFKA_BROKER_STATE_UP;
        rkb->rkb_connection_generation = 1;
        rkb->rkb_max_inflight = 1;
        rd_kafka_bufq_init(&rkb->rkb_outbufs);
        RD_UT_ASSERT(!rd_kafka_ordered_metadata_submit_on_connection(
                         rkb, NULL, 0, 13, 64, 1000, 1, &result),
                     "admit request on generation one");
        RD_UT_ASSERT(rd_kafka_bufq_cnt(&rkb->rkb_outbufs) == 1,
                     "request queued before reconnect");
        rkb->rkb_connection_generation = 2;
        RD_UT_ASSERT(rd_kafka_send(rkb) == 0 && rd_kafka_bufq_cnt(&rkb->rkb_outbufs) == 0,
                     "no request bytes transmitted on generation two");
        RD_UT_ASSERT(rd_kafka_ordered_metadata_poll(result, &error, &version, &body, &length) &&
                         error == RD_KAFKA_RESP_ERR__TRANSPORT && version == 13 && length == 0,
                     "stale connection is terminal without retry");
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == sizeof(*result) + 64,
                     "only the caller completion remains charged");
        rd_kafka_ordered_metadata_destroy(result);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == 0, "release final completion");
        rd_kafka_byte_account_destroy(account);
        rk->rk_byte_account = NULL;
        RD_UT_PASS();
}

static int ut_ordered_encode(void) {
        unsigned char bytes[64];
        size_t size;
        int16_t version;
        const unsigned char legacy[] = {0,0,0,1,0,1,'z'};
        RD_UT_ASSERT(!rd_kafka_ordered_body_size(&ut_ordered_topic,1,0,&size) && size==sizeof(legacy), "legacy size");
        rd_kafka_ordered_body_write(bytes,&ut_ordered_topic,1,0,rd_true);
        RD_UT_ASSERT(!memcmp(bytes,legacy,size), "legacy named topic");
        RD_UT_ASSERT(!rd_kafka_ordered_body_size(&ut_ordered_topic,1,13,&size) && size==sizeof(ut_ordered_v13_body), "v13 size");
        rd_kafka_ordered_body_write(bytes,&ut_ordered_topic,1,13,rd_true);
        RD_UT_ASSERT(!memcmp(bytes,ut_ordered_v13_body,size), "UUID, topic, tags, auto-create and auth flags");
        for (version=0; version<=13; version++) {
                RD_UT_ASSERT(!rd_kafka_ordered_body_size(&ut_ordered_topic,1,version,&size), "version %d size",version);
                memset(bytes,99,sizeof(bytes));
                rd_kafka_ordered_body_write(bytes,&ut_ordered_topic,1,version,rd_false);
                RD_UT_ASSERT(bytes[size]==99 && bytes[size-1]!=99, "exact v%d encoding extent",version);
        }
        RD_UT_ASSERT(rd_kafka_ordered_body_size(&ut_ordered_topic,0,13,&size)==RD_KAFKA_RESP_ERR__INVALID_ARG,"no all-cluster request");
        RD_UT_ASSERT(rd_kafka_ordered_body_size(&ut_ordered_topic,SIZE_MAX,13,&size)==RD_KAFKA_RESP_ERR__INVALID_ARG,"count rejected before input read");
        RD_UT_PASS();
}

static int ut_ordered_denial(rd_kafka_broker_t *rkb, int stage) {
        rd_kafka_t *rk = rkb->rkb_rk;
        size_t completion = sizeof(rd_kafka_ordered_metadata_t)+64;
        size_t request, size = sizeof(ut_ordered_v13_body);
        size_t capacity;
        rd_kafka_byte_account_t *account;
        rd_kafka_ordered_metadata_t *result = NULL;
        rd_kafka_resp_err_t error, published;
        const void *body;
        size_t length;
        int16_t version;
        rk->rk_byte_account = rd_kafka_byte_account_new(0);
        RD_UT_ASSERT(!rd_kafka_buf_accounted_metadata_size(rkb,13,size,&request),"sizing without admission");
        rd_kafka_byte_account_destroy(rk->rk_byte_account);
        capacity = completion - 1;
        if (stage>=1) capacity += size;
        if (stage>=2) capacity += request;
        if (stage>=3) capacity += ut_queue_node_charge();
        account = rd_kafka_byte_account_new(capacity);
        RD_UT_ASSERT(account,"create account"); rk->rk_byte_account = account;
        rkb->rkb_ops = rd_kafka_q_new(rk); rk->rk_ops = rd_kafka_q_new(rk);
        error = rd_kafka_ordered_metadata_submit(rkb,&ut_ordered_topic,1,13,64,1000,&result);
        if (stage<3) {
                RD_UT_ASSERT(error==RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE && !result,"pre-submission denial %d",stage);
                RD_UT_ASSERT(rd_kafka_byte_account_used(account)==0,"rejected submission released all owners");
        } else {
                RD_UT_ASSERT(!error && result,"node denial accepted then completed inline");
                RD_UT_ASSERT(rd_kafka_ordered_metadata_poll(result,&published,&version,&body,&length) && published==RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE,"typed direct error");
                RD_UT_ASSERT(rd_kafka_q_len(rk->rk_ops)==0 && rd_kafka_q_len(rkb->rkb_ops)==0,"no reply allocated after denial");
                RD_UT_ASSERT(rd_kafka_byte_account_used(account)==completion,"only caller result retained");
                rd_kafka_ordered_metadata_destroy(result);
        }
        RD_UT_ASSERT(rd_kafka_byte_account_used(account)==0,"last completion released");
        rd_kafka_q_destroy(rk->rk_ops); rd_kafka_q_destroy(rkb->rkb_ops);
        rd_kafka_byte_account_destroy(account); rk->rk_byte_account=NULL;
        RD_UT_PASS();
}

static int ut_ordered_complete(rd_kafka_broker_t *rkb, int mode) {
        rd_kafka_t *rk = rkb->rkb_rk;
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(65536);
        rd_kafka_ordered_metadata_t *result;
        rd_kafka_buf_t *request, *frame;
        rd_kafka_op_t *transmit, *reply;
        rd_kafka_resp_err_t error;
        const void *body;
        size_t length;
        int16_t version;
        size_t cap = mode==4 ? 0 : mode==1 ? sizeof(ut_ordered_response)-1 : sizeof(ut_ordered_response);
        RD_UT_ASSERT(account,"create account"); rk->rk_byte_account=account;
        rkb->rkb_ops=rd_kafka_q_new(rk); rk->rk_ops=rd_kafka_q_new(rk);
        RD_UT_ASSERT(!rd_kafka_ordered_metadata_submit(rkb,&ut_ordered_topic,1,13,cap,1000,&result),"accepted request");
        RD_UT_ASSERT(!rd_kafka_ordered_metadata_poll(result,&error,&version,&body,&length),"pending before response");
        transmit=rd_kafka_q_pop(rkb->rkb_ops,0,0);
        RD_UT_ASSERT(transmit && transmit->rko_type==RD_KAFKA_OP_XMIT_BUF,"actual accounted transmit queue");
        request=transmit->rko_u.xbuf.rkbuf; transmit->rko_u.xbuf.rkbuf=NULL;
        RD_UT_ASSERT(request->rkbuf_abs_timeout>rd_clock(),"operation deadline installed");
        frame=rd_kafka_recv_buf_new(account);
        RD_UT_ASSERT(frame && rd_kafka_recv_buf_grow(frame,sizeof(ut_ordered_response)),"receive storage admitted");
        rd_buf_write(&frame->rkbuf_buf,ut_ordered_response,sizeof(ut_ordered_response));
        rd_slice_init_full(&frame->rkbuf_reader,&frame->rkbuf_buf);
        rd_kafka_buf_keep(frame); rd_kafka_buf_keep(request);
        if (mode==2) rd_kafka_q_disable(rk->rk_ops);
        if (mode==3) rd_kafka_ordered_metadata_destroy(result);
        rd_kafka_buf_callback(rk,rkb,mode==4 ? RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED : 0,frame,request);
        if (mode!=2) {
                reply=rd_kafka_q_pop_serve(rk->rk_ops,0,0,RD_KAFKA_Q_CB_FORCE_RETURN,NULL,NULL);
                RD_UT_ASSERT(reply && reply->rko_type==RD_KAFKA_OP_RECV_BUF,"actual accounted reply queue");
                rd_kafka_buf_handle_op(reply,reply->rko_err);
                rd_kafka_op_destroy(reply);
        }
        if (mode!=3) {
                RD_UT_ASSERT(rd_kafka_ordered_metadata_poll(result,&error,&version,&body,&length),"terminal publication");
                RD_UT_ASSERT(error==(mode==4 ? RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED : mode==1 ? RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE : mode==2 ? RD_KAFKA_RESP_ERR__DESTROY : 0),"native error precedes bounded-copy or queue error");
                RD_UT_ASSERT(version==13,"negotiated version retained");
                if (!mode) RD_UT_ASSERT(length==sizeof(ut_ordered_response) && !memcmp(body,ut_ordered_response,length),"exact response bytes");
                else RD_UT_ASSERT(length==0,"no partial output on failure");
                rd_kafka_ordered_metadata_destroy(result);
        }
        RD_UT_ASSERT(rd_kafka_byte_account_used(account)==request->rkbuf_byte_charge+frame->rkbuf_byte_charge+ut_queue_node_charge(),"publication/result drop leave request/frame/transmit owners charged");
        rd_kafka_buf_destroy(request); rd_kafka_buf_destroy(frame);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account)==ut_queue_node_charge(),"queue node outlives its transferred request");
        rd_kafka_op_destroy(transmit);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account)==0,"last native owner released");
        rd_kafka_q_destroy(rk->rk_ops); rd_kafka_q_destroy(rkb->rkb_ops);
        rd_kafka_byte_account_destroy(account); rk->rk_byte_account=NULL;
        RD_UT_PASS();
}

static int ut_ordered_retry(rd_kafka_broker_t *rkb, rd_bool_t deny) {
        rd_kafka_t *rk=rkb->rkb_rk;
        rd_kafka_byte_account_t *account=rd_kafka_byte_account_new(65536);
        rd_kafka_ordered_metadata_t *result;
        rd_kafka_op_t *op;
        rd_kafka_buf_t *request;
        rd_kafka_resp_err_t error;
        const void *body;
        size_t length, held=0;
        int16_t version;
        RD_UT_ASSERT(account,"create account"); rk->rk_byte_account=account;
        rkb->rkb_ops=rd_kafka_q_new(rk); rk->rk_ops=rd_kafka_q_new(rk);
        RD_UT_ASSERT(!rd_kafka_ordered_metadata_submit(rkb,&ut_ordered_topic,1,13,32,1000,&result),"start retry candidate");
        op=rd_kafka_q_pop(rkb->rkb_ops,0,0); RD_UT_ASSERT(op,"transmit");
        request=op->rko_u.xbuf.rkbuf; op->rko_u.xbuf.rkbuf=NULL; rd_kafka_op_destroy(op);
        rd_kafka_replyq_destroy(&request->rkbuf_replyq);
        if (deny) {
                held=65536-rd_kafka_byte_account_used(account);
                RD_UT_ASSERT(rd_kafka_byte_account_acquire(account,held),"fill retry-node capacity");
        }
        rd_kafka_buf_callback(rk,rkb,RD_KAFKA_RESP_ERR__TRANSPORT,NULL,request);
        if (!deny) {
                RD_UT_ASSERT(!rd_kafka_ordered_metadata_poll(result,&error,&version,&body,&length),"retry did not publish or release accepted reference");
                op=rd_kafka_q_pop(rkb->rkb_ops,0,0);
                RD_UT_ASSERT(op && op->rko_type==RD_KAFKA_OP_XMIT_RETRY,"accounted retry queue");
                request=op->rko_u.xbuf.rkbuf; op->rko_u.xbuf.rkbuf=NULL; rd_kafka_op_destroy(op);
                request->rkbuf_abs_timeout=1;
                rd_kafka_buf_callback(rk,rkb,RD_KAFKA_RESP_ERR__TIMED_OUT,NULL,request);
        }
        RD_UT_ASSERT(rd_kafka_ordered_metadata_poll(result,&error,&version,&body,&length),"terminal retry result");
        RD_UT_ASSERT(error==(deny ? RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE : RD_KAFKA_RESP_ERR__TIMED_OUT),"retry error distinction");
        if (held) rd_kafka_byte_account_release(account,held);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account)==sizeof(*result)+32,"exact retained completion after nested callback or expiry");
        rd_kafka_ordered_metadata_destroy(result);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account)==0,"retry owner released once");
        rd_kafka_q_destroy(rk->rk_ops); rd_kafka_q_destroy(rkb->rkb_ops);
        rd_kafka_byte_account_destroy(account); rk->rk_byte_account=NULL;
        RD_UT_PASS();
}

static int ut_ordered_invalid_header_before_admission(rd_kafka_broker_t *rkb) {
        rd_kafka_t *rk=rkb->rkb_rk;
        rd_kafkap_str_t invalid=RD_ZERO_INIT;
        rd_kafkap_str_t *original=rk->rk_client_id;
        rd_kafka_ordered_metadata_t *result;
        rk->rk_byte_account=rd_kafka_byte_account_new(0);
        RD_UT_ASSERT(rk->rk_byte_account,"create empty account");
        invalid.len=INT_MAX; rk->rk_client_id=&invalid;
        RD_UT_ASSERT(rd_kafka_ordered_metadata_submit(rkb,&ut_ordered_topic,1,13,32,1000,&result)==RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE && !result,"invalid client ID precedes completion admission");
        invalid.len=-2;
        RD_UT_ASSERT(rd_kafka_ordered_metadata_submit(rkb,&ut_ordered_topic,1,13,32,1000,&result)==RD_KAFKA_RESP_ERR__INVALID_ARG && !result,"negative client ID precedes admission");
        rk->rk_client_id=original;
        RD_UT_ASSERT(rd_kafka_byte_account_used(rk->rk_byte_account)==0,"no invalid-header allocation");
        rd_kafka_byte_account_destroy(rk->rk_byte_account); rk->rk_byte_account=NULL;
        RD_UT_PASS();
}

static int ut_ordered_header_case(rd_kafka_broker_t *rkb,
                                    const unsigned char *tags, size_t tags_size,
                                    rd_kafka_resp_err_t expected) {
        rd_kafka_t *rk=rkb->rkb_rk;
        rd_kafka_byte_account_t *account=rd_kafka_byte_account_new(65536);
        rd_kafka_ordered_metadata_t *result;
        rd_kafka_buf_t *request,*frame;
        unsigned char header[8]={0,0,0,0,0,0,0,1};
        uint32_t wire_size=(uint32_t)(4+tags_size+sizeof(ut_ordered_response));
        rd_kafka_resp_err_t error;
        const void *body;
        size_t length;
        int16_t version;
        header[0]=(unsigned char)(wire_size>>24);
        header[1]=(unsigned char)(wire_size>>16);
        header[2]=(unsigned char)(wire_size>>8);
        header[3]=(unsigned char)wire_size;
        RD_UT_ASSERT(thrd_is_current(rkb->rkb_thread),"header cases run on broker thread");
        RD_UT_ASSERT(account,"create header account"); rk->rk_byte_account=account;
        result=rd_kafka_ordered_metadata_new(account,sizeof(ut_ordered_response));
        RD_UT_ASSERT(result,"admit completion"); rd_kafka_ordered_metadata_keep(result);
        RD_UT_ASSERT(!rd_kafka_buf_accounted_metadata(rkb,13,ut_ordered_v13_body,sizeof(ut_ordered_v13_body),&request),"accounted request");
        request->rkbuf_cb=rd_kafka_ordered_metadata_complete; request->rkbuf_opaque=result;
        request->rkbuf_corrid=1; request->rkbuf_ts_sent=rd_clock();
        rd_kafka_bufq_enq(&rkb->rkb_waitresps,request);
        frame=rd_kafka_recv_buf_new(account); RD_UT_ASSERT(frame,"header frame");
        rd_buf_write(&frame->rkbuf_buf,header,sizeof(header));
        RD_UT_ASSERT(rd_kafka_recv_buf_grow(frame,tags_size+sizeof(ut_ordered_response)),"body growth");
        rd_buf_write(&frame->rkbuf_buf,tags,tags_size);
        rd_buf_write(&frame->rkbuf_buf,ut_ordered_response,sizeof(ut_ordered_response));
        frame->rkbuf_reshdr.CorrId=1;
        frame->rkbuf_reshdr.Size=(int32_t)wire_size;
        frame->rkbuf_totlen=tags_size+sizeof(ut_ordered_response);
        RD_UT_ASSERT(rd_kafka_req_response(rkb,frame)==(expected ? -1 : 0),"actual ordered header dispatch");
        RD_UT_ASSERT(rd_kafka_bufq_cnt(&rkb->rkb_waitresps)==0,"request consumed once");
        RD_UT_ASSERT(rd_kafka_ordered_metadata_poll(result,&error,&version,&body,&length) && error==expected,"header result publication");
        RD_UT_ASSERT(version==13,"negotiated version retained");
        if (!expected)
                RD_UT_ASSERT(length==sizeof(ut_ordered_response) && !memcmp(body,ut_ordered_response,length),"body excludes correlation and flexible header tags");
        else
                RD_UT_ASSERT(length==0,"header failure precedes body copy");
        rd_kafka_ordered_metadata_destroy(result);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account)==0,"header test released every owner");
        rd_kafka_byte_account_destroy(account); rk->rk_byte_account=NULL;
        RD_UT_PASS();
}

static int ut_ordered_header_boundary(rd_kafka_broker_t *rkb) {
        const unsigned char tags[]={1,7,2,91,92};
        const unsigned char duplicate[]={2,7,0,7,0};
        const unsigned char decreasing[]={2,7,0,6,0};
        const unsigned char negative_id[]={1,255,255,255,255,15,0};
        const unsigned char negative_count[]={255,255,255,255,15};
        const unsigned char negative_size[]={1,0,255,255,255,255,15};
        const unsigned char continuing[]={128,128,128,128,128};
        unsigned char terminal[]={128,128,128,128,0};
        unsigned char many[20005];
        unsigned char *end;
        uint32_t count;
        unsigned int high;
        int fails=0;
        fails+=ut_ordered_header_case(rkb,tags,sizeof(tags),0);
        fails+=ut_ordered_header_case(rkb,duplicate,sizeof(duplicate),0);
        fails+=ut_ordered_header_case(rkb,decreasing,sizeof(decreasing),0);
        fails+=ut_ordered_header_case(rkb,negative_id,sizeof(negative_id),0);
        fails+=ut_ordered_header_case(rkb,negative_count,sizeof(negative_count),RD_KAFKA_RESP_ERR__BAD_MSG);
        fails+=ut_ordered_header_case(rkb,negative_size,sizeof(negative_size),RD_KAFKA_RESP_ERR__BAD_MSG);
        fails+=ut_ordered_header_case(rkb,continuing,sizeof(continuing),RD_KAFKA_RESP_ERR__BAD_MSG);
        for (high=0;high<128;high+=16) {
                terminal[4]=(unsigned char)high;
                fails+=ut_ordered_header_case(rkb,terminal,sizeof(terminal),0);
        }
        for (count=10000;count<=10001;count++) {
                end=rd_kafka_ordered_varint(many,count);
                memset(end,0,2*count);
                fails+=ut_ordered_header_case(rkb,many,(size_t)(end-many)+2*count,
                                             count==10000 ? 0 : RD_KAFKA_RESP_ERR__BAD_MSG);
        }
        return fails;
}

static int ut_ordered_version_intersection(rd_kafka_broker_t *rkb) {
        struct rd_kafka_ApiVersion api={.ApiKey=RD_KAFKAP_Metadata,.MinVer=7,.MaxVer=9};
        rkb->rkb_ApiVersions=&api; rkb->rkb_ApiVersions_cnt=1;
        RD_UT_ASSERT(rd_kafka_broker_ApiVersion_supported(rkb,RD_KAFKAP_Metadata,0,13,NULL)==9,"negotiated broker maximum");
        api.MaxVer=15;
        RD_UT_ASSERT(rd_kafka_broker_ApiVersion_supported(rkb,RD_KAFKAP_Metadata,0,13,NULL)==13,"transport version ceiling");
        api.MinVer=14;
        RD_UT_ASSERT(rd_kafka_broker_ApiVersion_supported(rkb,RD_KAFKAP_Metadata,0,13,NULL)==-1,"unsupported future-only broker");
        api.MinVer=0; api.MaxVer=3;
        RD_UT_ASSERT(rd_kafka_broker_ApiVersion_supported(rkb,RD_KAFKAP_Metadata,4,13,NULL)==-1,"cannot disable auto-creation on legacy broker");
        rkb->rkb_ApiVersions=NULL; rkb->rkb_ApiVersions_cnt=0;
        RD_UT_PASS();
}

struct ut_ordered_queue_cases {
        rd_kafka_broker_t *broker;
};

static int ut_ordered_queue_cases_run(void *opaque) {
        const struct ut_ordered_queue_cases *cases=opaque;
        rd_kafka_broker_t *rkb=cases->broker;
        int fails=0, stage;
        RD_UT_ASSERT(!thrd_is_current(rkb->rkb_thread),"queue cases run off broker thread");
        for (stage=0;stage<4;stage++) fails+=ut_ordered_denial(rkb,stage);
        for (stage=0;stage<5;stage++) fails+=ut_ordered_complete(rkb,stage);
        fails+=ut_ordered_retry(rkb,rd_false);
        fails+=ut_ordered_retry(rkb,rd_true);
        return fails;
}

static int ut_ordered_queue_cases_joined(rd_kafka_broker_t *rkb) {
        struct ut_ordered_queue_cases cases={rkb};
        thrd_t worker;
        int fails;
        RD_UT_ASSERT(thrd_create(&worker,ut_ordered_queue_cases_run,&cases)==thrd_success,"create queue-case helper");
        RD_UT_ASSERT(thrd_join(worker,&fails)==thrd_success,"join queue-case helper before releasing fixture owners");
        return fails;
}

int unittest_ordered_metadata(void) {
        rd_kafka_t rk=RD_ZERO_INIT;
        rd_kafka_broker_t rkb=RD_ZERO_INIT;
        int fails=0;
#if defined(_TTHREAD_WIN32_)
        RD_UT_ASSERT(DuplicateHandle(GetCurrentProcess(),thrd_current(),
                                    GetCurrentProcess(),&rkb.rkb_thread,
                                    0,FALSE,DUPLICATE_SAME_ACCESS),"stable broker thread identity");
#else
        rkb.rkb_thread=thrd_current();
#endif
        rk.rk_client_id=rd_kafkap_str_new("x",-1);
        rk.rk_conf.allow_auto_create_topics=rd_true;
        rkb.rkb_rk=&rk; rkb.rkb_logname="ordered-unit";
        rd_kafka_bufq_init(&rkb.rkb_waitresps);
        rd_atomic64_init(&rkb.rkb_c.rx_err,0);
        rd_avg_init(&rkb.rkb_avg_rtt,RD_AVG_GAUGE,0,0,0,0);
        rd_avg_init(&rkb.rkb_telemetry.rd_avg_current.rkb_avg_rtt,RD_AVG_GAUGE,0,0,0,0);
        mtx_init(&rkb.rkb_logname_lock,mtx_plain);
        mtx_init(&rkb.rkb_lock,mtx_plain);
        rd_refcnt_init(&rkb.rkb_refcnt,1);
        fails+=ut_ordered_encode();
        RD_UT_ASSERT(rd_refcnt_get(&rkb.rkb_refcnt)==1,"encoding retains no broker reference");
        fails+=ut_ordered_version_intersection(&rkb);
        RD_UT_ASSERT(rd_refcnt_get(&rkb.rkb_refcnt)==1,"version selection releases broker references");
        fails += ut_ordered_brokers_only(&rkb);
        fails += ut_ordered_target_selection();
        fails+=ut_ordered_header_boundary(&rkb);
        RD_UT_ASSERT(rd_refcnt_get(&rkb.rkb_refcnt)==1,"header cases release broker references: %d",rd_refcnt_get(&rkb.rkb_refcnt));
        fails+=ut_ordered_invalid_header_before_admission(&rkb);
        RD_UT_ASSERT(rd_refcnt_get(&rkb.rkb_refcnt)==1,"invalid headers retain no broker reference");
        fails+=ut_ordered_queue_cases_joined(&rkb);
        RD_UT_ASSERT(rd_refcnt_get(&rkb.rkb_refcnt)==1,"joined queue cases release broker references: %d",rd_refcnt_get(&rkb.rkb_refcnt));
        RD_UT_ASSERT(thrd_is_current(rkb.rkb_thread),"broker identity retained after joined queue cases");
        fails += ut_ordered_endpoint_selection();
        fails += ut_ordered_stale_connection(&rkb);
        RD_UT_ASSERT(rd_refcnt_sub(&rkb.rkb_refcnt)==0,"last ordered broker reference");
        rd_refcnt_destroy(&rkb.rkb_refcnt); mtx_destroy(&rkb.rkb_logname_lock); mtx_destroy(&rkb.rkb_lock);
        rd_avg_destroy(&rkb.rkb_avg_rtt);
        rd_avg_destroy(&rkb.rkb_telemetry.rd_avg_current.rkb_avg_rtt);
        rd_kafkap_str_destroy(rk.rk_client_id);
#if defined(_TTHREAD_WIN32_)
        RD_UT_ASSERT(CloseHandle(rkb.rkb_thread),"release stable broker thread handle");
#endif
        return fails;
}
