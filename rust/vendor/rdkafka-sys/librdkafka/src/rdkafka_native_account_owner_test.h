int unittest_native_account_owner_enqueue(rd_kafka_queue_t *queue);

int unittest_native_account_owner_enqueue(rd_kafka_queue_t *queue) {
        rd_kafka_op_t *op = rd_kafka_op_new_accounted(
            __FUNCTION__, RD_KAFKA_OP_ERR, queue->rkqu_rk->rk_byte_account);
        if (!op)
                return 0;
        op->rko_err = RD_KAFKA_RESP_ERR__FAIL;
        return rd_kafka_q_enq(queue->rkqu_q, op);
}
