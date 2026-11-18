#include "rdunittest.h"

int unittest_connection_op_allocation(void);

static void *ut_connection_op_allocate_denied(size_t count, size_t size) {
        (void)count;
        (void)size;
        return NULL;
}

int unittest_connection_op_allocation(void) {
        rd_kafka_byte_account_t *account = rd_kafka_byte_account_new(sizeof(rd_kafka_op_t));
        rd_kafka_op_t *op;
        RD_UT_ASSERT(account, "create fallible control account");
        op = rd_kafka_op_new_accounted0(RD_KAFKA_OP_SOURCE,
            RD_KAFKA_OP_CONNECTION_OBSERVATION, account, ut_connection_op_allocate_denied);
        RD_UT_ASSERT(!op && rd_kafka_byte_account_used(account) == 0,
                     "native control allocation failure returns its admitted charge");
        op = rd_kafka_op_new_accounted(RD_KAFKA_OP_SOURCE, RD_KAFKA_OP_CONNECTION_OBSERVATION, account);
        RD_UT_ASSERT(op && !strcmp(rd_kafka_op2str(op->rko_type), "CONNECTION_OBSERVATION"),
                     "native control has its own explicit type, size and name");
        rd_kafka_op_destroy(op);
        RD_UT_ASSERT(rd_kafka_byte_account_used(account) == 0, "empty control destruction returns its exact charge");
        rd_kafka_byte_account_destroy(account);
        RD_UT_PASS();
}
