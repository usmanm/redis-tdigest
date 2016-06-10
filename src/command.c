/*
 * src/command.c
 *
 * Copyright (c) 2016, Usman Masood <usmanm at fastmail dot fm>
 */

#include "command.h"
#include "tdigest.h"

#define TYPE_NAME "t-digest0"
#define ENCODING_VER 0

static RedisModuleType *TDigestType;

/* ========================== "tdigest" type commands ======================= */

/* TDIGEST.ADD key value count */
static int TDigestTypeAdd_RedisCommand(RedisModuleCtx *ctx,
        RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    if (argc != 4)
        return RedisModule_WrongArity(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1],
            REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY
            && RedisModule_ModuleTypeGetType(key) != TDigestType) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    double value;
    if ((RedisModule_StringToDouble(argv[2], &value) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,
                "ERR invalid value: must be a double");
    }

    long long count;
    if ((RedisModule_StringToLongLong(argv[3], &count) != REDISMODULE_OK)
            || count <= 0) {
        return RedisModule_ReplyWithError(ctx,
                "ERR invalid count: must be a positive 64 bit integer");
    }

    struct TDigest *t;
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
        t = tdigestNew();
        RedisModule_ModuleTypeSetValue(key, TDigestType, t);
    } else {
        t = RedisModule_ModuleTypeGetValue(key);
    }

    tdigestAdd(t, value, count);

    RedisModule_ReplyWithLongLong(ctx, count);
    RedisModule_ReplicateVerbatim(ctx);

    return REDISMODULE_OK;
}

/* TDIGEST.CDF key value */
static int TDigestTypeCDF_RedisCommand(RedisModuleCtx *ctx,
        RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    if (argc != 3)
        return RedisModule_WrongArity(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1],
            REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY
            && RedisModule_ModuleTypeGetType(key) != TDigestType) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    double value;
    if ((RedisModule_StringToDouble(argv[2], &value) != REDISMODULE_OK)) {
        return RedisModule_ReplyWithError(ctx,
                "ERR invalid value: must be a double");
    }

    struct TDigest *t = RedisModule_ModuleTypeGetValue(key);

    RedisModule_ReplyWithDouble(ctx, tdigestCDF(t, value));

    return REDISMODULE_OK;
}

/* TDIGEST.QUANTILE key quantile */
static int TDigestTypeQuantile_RedisCommand(RedisModuleCtx *ctx,
        RedisModuleString **argv, int argc) {
    RedisModule_AutoMemory(ctx); /* Use automatic memory management. */

    if (argc != 3)
        return RedisModule_WrongArity(ctx);

    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1],
    REDISMODULE_READ | REDISMODULE_WRITE);
    int type = RedisModule_KeyType(key);
    if (type != REDISMODULE_KEYTYPE_EMPTY
            && RedisModule_ModuleTypeGetType(key) != TDigestType) {
        return RedisModule_ReplyWithError(ctx, REDISMODULE_ERRORMSG_WRONGTYPE);
    }

    double quantile;
    if ((RedisModule_StringToDouble(argv[2], &quantile) != REDISMODULE_OK)
            || quantile < 0 || quantile > 1) {
        return RedisModule_ReplyWithError(ctx,
                "ERR invalid quantile: must be a double between 0..1");
    }

    struct TDigest *t = RedisModule_ModuleTypeGetValue(key);

    RedisModule_ReplyWithDouble(ctx, tdigestQuantile(t, quantile));

    return REDISMODULE_OK;
}

/* ========================== "tdigest" type methods ======================= */

static void *TDigestTypeRdbLoad(RedisModuleIO *rdb, int encver) {
    if (encver != ENCODING_VER) {
        return NULL;
    }

    uint64_t num_centroids = RedisModule_LoadUnsigned(rdb);
    struct TDigest *t = tdigestNew();

    while (num_centroids--) {
        double mean = RedisModule_LoadDouble(rdb);
        long long weight = RedisModule_LoadUnsigned(rdb);

        tdigestAdd(t, mean, weight);
    }
    return t;
}

static void TDigestTypeRdbSave(RedisModuleIO *rdb, void *value) {
    struct TDigest *t = value;

    tdigestCompress(t);

    RedisModule_SaveUnsigned(rdb, t->num_centroids);

    int i;
    for (i = 0; i < t->num_centroids; i++) {
        struct Centroid *c = &t->centroids[i];
        RedisModule_SaveDouble(rdb, c->mean);
        RedisModule_SaveUnsigned(rdb, c->weight);
    }
}

static void TDigestTypeAofRewrite(RedisModuleIO *aof, RedisModuleString *key,
        void *value) {
    struct TDigest *t = value;
    int i;

    tdigestCompress(t);

    for (i = 0; i < t->num_centroids; i++) {
        struct Centroid *c = &t->centroids[i];
        RedisModule_EmitAOF(aof, "TDIGEST.ADD", "%s %f %ll", key, c->mean,
                c->weight);
    }
}

static void TDigestTypeDigest(RedisModuleDigest *digest, void *value) {
    /* TODO: The DIGEST module interface is yet not implemented. */
}

static void TDigestTypeFree(void *value) {
    tdigestFree(value);
}

int RedisModule_OnLoad(RedisModuleCtx *ctx) {
    if (RedisModule_Init(ctx, TYPE_NAME, 1,
            REDISMODULE_APIVER_1) == REDISMODULE_ERR)
    {
        printf("FUCK 0");
        return REDISMODULE_ERR;
    }

    TDigestType = RedisModule_CreateDataType(ctx, TYPE_NAME, ENCODING_VER,
            TDigestTypeRdbLoad, TDigestTypeRdbSave, TDigestTypeAofRewrite,
            TDigestTypeDigest, TDigestTypeFree);
    if (TDigestType == NULL)
    {
        printf("FUCK 1");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "tdigest.add",
            TDigestTypeAdd_RedisCommand, "write deny-oom", 1, 1,
            1) == REDISMODULE_ERR)
    {
        printf("FUCK 2");
        return REDISMODULE_ERR;
       }

    if (RedisModule_CreateCommand(ctx, "tdigest.cdf",
            TDigestTypeCDF_RedisCommand, "readonly", 1, 1, 1) == REDISMODULE_ERR)
    {
        printf("FUCK 3");
        return REDISMODULE_ERR;
    }

    if (RedisModule_CreateCommand(ctx, "tdigest.quantile",
            TDigestTypeQuantile_RedisCommand, "readonly", 1, 1,
            1) == REDISMODULE_ERR)
    {
        printf("FUCK 4");
        return REDISMODULE_ERR;
    }

    return REDISMODULE_OK;
}
