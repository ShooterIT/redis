#include "server.h"
#include "client_comp.h"
#include <zstd.h>

/* Abstraction over compression library */
typedef struct compressionType {
    int (*init_compress)(struct compressionState *st, int level);
    int (*init_decompress)(struct compressionState *st);
    int (*compress)(struct compressionState *st, int flush);
    int (*decompress)(struct compressionState *st);
    void (*end)(struct compressionState *st);
} compressionType;

/* Temporary buffer used by compression library to store compressed/decompressed
 * data. */
typedef struct {
    unsigned char *data;
    int size;
    int written;
    int consumed;
} tempBuf;

/* Main compression state struct */
struct compressionState {
    const compressionType *type;
    tempBuf input;  /* Buffer holding compressed data */
    tempBuf output; /* Buffer holding uncompressed(decompressed) data */
    union {
        ZSTD_CStream *zstdCCtx;  /* Zstd compression ctx */
        ZSTD_DStream *zstdDCtx;  /* Zstd decompression ctx */
    } ctx;
    int write_flush_pending;    /* write flush not yet completed */
    int read_flush_pending;     /* read flush not yet completed */
    int drain_only;             /* skip connRead; only drain internal decompression buffers */
    mstime_t last_write;  /* Time since last write. Used to check if it's time
                           * to flush the buffer */
    compressionDirection dir;
};

/* --- zstd --- */

static int zstdInitCompress(compressionState *st, int level) {
    st->ctx.zstdCCtx = ZSTD_createCStream();
    if (!st->ctx.zstdCCtx) {
        serverLog(LL_NOTICE, "Failed to create ZSTD compression context");
        return -1;
    }
    size_t res = ZSTD_CCtx_setParameter(st->ctx.zstdCCtx, ZSTD_c_compressionLevel, level);
    if (ZSTD_isError(res)) {
        ZSTD_freeCStream(st->ctx.zstdCCtx);
        serverLog(LL_NOTICE, "Failed to set compression level for ZSTD compression context");
        return -1;
    }

    /* temp buf storing compressed data */
    size_t outSize = ZSTD_CStreamOutSize();
    st->output.data = zmalloc(outSize);
    st->output.size = outSize;
    st->output.written = 0;
    st->output.consumed = 0;

    /* temp buf storing uncompressed data */
    size_t inSize = ZSTD_CStreamInSize();
    st->input.data = zmalloc(inSize);
    st->input.size = inSize;
    st->input.written = 0;
    st->input.consumed = 0;

    st->write_flush_pending = 0;

    return 0;
}

static int zstdInitDecompress(compressionState *st) {
    st->ctx.zstdDCtx = ZSTD_createDStream();
    if (!st->ctx.zstdDCtx) {
        serverLog(LL_NOTICE, "Failed to create ZSTD decompression context");
        return -1;
    }

    /* temp buf storing compressed data */
    size_t inSize = ZSTD_DStreamInSize();
    st->input.data = zmalloc(inSize);
    st->input.size = inSize;
    st->input.written = 0;
    st->input.consumed = 0;

    /* temp buf storing decompressed data */
    size_t outSize = ZSTD_DStreamOutSize();
    st->output.data = zmalloc(outSize);
    st->output.size = outSize;
    st->output.written = 0;
    st->output.consumed = 0;

    st->read_flush_pending = 0;
    st->drain_only = 0;

    return 0;
}

static int zstdCompress(compressionState *st, int flush) {
    ZSTD_inBuffer input = {
        .src  = st->input.data,
        .size = st->input.written,
        .pos  = st->input.consumed
    };
    ZSTD_outBuffer output = {
        .dst  = st->output.data,
        .size = st->output.size,
        .pos  = st->output.written
    };

    ZSTD_EndDirective directive;
    /* We use ZSTD_e_end instead of ZSTD_e_flush when we want to flush zstd's.
     * This flushes zstd's internal buffers but also ends the current frame.
     * This of course lowers the compression ratio but massively increases speed
     * on the decompression side also, as it doesn't need to wait for more data.
     * The resulting compression ratio is still very good (tested with default
     * compression level). */
    if (flush || st->write_flush_pending)
        directive = ZSTD_e_end;
    else
        directive = ZSTD_e_continue;

    size_t ret;
    do {
        ret = ZSTD_compressStream2(st->ctx.zstdCCtx, &output, &input, directive);
        if (ZSTD_isError(ret)) {
            serverLog(LL_WARNING, "zstd compress error: %s", ZSTD_getErrorName(ret));
            return -1;
        }
    } while (ret > 0 && output.pos < output.size);

    /* If we pass a directive different than ZSTD_e_continue to zstd we want to
     * keep using that directive until compressStream2 returns 0. By keeping
     * this flag raised we know we are in the process of flushing data, i.e we
     * cannot use ZSTD_e_continue before we have flushed it all. */
    st->write_flush_pending = (directive == ZSTD_e_end && ret > 0);

    st->input.consumed = input.pos;
    st->output.written = output.pos;

    return 0;
}

static int zstdDecompress(compressionState *st) {
    ZSTD_inBuffer input = {
        .src  = st->input.data,
        .size = st->input.written,
        .pos  = st->input.consumed
    };
    ZSTD_outBuffer output = {
        .dst  = st->output.data,
        .size = st->output.size,
        .pos  = st->output.written
    };

    size_t ret = ZSTD_decompressStream(st->ctx.zstdDCtx, &output, &input);
    if (ZSTD_isError(ret)) {
        serverLog(LL_NOTICE, "zstd decompress error: %s", ZSTD_getErrorName(ret));
        return -1;
    }

    /* Don't try to flush again if we already tried, no more progress can be
     * made without additional input. */
    st->read_flush_pending = !st->read_flush_pending && (ret > 0);

    st->input.consumed = input.pos;
    st->output.written = output.pos;

    return 0;
}

static void zstdEnd(compressionState *st) {
    if (st->dir == COMPRESS && st->ctx.zstdCCtx) {
        /* Flush any pending data so context is in a good state before closing it */
        if (st->write_flush_pending) {
            size_t sz = ZSTD_CStreamOutSize();
            char *tmp = zmalloc(sz);
            ZSTD_inBuffer input = {
                .src  = NULL,
                .size = 0,
                .pos  = 0
            };
            ZSTD_outBuffer output = {
                .dst  = tmp,
                .size = sz,
                .pos  = 0
            };
            while (ZSTD_compressStream2(st->ctx.zstdCCtx, &output, &input, ZSTD_e_end) > 0) {
                /* Just ignore the output, we are closing the compression state
                 * anyways */
                output.pos = 0;
            }
            zfree(tmp);
        }
        ZSTD_freeCStream(st->ctx.zstdCCtx);
        st->ctx.zstdCCtx = NULL;
    } else if (st->dir == DECOMPRESS && st->ctx.zstdDCtx) {
        ZSTD_freeDStream(st->ctx.zstdDCtx);
        st->ctx.zstdDCtx = NULL;
    }
}

static const compressionType zstdType = {
    .init_compress = zstdInitCompress,
    .init_decompress = zstdInitDecompress,
    .compress = zstdCompress,
    .decompress = zstdDecompress,
    .end = zstdEnd,
};

int decompressInto(compressionState *state, char *buf, size_t buflen);

/* clientCodecWrite - Write `len` bytes from `data` through the codec.
 * If compression is active (COMPRESS direction + flag), feeds data into the
 * ZSTD compressor and flushes compressed bytes to c->conn.  *sock receives
 * the physical bytes written to the socket; the return value is the number of
 * logical (uncompressed) bytes consumed from `data` — what the caller should
 * use to advance replication offsets.
 * When compression is inactive the call degenerates to a plain connWrite and
 * return value == *sock. */
ssize_t clientCodecWrite(client *c, const void *data, size_t len, size_t *sock) {
    *sock = 0;
    compressionState *st = c->compression_state;
    if (!st || st->dir != COMPRESS || !(c->io_flags & CLIENT_IO_COMPRESSION_ENABLED)) {
        ssize_t n = connWrite(c->conn, data, len);
        if (n > 0) *sock = (size_t)n;
        return n;
    }

    ssize_t consumed = 0;
    while ((size_t)consumed < len) {
        int to_consume = min(st->input.size - st->input.written,
                             (int)(len - consumed));
        serverAssert(to_consume >= 0);

        memcpy(st->input.data + st->input.written,
               (const char *)data + consumed, to_consume);
        st->input.written += to_consume;
        consumed += to_consume;

        int written = 0;
        if (compressAndWrite(c, &written)) {
            /* compressAndWrite returns non-zero on socket write error */
            if (connGetState(c->conn) != CONN_STATE_CONNECTED) return -1;
            break;
        }
        *sock += (size_t)written;

        /* No progress: compressed output buffer is full and socket is stuck */
        if (written == 0 && st->output.written == st->output.consumed) break;
    }
    return consumed;
}

/* clientCodecRead - Read up to `len` bytes into `buf` through the codec.
 * If decompression is active (DECOMPRESS direction + flag), reads compressed
 * data from c->conn into the codec's input buffer, decompresses it, and
 * returns the number of logical (decompressed) bytes placed in `buf`.
 * *sock receives the physical bytes read from the socket.
 * When st->drain_only is set the connRead step is skipped — only bytes
 * already buffered inside the decompressor are drained into `buf`.
 * When compression is inactive the call degenerates to a plain connRead and
 * return value == *sock. */
ssize_t clientCodecRead(client *c, void *buf, size_t len, size_t *sock) {
    *sock = 0;
    compressionState *st = c->compression_state;
    if (!st || st->dir != DECOMPRESS || !(c->io_flags & CLIENT_IO_COMPRESSION_ENABLED)) {
        ssize_t n = connRead(c->conn, buf, len);
        if (n > 0) *sock = (size_t)n;
        return n;
    }

    size_t decompressed = 0;
    do {
        int curr = decompressInto(st, (char *)buf + decompressed, len - decompressed);
        if (curr < 0) return -1; /* decompression error */
        decompressed += curr;

        int nread = 0;
        if (!st->drain_only) {
            nread = connRead(c->conn,
                             st->input.data + st->input.written,
                             st->input.size - st->input.written);
            if (nread < 0 && connGetState(c->conn) == CONN_STATE_ERROR) return -1;
            if (nread > 0) {
                *sock += (size_t)nread;
                st->input.written += nread;
            }
        }

        if (curr <= 0 && nread <= 0) break;
    } while (decompressed < len);

    /* Return -1 (EAGAIN equivalent) when nothing was decompressed */
    if (decompressed == 0 && connGetState(c->conn) == CONN_STATE_CONNECTED)
        return -1;

    return (ssize_t)decompressed;
}

/* clientCodecDrainPending - Re-invoke the client's read handler with drain_only
 * set so bytes still buffered inside the decompressor are pushed into the
 * query buffer without issuing a new read(2) call.
 * Called from IOThreadBeforeSleep for master-clients whose codec still holds
 * undelivered decompressed bytes after the last socket read. */
void clientCodecDrainPending(client *c) {
    compressionState *st = c->compression_state;
    if (!st || st->dir != DECOMPRESS) return;
    if (!clientHasPendingCompressedData(c)) return;

    connection *conn = c->conn;
    if (conn->read_handler) {
        st->drain_only = 1;
        conn->read_handler(conn);
        st->drain_only = 0;
    }
}

/* Create compression state for the client */
int compressionStateCreate(client *c) {
    compressionState *st = zcalloc(sizeof(compressionState));
    st->type = &zstdType;
    st->last_write = 0;
    st->write_flush_pending = 0;
    st->read_flush_pending = 0;
    st->dir = CD_INVALID;

    c->compression_state = st;

    return 1;
}

void compressionStateDestroy(compressionState *state) {
    if (state == NULL) return;

    state->type->end(state);
    zfree(state->input.data);
    zfree(state->output.data);
    zfree(state);
}

/* Create and initialize a compression state for the client. No-op if already
 * initialized. `dir` indicates the compression direction, i.e if the client
 * will compress or decompress data.
 * Currently only viable for master/replica clients. */
int clientCreateCompressionState(client *c, compressionDirection dir) {
    /* Client compression already initialized */
    if (c->compression_state != NULL)
        return 1;

    serverAssert(compressionStateCreate(c));

    compressionState *st = c->compression_state;

    if (dir == COMPRESS) {
        serverAssert(c->compression_level > 0 && c->flags & CLIENT_SLAVE);

        if (st->type->init_compress(st, c->compression_level) == -1) {
            compressionStateDestroy(c->compression_state);
            c->compression_state = NULL;
            return 0;
        }

        st->dir = COMPRESS;

        serverLog(LL_NOTICE, "Initialized compression at level %d for client #%llu...",
                c->compression_level, (unsigned long long)c->id);
    } else if (dir == DECOMPRESS) {
        serverAssert(server.repl_master_compression_level > 0);

        if (st->type->init_decompress(st) == -1) {
            compressionStateDestroy(c->compression_state);
            c->compression_state = NULL;
            return 0;
        }
 
        st->dir = DECOMPRESS;

        serverLog(LL_NOTICE, "Decompression for master client initialized.");
    } else {
        /* Inaccessible */
        serverAssert(0);
    }

    return 1;
}

void clientDestroyCompressionState(client *c) {
    if (c->compression_state == NULL) return;

    compressionStateDestroy(c->compression_state);
    c->compression_state = NULL;
    c->compression_level = 0;
    c->io_flags &= ~CLIENT_IO_COMPRESSION_ENABLED;

    serverLog(LL_NOTICE, "Compression state for client #%llu%s destroyed...",
              (unsigned long long)c->id,
              c->flags & CLIENT_MASTER ? " (master)" : c->flags & CLIENT_SLAVE ?
              " (slave)" : "");
}

/* Enable client compression and create compression state if not present.
 * Currently only valid for primary/replica clients.
 * Return 0 if compression state was not created and failed to be initialized,
 * 1 if compression was enabled. */
int clientEnableCompression(client *c, compressionDirection dir) {
    serverAssert((c->flags & CLIENT_MASTER) || (c->flags & CLIENT_SLAVE));
    if (!clientCreateCompressionState(c, dir)) {
        return 0;
    }

    c->io_flags |= CLIENT_IO_COMPRESSION_ENABLED;
    return 1;
}

/* Disable compression without destroying compression state. */
void clientDisableCompression(client *c) {
    c->io_flags &= ~CLIENT_IO_COMPRESSION_ENABLED;
}

/* Compress any data staged in the codec's input buffer and write the
 * resulting compressed bytes directly to c->conn.
 * The compression library may not emit a compressed frame immediately; this
 * call may write nothing to the socket in that case.
 * Force-flushes the compressed buffer when compression_max_latency ms have
 * elapsed since the last flush.
 * Returns 0 on success, non-zero on socket write error. *tot_written is set
 * to the number of compressed bytes sent to the socket. */
int compressAndWrite(client *c, int *tot_written) {
    if (c->compression_level <= 0)
        return 0;

    compressionState *state = c->compression_state;
    serverAssert(state);

    /* All available uncompressed data was consumed so we need to reset the
     * uncompressed buffer */
    if (state->input.written == state->input.size &&
        state->input.consumed == state->input.size)
    {
        state->input.written = 0;
        state->input.consumed = 0;
    }

    if (state->output.written < state->output.size) {
        /* Force flush after `compression_max_latency` ms have passed.
         * Note, this only makes sense when we have enough space for compressing
         * data. */
        int flush = mstime() - state->last_write > server.compression_max_latency;
        if (state->type->compress(state, flush) == -1) {
            clientDestroyCompressionState(c);
            return 1;
        }
    }

    /* Try to write all the data available in the compressed buffer. */
    *tot_written = 0;
    int towrite =
        state->output.written - state->output.consumed;
    do {
        int written = connWrite(
            c->conn, state->output.data + state->output.consumed, towrite);
        if (written < 0) {
            return 1;
        }

        state->output.consumed += written;
        *tot_written += written;
        towrite -= written;

        /* All of the compressed data was send to the socket so we need to reset
         * the compression buffer. */
        if (state->output.consumed == state->output.size) {
            serverAssert(towrite == 0 && state->output.written == state->output.size);

            state->output.written = 0;
            state->output.consumed = 0;
        }
    } while (towrite > 0);

    if (*tot_written > 0)
        state->last_write = mstime();

    return 0;
}

/* Decompress input compressed data and put it in `buf`. If decompressed data
 * is more than buflen this function must be called again so output data can
 * be consumed. If buflen is sufficiently large this function will decompress
 * as much data as possible. */
int decompressInto(compressionState *state, char *buf, size_t buflen) {
    if (buflen == 0)
      return 0;

    int consumed = 0;

    /* Decompress as much data as possible */
    while ((size_t)consumed < buflen &&
           (state->read_flush_pending ||
            state->input.written > state->input.consumed ||
            state->output.written > state->output.consumed))
    {
        /* Reset the decompressed buffer if all the available data is consumed */
        if (state->output.consumed == state->output.size) {
            state->output.written = 0;
            state->output.consumed = 0;
        }

        if ((state->read_flush_pending && state->output.size > state->output.written) ||
             state->input.written > state->input.consumed)
        {
            if (state->type->decompress(state) == -1) {
                return -1;
            }
        }

        /* Copy the decompressed data to the output buffer */
        if (state->output.written > state->output.consumed) {
            size_t nonconsumed_decompressed = state->output.written - state->output.consumed;
            int to_consume = min(buflen - consumed, nonconsumed_decompressed);
            memcpy(buf + consumed, state->output.data + state->output.consumed, to_consume);

            state->output.consumed += to_consume;
            consumed += to_consume;
        }
    }

    if (state->output.consumed == state->output.size)
    {
        state->output.written = 0;
        state->output.consumed = 0;
    }

    /* Reset the compressed buffer if we decompressed all the available data */
    if (state->input.consumed == state->input.size) {
        state->input.written = 0;
        state->input.consumed = 0;
    }

    serverAssert((size_t)consumed <= buflen);
    return consumed;
}

/* Read data from input_buf and decompress it immediately. The result is written
 * into output_buf.
 * Return number of bytes decompressed. *consumed stores number of bytes consumed
 * from input_buf.
 * Note, that we may have enough compressed data inside input buf so that decompressing
 * it will exceed output_len. The function must be ran in a loop until input_buf
 * is fully consumed - so make sure to have free space in output_buf on each call. */
int readFromBufAndDecompress(client *c, char *input_buf, size_t input_len,
                             char *output_buf, size_t output_len, size_t *consumed)
{
    compressionState *state = c->compression_state;
    if (!state) {
        return -1;
    }

    int tot_decompressed = 0;
    *consumed = 0;
    while (*consumed <= input_len && (size_t)tot_decompressed < output_len) {
        int to_consume =
            min(state->input.size - state->input.written,
                (int)(input_len - *consumed));

        if (to_consume)
            memcpy(state->input.data + state->input.written,
                   input_buf + *consumed, to_consume);

        *consumed += to_consume;

        state->input.written += to_consume;

        int decompressed = decompressInto(state, output_buf + tot_decompressed,
                                          output_len - tot_decompressed);
        if (decompressed <= 0)
            break;
        tot_decompressed += decompressed;
    }

    return tot_decompressed;
}

/* Check if we need to flush compressed data. Compression library may wait for
 * a lot of compressed data before it finishes a frame and gives it back to user.
 * While this gives the best compression ratio it introduces a lot of latency so
 * we make a compromise and flush periodically. */
int clientHasPendingCompressionFlush(client *c) {
    compressionState *state = c->compression_state;
    if (!state) return 0;
    if (state->dir != COMPRESS) return 0;

    return state->write_flush_pending || (mstime() - state->last_write >= server.compression_max_latency);
}

/* Check if we still have pending compressed data. This may mean that either the
 * compression library still has data in it's internal buffers, we still have
 * compressed data that needs to be consumed by the library or we have stored
 * decompressed data that we still have not consumed. */
int clientHasPendingCompressedData(client *c) {
    compressionState *state = c->compression_state;
    if (!state) return 0;
    if (state->dir != DECOMPRESS) return 0;

    return (state->read_flush_pending && state->output.size > state->output.written) ||
           state->input.written > state->input.consumed ||
           state->output.written > state->output.consumed;
}

