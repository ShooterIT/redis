/*
 * Copyright (c) 2009-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#ifndef __CLIENT_COMP_H
#define __CLIENT_COMP_H

#include <sys/types.h>

/* Opaque handle to client's compression state used internally by client_comp */
typedef struct compressionState compressionState;

struct client;

typedef enum {
    CD_INVALID,
    COMPRESS,
    DECOMPRESS,
} compressionDirection;

int clientCreateCompressionState(struct client *c, compressionDirection dir);
void clientDestroyCompressionState(struct client *c);

int clientEnableCompression(struct client *c, compressionDirection dir);
void clientDisableCompression(struct client *c);

/* Codec I/O helpers used by the replication write and read choke-points.
 * Return value: logical (uncompressed/decompressed) bytes consumed/produced.
 * *sock: physical bytes pushed to / pulled from the socket. */
ssize_t clientCodecWrite(struct client *c, const void *data, size_t len, size_t *sock);
ssize_t clientCodecRead(struct client *c, void *buf, size_t len, size_t *sock);

/* Re-drain bytes still buffered inside the decompressor without a socket read.
 * Called from IOThreadBeforeSleep for master-clients with leftover codec data. */
void clientCodecDrainPending(struct client *c);

/* Lower-level helpers (used by IOThreadBeforeSleep flush and replication.c). */
int compressAndWrite(struct client *c, int *tot_written);
int readFromBufAndDecompress(struct client *c, char *input_buf, size_t input_len,
                             char *output_buf, size_t output_len,
                             size_t *consumed);

int clientHasPendingCompressionFlush(struct client *c);
int clientHasPendingCompressedData(struct client *c);

#endif /* __CLIENT_COMP_H */
