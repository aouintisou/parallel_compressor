#ifndef COMMON_H
#define COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <pthread.h>
#include <semaphore.h>
#include <zlib.h>

#define MAX_PATH     512
#define MAX_FILES    256
#define CHUNK_SIZE   16384

typedef struct {
    long mtype;
    char filepath[MAX_PATH];
    char outdir[MAX_PATH];
    int  mode;
} Task;

typedef struct {
    int ok;
    int errors;
} Result;

typedef struct {
    int total_processed;
} SharedData;

#endif
