#ifndef WORKER_H
#define WORKER_H

#include "common.h"

typedef struct {
    int       thread_id;
    int       proc_id;
    Task*     tasks;
    int       num_tasks;
    char*     outdir;
    int       mode;
    sem_t*    log_sem;
    int*      ok_count;
    int*      err_count;
    pthread_mutex_t* lock;
} ThreadArgs;

void* thread_worker(void* arg);
void  worker_process(int proc_id, int num_threads, int pipe_fd,
                     int msg_queue_id, sem_t* task_sem,
                     int shm_id, char* outdir, int mode);
#endif
