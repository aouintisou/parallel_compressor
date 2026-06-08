#ifndef IPC_H
#define IPC_H

#include "common.h"
#include <sys/ipc.h>
#include <sys/shm.h>
#include <sys/msg.h>

int         create_msg_queue(void);
void        send_task(int mqid, Task* task);
int         recv_task(int mqid, Task* task);
void        destroy_msg_queue(int mqid);

int         create_shm(void);
SharedData* attach_shm(int shm_id);
void        detach_shm(SharedData* ptr);
void        destroy_shm(int shm_id);

void        write_log(const char* outdir, int proc_id, int thread_id,
                      const char* fname, const char* status, sem_t* sem);
#endif
