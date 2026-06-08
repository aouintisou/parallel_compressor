#include "../include/worker.h"
#include "../include/ipc.h"
#include <fcntl.h>
#include <sys/stat.h>

static int compress_file(const char* src, const char* dst) {
    FILE* in = fopen(src, "rb");
    if (!in) return -1;
    gzFile out = gzopen(dst, "wb");
    if (!out) { fclose(in); return -1; }
    unsigned char buf[CHUNK_SIZE];
    size_t n;
    while ((n = fread(buf, 1, CHUNK_SIZE, in)) > 0)
        gzwrite(out, buf, n);
    fclose(in);
    gzclose(out);
    return 0;
}

static int decompress_file(const char* src, const char* dst) {
    gzFile in = gzopen(src, "rb");
    if (!in) return -1;
    FILE* out = fopen(dst, "wb");
    if (!out) { gzclose(in); return -1; }
    unsigned char buf[CHUNK_SIZE];
    int n;
    while ((n = gzread(in, buf, CHUNK_SIZE)) > 0)
        fwrite(buf, 1, n, out);
    gzclose(in);
    fclose(out);
    return 0;
}

static void build_output_path(const char* fpath, const char* outdir,
                               int mode, char* out, size_t sz) {
    const char* fname = strrchr(fpath, '/');
    fname = fname ? fname + 1 : fpath;
    if (mode == 0) {
        snprintf(out, sz, "%s/%s.gz", outdir, fname);
    } else {
        char tmp[MAX_PATH];
        strncpy(tmp, fname, MAX_PATH);
        size_t len = strlen(tmp);
        if (len > 3 && strcmp(tmp + len - 3, ".gz") == 0)
            tmp[len - 3] = '\0';
        snprintf(out, sz, "%s/%s", outdir, tmp);
    }
}

void* thread_worker(void* arg) {
    ThreadArgs* a = (ThreadArgs*)arg;
    for (int i = 0; i < a->num_tasks; i++) {
        char out_path[MAX_PATH];
        build_output_path(a->tasks[i].filepath, a->outdir,
                          a->mode, out_path, MAX_PATH);
        int ret = (a->mode == 0)
                  ? compress_file(a->tasks[i].filepath, out_path)
                  : decompress_file(a->tasks[i].filepath, out_path);
        const char* fname = strrchr(a->tasks[i].filepath, '/');
        fname = fname ? fname + 1 : a->tasks[i].filepath;
        pthread_mutex_lock(a->lock);
        if (ret == 0) a->ok_count[0]++;
        else          a->err_count[0]++;
        pthread_mutex_unlock(a->lock);
        write_log(a->outdir, a->proc_id, a->thread_id,
                  fname, ret == 0 ? "OK" : "ERROR", a->log_sem);
    }
    return NULL;
}

void worker_process(int proc_id, int num_threads, int pipe_fd,
                    int msg_queue_id, sem_t* task_sem,
                    int shm_id, char* outdir, int mode) {
    Task  local_tasks[MAX_FILES];
    int   num_tasks = 0;
    int   ok_count  = 0;
    int   err_count = 0;
    pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
    sem_t log_sem;
    sem_init(&log_sem, 0, 1);

    while (1) {
        sem_wait(task_sem);
        Task t;
        if (recv_task(msg_queue_id, &t) < 0) break;
        if (strcmp(t.filepath, "STOP") == 0)  break;
        local_tasks[num_tasks++] = t;
        while (num_tasks < MAX_FILES) {
            if (sem_trywait(task_sem) != 0) break;
            if (recv_task(msg_queue_id, &t) < 0) break;
            if (strcmp(t.filepath, "STOP") == 0) goto done;
            local_tasks[num_tasks++] = t;
        }
        break;
    }
    done:;

    if (num_tasks == 0) {
        Result res = {0, 0};
        write(pipe_fd, &res, sizeof(Result));
        close(pipe_fd);
        return;
    }

    printf("[Worker-%d] %d fichiers | %d threads\n",
           proc_id, num_tasks, num_threads);

    pthread_t  threads[64];
    ThreadArgs args[64];
    int per_thread = num_tasks / num_threads;
    int remainder  = num_tasks % num_threads;
    int start      = 0;

    for (int i = 0; i < num_threads; i++) {
        int count = per_thread + (i < remainder ? 1 : 0);
        if (count == 0) break;
        args[i].thread_id = i;
        args[i].proc_id   = proc_id;
        args[i].tasks     = &local_tasks[start];
        args[i].num_tasks = count;
        args[i].outdir    = outdir;
        args[i].mode      = mode;
        args[i].log_sem   = &log_sem;
        args[i].ok_count  = &ok_count;
        args[i].err_count = &err_count;
        args[i].lock      = &lock;
        pthread_create(&threads[i], NULL, thread_worker, &args[i]);
        start += count;
    }

    int actual = (num_threads < num_tasks) ? num_threads : num_tasks;
    for (int i = 0; i < actual; i++)
        pthread_join(threads[i], NULL);

    sem_destroy(&log_sem);
    pthread_mutex_destroy(&lock);

    SharedData* shm = attach_shm(shm_id);
    shm->total_processed += ok_count;
    detach_shm(shm);

    Result res = { ok_count, err_count };
    write(pipe_fd, &res, sizeof(Result));
    close(pipe_fd);
    printf("[Worker-%d] done — ok=%d err=%d\n", proc_id, ok_count, err_count);
}
