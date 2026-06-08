#include "../include/common.h"
#include "../include/ipc.h"
#include "../include/worker.h"
#include <dirent.h>
#include <time.h>
#include <sys/stat.h>
#include <fcntl.h>

static void generate_test_files(const char* folder, int n, int size_kb) {
    struct stat st;
    if (stat(folder, &st) != 0) mkdir(folder, 0755);
    DIR* d = opendir(folder);
    int count = 0;
    struct dirent* entry;
    while ((entry = readdir(d)) != NULL)
        if (strstr(entry->d_name, ".txt")) count++;
    closedir(d);
    if (count >= n) {
        printf("[INFO] %d fichiers test deja presents\n", count);
        return;
    }
    printf("[INFO] Generation de %d fichiers (%d KB)...\n", n, size_kb);
    for (int i = 0; i < n; i++) {
        char path[MAX_PATH];
        snprintf(path, MAX_PATH, "%s/file_%03d.txt", folder, i);
        FILE* f = fopen(path, "w");
        if (!f) continue;
        for (int j = 0; j < size_kb * 10; j++)
            fprintf(f, "Fichier %d ligne %d - test compression parallele\n", i, j);
        fclose(f);
    }
    printf("[INFO] Fichiers generes.\n");
}

static int collect_files(const char* folder, int mode,
                          char files[][MAX_PATH], int max) {
    DIR* d = opendir(folder);
    if (!d) { perror("opendir"); return 0; }
    int n = 0;
    struct dirent* entry;
    while ((entry = readdir(d)) != NULL && n < max) {
        if (entry->d_name[0] == '.') continue;
        int is_gz = strstr(entry->d_name, ".gz") != NULL;
        if (mode == 0 && !is_gz)
            snprintf(files[n++], MAX_PATH, "%s/%s", folder, entry->d_name);
        else if (mode == 1 && is_gz)
            snprintf(files[n++], MAX_PATH, "%s/%s", folder, entry->d_name);
    }
    closedir(d);
    return n;
}

static double dispatch(const char* input, const char* output,
                        int mode, int num_procs, int num_threads) {
    struct stat st;
    if (stat(output, &st) != 0) mkdir(output, 0755);

    char files[MAX_FILES][MAX_PATH];
    int  nfiles = collect_files(input, mode, files, MAX_FILES);
    if (nfiles == 0) {
        printf("[Dispatcher] Aucun fichier a traiter.\n");
        return 0.0;
    }
    printf("[Dispatcher] %d fichiers | %d processus x %d threads\n",
           nfiles, num_procs, num_threads);

    int      mqid    = create_msg_queue();
    int      shm_id  = create_shm();

    sem_unlink("/task_sem");
    sem_t* task_sem = sem_open("/task_sem", O_CREAT, 0666, 0);

    int pipes[num_procs][2];
    for (int i = 0; i < num_procs; i++)
        pipe(pipes[i]);

    struct timespec t_start, t_end;
    clock_gettime(CLOCK_MONOTONIC, &t_start);

    for (int i = 0; i < num_procs; i++) {
        pid_t pid = fork();
        if (pid == 0) {
            close(pipes[i][0]);
            worker_process(i, num_threads, pipes[i][1],
                           mqid, task_sem, shm_id,
                           (char*)output, mode);
            exit(0);
        }
        close(pipes[i][1]);
    }

    for (int i = 0; i < nfiles; i++) {
        Task t;
        t.mtype = 1;
        strncpy(t.filepath, files[i], MAX_PATH);
        strncpy(t.outdir,   output,   MAX_PATH);
        t.mode = mode;
        send_task(mqid, &t);
        sem_post(task_sem);
    }

    for (int i = 0; i < num_procs; i++) {
        Task stop;
        stop.mtype = 1;
        strncpy(stop.filepath, "STOP", MAX_PATH);
        send_task(mqid, &stop);
        sem_post(task_sem);
    }

    int total_ok = 0, total_err = 0;
    for (int i = 0; i < num_procs; i++) {
        Result res;
        read(pipes[i][0], &res, sizeof(Result));
        close(pipes[i][0]);
        total_ok  += res.ok;
        total_err += res.errors;
    }

    for (int i = 0; i < num_procs; i++)
        wait(NULL);

    clock_gettime(CLOCK_MONOTONIC, &t_end);
    double elapsed = (t_end.tv_sec  - t_start.tv_sec) +
                     (t_end.tv_nsec - t_start.tv_nsec) / 1e9;

    SharedData* shm = attach_shm(shm_id);
    printf("[Dispatcher] OK=%d ERR=%d SharedCounter=%d Temps=%.2fs\n",
           total_ok, total_err, shm->total_processed, elapsed);
    detach_shm(shm);

    sem_close(task_sem);
    sem_unlink("/task_sem");
    destroy_msg_queue(mqid);
    destroy_shm(shm_id);

    return elapsed;
}

static void run_benchmark(const char* input, const char* output, int mode) {
    int configs[][2] = {{1,1},{1,2},{2,1},{2,2},{4,1},{4,2},{4,4}};
    int n = sizeof(configs) / sizeof(configs[0]);
    double baseline = 0.0;

    printf("\n========================================\n");
    printf("  BENCHMARK — Parallel File Compressor\n");
    printf("========================================\n\n");

    for (int i = 0; i < n; i++) {
        int p = configs[i][0], t = configs[i][1];
        char tmp_out[MAX_PATH];
        snprintf(tmp_out, MAX_PATH, "%s/bench_P%dT%d", output, p, t);
        double elapsed = dispatch(input, tmp_out, mode, p, t);
        if (i == 0) baseline = elapsed;
        double speedup = (baseline > 0) ? baseline / elapsed : 1.0;
        printf("  P%-2d x T%-2d -> %.2fs  speedup: %.2fx\n",
               p, t, elapsed, speedup);
    }
    printf("\n========================================\n");
}

int main(int argc, char* argv[]) {
    int  num_procs   = 2;
    int  num_threads = 2;
    int  mode        = 0;
    int  benchmark   = 0;
    char input[MAX_PATH]  = "./test_files";
    char output[MAX_PATH] = "./output_files";

    for (int i = 1; i < argc; i++) {
        if      (strcmp(argv[i], "--processes")  == 0) num_procs   = atoi(argv[++i]);
        else if (strcmp(argv[i], "--threads")    == 0) num_threads = atoi(argv[++i]);
        else if (strcmp(argv[i], "--mode")       == 0)
            mode = strcmp(argv[++i], "decompress") == 0 ? 1 : 0;
        else if (strcmp(argv[i], "--input")      == 0) strncpy(input,  argv[++i], MAX_PATH);
        else if (strcmp(argv[i], "--output")     == 0) strncpy(output, argv[++i], MAX_PATH);
        else if (strcmp(argv[i], "--benchmark")  == 0) benchmark = 1;
    }

    generate_test_files(input, 20, 2000);

    if (benchmark)
        run_benchmark(input, output, mode);
    else {
        double t = dispatch(input, output, mode, num_procs, num_threads);
        printf("\n  Mode=%s  P=%d  T=%d  Temps=%.2fs\n\n",
               mode == 0 ? "compress" : "decompress",
               num_procs, num_threads, t);
    }
    return 0;
}
