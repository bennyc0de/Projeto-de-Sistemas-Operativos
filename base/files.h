#ifndef FILES_H
#define FILES_H
#include "constants.h"

struct files {
    int fd_in;
    int fd_out;
    char input_path[MAX_JOB_FILE_NAME_SIZE];
    char output_path[MAX_JOB_FILE_NAME_SIZE];
    int num_backups;
    char backup_path[MAX_JOB_FILE_NAME_SIZE];
};

typedef struct {
    DIR *dir;
    char directory_path[MAX_JOB_FILE_NAME_SIZE];
    int lim_backups;
    int max_threads;
    pthread_mutex_t trinco;
} thread_args_t;

#endif  // FILES_H