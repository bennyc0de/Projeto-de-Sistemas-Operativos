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

#endif  // FILES_H