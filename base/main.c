#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include <string.h>
#include "constants.h"
#include "parser.h"
#include "operations.h"
#include <dirent.h>
#include "files.h"

int main(int argc, char *argv[])
{
  DIR *dir;
  // tem que ter 4 args
  if (argc != 4)
  {
    fprintf(stderr, "Usage: %s <directory>\n", argv[0]);
    return -1;
  }

  if (kvs_init())
  {
    perror("Failed to initialize KVS\n");
    return 1;
  }
  dir = opendir(argv[1]);
  if (!dir)
  {
    perror("Failed to open directory");
    kvs_terminate();
    return 1;
  }

  int max_threads = atoi(argv[3]);
  int lim_backups = atoi(argv[2]);

  pthread_mutex_t trinco;
  pthread_mutex_init(&trinco, NULL);
  pthread_t tid[max_threads];
  thread_args_t args = {
        .dir = dir,
        .lim_backups = lim_backups,
        .max_threads = max_threads,
        .trinco = trinco
  };
  strncpy(args.directory_path, argv[1], MAX_JOB_FILE_NAME_SIZE);
  for (int i = 0; i < max_threads; i++) {
    pthread_create(&tid[i], NULL, process_files, (void *)&args);
  }

  for (int i = 0; i < max_threads; i++) {
    pthread_join(tid[i], NULL);
  }

  closedir(dir);
  kvs_terminate();
  return 0;
}
