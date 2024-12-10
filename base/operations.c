#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <dirent.h>
#include <fcntl.h>
#include <pthread.h>
#include "kvs.h"
#include "constants.h"
#include "files.h"
#include "parser.h"
#include <unistd.h>
#include <sys/wait.h>


static struct HashTable *kvs_table = NULL;

/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms)
{
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

int kvs_init()
{
  if (kvs_table != NULL)
  {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate()
{
  if (kvs_table == NULL)
  {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE])
{
  if (kvs_table == NULL)
  {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  for (size_t i = 0; i < num_pairs; i++)
  {
    if (write_pair(kvs_table, keys[i], values[i]) != 0)
    {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }
  }

  return 0;
}

int kvs_read(int fd_out, size_t num_pairs, char keys[][MAX_STRING_SIZE])
{
  size_t len_key, len_res;
  if (kvs_table == NULL)
  {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  write(fd_out, "[", 1);
  for (size_t i = 0; i < num_pairs; i++)
  {
    char *result = read_pair(kvs_table, keys[i]);
    len_key = strlen(keys[i]);
    if (result == NULL)
    {
      write(fd_out, "(", 1);
      write(fd_out, keys[i], len_key);
      write(fd_out, ",KVSERROR)", 10);
    }
    else
    {
      len_res = strlen(result);
      write(fd_out, "(", 1);
      write(fd_out, keys[i], len_key);
      write(fd_out, ",", 1);
      write(fd_out, result, len_res);
      write(fd_out, ")", 1);
    }
    free(result);
  }
  write(fd_out, "]\n", 2);
  return 0;
}

int kvs_delete(int fd_out, size_t num_pairs, char keys[][MAX_STRING_SIZE])
{
  size_t len_key;
  if (kvs_table == NULL)
  {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  int aux = 0;

  for (size_t i = 0; i < num_pairs; i++)
  {
    if (delete_pair(kvs_table, keys[i]) != 0)
    {
      if (!aux)
      {
        write(fd_out, "[", 1);
        aux = 1;
      }
      len_key = strlen(keys[i]);
      write(fd_out, "(", 1);
      write(fd_out, keys[i], len_key);
      write(fd_out, ",KVSMISSING)", 11);
    }
  }
  if (aux)
  {
    write(fd_out, "]\n", 2);
  }

  return 0;
}

void kvs_show(int fd_out)
{
  size_t len_key, len_value;
  for (int i = 0; i < TABLE_SIZE; i++)
  {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL)
    {
      len_key = strlen(keyNode->key);
      len_value = strlen(keyNode->value);
      write(fd_out, "(", 1);
      write(fd_out, keyNode->key, len_key);
      write(fd_out, ",", 1);
      write(fd_out, keyNode->value, len_value);
      write(fd_out, ")\n", 2);
      keyNode = keyNode->next; // Move to the next node
    }
  }
}

int kvs_backup(struct files f, int lim_backups)
{
  size_t len = strlen(f.input_path);
  strcpy(f.backup_path, f.input_path);
  snprintf(f.backup_path, MAX_JOB_FILE_NAME_SIZE, "%.*s-%d.bck", (int)(len - 4), 
          f.input_path, f.num_backups);

  if (lim_backups == 0) {
    int status;
    pid_t pid = wait(&status);
    //pode ser cagativo
    if (pid == -1) {
      perror("wait deu merda");
      return 1;} 
    else {
      if (WIFEXITED(status)) {
        if (WEXITSTATUS(status) != 0) {
          fprintf(stderr, "Child process exited with status %d\n", WEXITSTATUS(status));}
    } else {
        fprintf(stderr, "Child process did not exit successfully\n");
      }
    }
  }
  pid_t pid = fork();
    if (pid < 0) {
        perror("Failed to fork");
        return 1;
    } else if (pid == 0) {
        int fd_backup = open(f.backup_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd_backup < 0) {
            perror("Failed to open backup file");
            _exit(1);
        }

        kvs_show(fd_backup);
        close(fd_backup);
        _exit(0);
    }
  return 0;
}

void kvs_wait(unsigned int delay_ms)
{
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}

struct files get_next_file(DIR *dir, char *directory_path) {
    struct files files;

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        size_t name_len = strlen(entry->d_name);
        if (name_len < 4 || strcmp(entry->d_name + name_len - 4, ".job") != 0) {
            continue; // Skip non-.job files
        }

        size_t input_len = strlen(directory_path) + name_len;

        strcpy(files.input_path, directory_path); // dir_pathname/
        strcat(files.input_path, entry->d_name);  // dir_pathname/file_name.job
        strcpy(files.output_path, files.input_path);
        strcpy(files.output_path + input_len - 4, ".out");

        files.fd_in = open(files.input_path, O_RDONLY);
        if (files.fd_in < 0) {
            perror("Failed to open input file");
            continue;
        }

        files.fd_out = open(files.output_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (files.fd_out < 0) {
            perror("Failed to open output file");
            close(files.fd_in);
            files.fd_in = NO_FILES_LEFT;
            continue;
        }

        return files; // Return the first valid .job file
    }
    files.fd_in = NO_FILES_LEFT;
    return files;
}

void* process_files(void *arg) {
    thread_args_t *args = (thread_args_t *)arg;
    DIR *dir = args->dir;
    char *directory_path = args->directory_path;
    int lim_backups = args->lim_backups;

    while (1) {
      pthread_mutex_t trinco;
      pthread_mutex_init(&trinco, NULL);
      pthread_mutex_lock(&trinco);
        struct files files = get_next_file(dir, directory_path);
        files.num_backups = 0;
        files.fd_in;
        files.fd_out;
        pthread_mutex_unlock(&trinco);
        if (files.fd_in < 0) {
            break;
        }

        int done = 0;
        while (!done) {
            char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
            char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
            unsigned int delay;
            size_t num_pairs;

            int command = get_next(files.fd_in);
            if (command == CMD_EMPTY || command == EOC) {
                if (command == EOC) {
                    close(files.fd_in);
                    close(files.fd_out);
                }
                break;
            }

            switch (command) {
                case CMD_WRITE:
                    num_pairs = parse_write(files.fd_in, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                    if (num_pairs == 0) {
                        write(STDERR_FILENO, "Invalid command. See HELP for usage\n", 36);
                        continue;
                    }

                    if (kvs_write(num_pairs, keys, values)) {
                        write(STDERR_FILENO, "Failed to write pair\n", 21);
                    }
                    break;

                case CMD_READ:
                    num_pairs = parse_read_delete(files.fd_in, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                    if (num_pairs == 0) {
                        write(STDERR_FILENO, "Invalid command. See HELP for usage\n", 36);
                        continue;
                    }

                    if (kvs_read(files.fd_out, num_pairs, keys)) {
                        write(STDERR_FILENO, "Failed to read pair\n", 20);
                    }
                    break;

                case CMD_DELETE:
                    num_pairs = parse_read_delete(files.fd_in, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                    if (num_pairs == 0) {
                        write(STDERR_FILENO, "Invalid command. See HELP for usage\n", 36);
                        continue;
                    }

                    if (kvs_delete(files.fd_out, num_pairs, keys)) {
                        write(STDERR_FILENO, "Failed to delete pair\n", 22);
                    }
                    break;

                case CMD_SHOW:
                    kvs_show(files.fd_out);
                    break;

                case CMD_WAIT:
                    if (parse_wait(files.fd_in, &delay, NULL) == -1) {
                        write(STDERR_FILENO, "Invalid command. See HELP for usage\n", 36);
                        continue;
                    }

                    if (delay > 0) {
                        printf("Waiting...\n");
                        kvs_wait(delay);
                    }
                    break;

                case CMD_BACKUP:
                    files.num_backups++;
                    if (kvs_backup(files, lim_backups) == 1) {
                        write(STDERR_FILENO, "Failed to perform backup.\n", 26);
                        files.num_backups--;
                    }
                    break;

                case CMD_INVALID:
                    write(STDERR_FILENO, "Invalid command. See HELP for usage\n", 36);
                    break;

                case CMD_HELP:
                    printf(
                        "Available commands:\n"
                        "  WRITE [(key,value)(key2,value2),...]\n"
                        "  READ [key,key2,...]\n"
                        "  DELETE [key,key2,...]\n"
                        "  SHOW\n"
                        "  WAIT <delay_ms>\n"
                        "  BACKUP\n"
                        "  HELP\n"
                    );
                    break;

                default:
                    write(STDERR_FILENO, "Invalid command. See HELP for usage\n", 36);
                    break;
            }
        }
        }

    return NULL;
}