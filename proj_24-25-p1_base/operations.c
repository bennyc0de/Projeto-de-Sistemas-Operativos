#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#include <ctype.h>
#include "kvs.h"
#include "constants.h"
#include "parser.h"
#include "operations.h"

static struct HashTable* kvs_table = NULL;


/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }
  }

  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  printf("[");
  for (size_t i = 0; i < num_pairs; i++) {
    char* result = read_pair(kvs_table, keys[i]);
    if (result == NULL) {
      printf("(%s,KVSERROR)", keys[i]);
    } else {
      printf("(%s,%s)", keys[i], result);
    }
    free(result);
  }
  printf("]\n");
  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  int aux = 0;

  for (size_t i = 0; i < num_pairs; i++) {
    if (delete_pair(kvs_table, keys[i]) != 0) {
      if (!aux) {
        printf("[");
        aux = 1;
      }
      printf("(%s,KVSMISSING)", keys[i]);
    }
  }
  if (aux) {
    printf("]\n");
  }

  return 0;
}

void kvs_show() {
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
      printf("(%s, %s)\n", keyNode->key, keyNode->value);
      keyNode = keyNode->next; // Move to the next node
    }
  }
}

int kvs_backup() {
  return 0;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}

void process_file(const char *input_path, const char *output_path) {
    int fd_in = open(input_path, O_RDONLY);
    if (fd_in < 0) {
        perror("Failed to open input file");
        return;
    }

    int fd_out = open(output_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd_out < 0) {
        perror("Failed to open output file");
        close(fd_in);
        return;
    }

    // Save the original stdout file descriptor
    int original_stdout = dup(STDOUT_FILENO);
    if (original_stdout == -1) {
        perror("dup");
        close(fd_in);
        close(fd_out);
        return;
    }

    // Redirect stdout to the output file descriptor
    if (dup2(fd_out, STDOUT_FILENO) == -1) {
        perror("dup2");
        close(fd_in);
        close(fd_out);
        close(original_stdout);
        return;
    }

    while (!EOC) {
        switch (get_next(fd_in)) {
            case CMD_WRITE: {
                char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
                char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
                size_t num_pairs = parse_write(fd_in, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    printf("Invalid command. See HELP for usage\n");
                    continue;
                }

                if (kvs_write(num_pairs, keys, values)) {
                    printf("Failed to write pair\n");
                }
                break;
            }

            case CMD_READ: {
                char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
                size_t num_pairs = parse_read_delete(fd_in, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    printf("Invalid command. See HELP for usage\n");
                    continue;
                }

                if (kvs_read(num_pairs, keys)) {
                    printf("Failed to read pair\n");
                }
                break;
            }

            case CMD_DELETE: {
                char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
                size_t num_pairs = parse_read_delete(fd_in, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
                if (num_pairs == 0) {
                    printf("Invalid command. See HELP for usage\n");
                    continue;
                }

                if (kvs_delete(num_pairs, keys)) {
                    printf("Failed to delete pair\n");
                }
                break;
            }

            case CMD_SHOW:
                kvs_show();
                break;

            case CMD_WAIT: {
                unsigned int delay;
                if (parse_wait(fd_in, &delay, NULL) == -1) {
                    printf("Invalid command. See HELP for usage\n");
                    continue;
                }

                if (delay > 0) {
                    printf("Waiting...\n");
                    kvs_wait(delay);
                }
                break;
            }
            case CMD_BACKUP:
                if (kvs_backup()) {
                    printf("Failed to perform backup.\n");
                }
                break;

            case CMD_INVALID:
                printf("Invalid command. See HELP for usage\n");
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
            
            case EOC:
            case CMD_EMPTY:
            case CMD_OPENDIR:
            case CMD_QUIT:
                // These commands are not relevant in the context of process_file
                break;

            default:
                printf("Invalid command. See HELP for usage\n");
                break;

    }
  }
    // Restore the original stdout
    if (dup2(original_stdout, STDOUT_FILENO) == -1) {
        perror("dup2");
    }
    close(original_stdout);

    close(fd_in);
    close(fd_out);
}

void trim_whitespace(char *str) {
    char *start = str;

    // Find the first non-whitespace character
    while (isspace((unsigned char)*start)) {
        start++;
    }

    // Shift the string to the left
    if (start != str) {
        memmove(str, start, strlen(start) + 1);
    }
}

void kvs_process_directory(const char *directory_path) {
    char trimmed_path[MAX_JOB_FILE_NAME_SIZE];
    strncpy(trimmed_path, directory_path, MAX_JOB_FILE_NAME_SIZE);
    trimmed_path[MAX_JOB_FILE_NAME_SIZE - 1] = '\0'; // Ensure null-termination
    trim_whitespace(trimmed_path);

    DIR *dir = opendir(trimmed_path);
    if (!dir) {
        perror("Failed to open directory");
        return;
    }

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strstr(entry->d_name, ".job")) {
            char input_path[MAX_JOB_FILE_NAME_SIZE];
            char output_path[MAX_JOB_FILE_NAME_SIZE];

            size_t dir_len = strlen(directory_path);
            size_t name_len = strlen(entry->d_name);

            if (dir_len + 1 + name_len >= MAX_JOB_FILE_NAME_SIZE) {
                fprintf(stderr, "Path too long: %s/%s\n", directory_path, entry->d_name);
                continue;
            }

            strcpy(input_path, trimmed_path);
            strcat(input_path, entry->d_name);

            if (dir_len + 1 + name_len + 4 >= MAX_JOB_FILE_NAME_SIZE) { // 4 for ".out"
                fprintf(stderr, "Path too long: %s/%s.out\n", directory_path, entry->d_name);
                continue;
            }

            strcpy(output_path, input_path);
            size_t input_len = strlen(input_path);
            if (input_len >= 4 && strcmp(input_path + input_len - 4, ".job") == 0) {
                strcpy(output_path + input_len - 4, ".out");
            } else {
                fprintf(stderr, "File does not end with .job: %s\n", input_path);
                continue;
            }

            process_file(input_path, output_path);
        }
    }

    closedir(dir);
}