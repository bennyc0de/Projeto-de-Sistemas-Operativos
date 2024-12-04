#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <dirent.h>
#include <fcntl.h>
#include "kvs.h"
#include "constants.h"

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

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE])
{
  if (kvs_table == NULL)
  {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  printf("[");
  for (size_t i = 0; i < num_pairs; i++)
  {
    char *result = read_pair(kvs_table, keys[i]);
    if (result == NULL)
    {
      printf("(%s,KVSERROR)", keys[i]);
    }
    else
    {
      printf("(%s,%s)", keys[i], result);
    }
    free(result);
  }
  printf("]\n");
  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE])
{
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
        printf("[");
        aux = 1;
      }
      printf("(%s,KVSMISSING)", keys[i]);
    }
  }
  if (aux)
  {
    printf("]\n");
  }

  return 0;
}

void kvs_show()
{
  for (int i = 0; i < TABLE_SIZE; i++)
  {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL)
    {
      printf("(%s, %s)\n", keyNode->key, keyNode->value);
      keyNode = keyNode->next; // Move to the next node
    }
  }
}

int kvs_backup()
{
  return 0;
}

void kvs_wait(unsigned int delay_ms)
{
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}

int* get_list_of_integers(size_t* size) {
    // Define the size of the list
    *size = 2;

    // Allocate memory for the list
    int* list = (int*)malloc(*size * sizeof(int));
    if (list == NULL) {
        perror("Failed to allocate memory");
        return NULL;
    }

    // Initialize the list with some values
    for (size_t i = 0; i < *size; i++) {
        list[i] = -1 * (int)i + 1;
    }

    return list;
}

int* get_next_file(DIR *dir, char *directory_path)
{
  size_t size;
  int* list = get_list_of_integers(&size);

  struct dirent *entry;
  if ((entry = readdir(dir)) != NULL)
  {
    size_t name_len = strlen(entry->d_name);
    if (strcmp(entry->d_name + name_len - 4, ".job") == 0)
    {
      char input_path[MAX_JOB_FILE_NAME_SIZE];
      char output_path[MAX_JOB_FILE_NAME_SIZE];

      size_t input_len = strlen(directory_path) + name_len;

      if (input_len >= MAX_JOB_FILE_NAME_SIZE)
      {
        fprintf(stderr, "Path too long: %s/%s\n", directory_path, entry->d_name);
      }

      strcpy(input_path, directory_path); // dir_pathname/
      strcat(input_path, entry->d_name);  // dir_pattname/file_name.job
      strcpy(output_path, input_path);
      strcpy(output_path + input_len - 4, ".out");

      int fd_in = open(input_path, O_RDONLY);
      int fd_out = open(output_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);

      list[0] = fd_in;
      list[1] = fd_out;
    }
    return list;
  }
  else
  {
    return list;
  }
}