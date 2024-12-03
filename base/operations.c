#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <dirent.h>

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

void kvs_process_directory(const char *directory_path)
{
  DIR *dir = opendir(directory_path);
  if (!dir)
  {
    perror("Failed to open directory");
    return;
  }

  struct dirent *entry;
  // get next file in dir
  while ((entry = readdir(dir)) != NULL)
  {

    // todo : check if i can use d_namlen
    if (strcmp(entry->d_name + entry->d_namlen - 4, ".job") == 0)
    {
      char input_path[MAX_JOB_FILE_NAME_SIZE];
      char output_path[MAX_JOB_FILE_NAME_SIZE];

      int input_len = strlen(directory_path) + 1 + entry->d_namlen;

      if (input_len >= MAX_JOB_FILE_NAME_SIZE)
      {
        fprintf(stderr, "Path too long: %s/%s\n", directory_path, entry->d_name);
        continue;
      }

      strcpy(input_path, directory_path); // dir_pathname/
      strcat(input_path, entry->d_name);  // dir_pattname/file_name.out

      strcpy(input_len - 4, ".out");

      process_file(input_path, output_path);
    }
  }

  closedir(dir);
}

int get_next_file(const DIR *dir, const char *directory_path)
{
  struct dirent *entry;
  if ((entry = readdir(dir)) != NULL)
  {
    // todo : check if i can use d_namlen
    if (strcmp(entry->d_name + entry->d_namlen - 4, ".job") == 0)
    {
      char input_path[MAX_JOB_FILE_NAME_SIZE];
      char output_path[MAX_JOB_FILE_NAME_SIZE];

      int input_len = strlen(directory_path) + 1 + entry->d_namlen;

      if (input_len >= MAX_JOB_FILE_NAME_SIZE)
      {
        fprintf(stderr, "Path too long: %s/%s\n", directory_path, entry->d_name);
      }

      strcpy(input_path, directory_path); // dir_pathname/
      strcat(input_path, entry->d_name);  // dir_pattname/file_name.out

      strcpy(input_len - 4, ".out");
    }
  }
  else
  {
    return NULL;
  }
}