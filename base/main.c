#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"
#include <dirent.h>
#include "files.h"

int main(int argc, char *argv[])
{
  DIR *dir;
  int fd_in, fd_out;
  // tem que ter 2 args
  if (argc != 2)
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

  while (1)
  {
    struct files files = get_next_file(dir, argv[1]);
    fd_in = files.fd_in;
    //fd_out = list[1];
    printf("File descriptor: %d\n", fd_in); 
    if (fd_in < -1){
     continue;
    }
    while (1)
    {
      char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
      char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
      unsigned int delay;
      size_t num_pairs;

      fflush(stdout);

      switch (get_next(fd_in))
      {
      case CMD_WRITE:
        num_pairs = parse_write(fd_in, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0)
        {
          write(STDERR_FILENO, "Invalid command. See HELP for usage\n", 36);
          continue;
        }

        if (kvs_write(num_pairs, keys, values))
        {
          write(STDERR_FILENO, "Failed to write pair\n", 21);
        }

        break;

      case CMD_READ:
        num_pairs = parse_read_delete(fd_in, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0)
        {
          write(STDERR_FILENO, "Invalid command. See HELP for usage\n", 36);
          continue;
        }

        if (kvs_read(fd_out, num_pairs, keys))
        {
          write(STDERR_FILENO, "Failed to read pair\n", 20);
        }
        break;

      case CMD_DELETE:
        num_pairs = parse_read_delete(fd_in, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0)
        {
          write(STDERR_FILENO, "Invalid command. See HELP for usage\n", 36);
          continue;
        }

        if (kvs_delete(num_pairs, keys))
        {
          write(STDERR_FILENO, "Failed to delete pair\n", 22);
        }
        break;

      case CMD_SHOW:

        kvs_show(fd_out);
        break;

      case CMD_WAIT:
        if (parse_wait(fd_in, &delay, NULL) == -1)
        {
          write(STDERR_FILENO, "Invalid command. See HELP for usage\n", 36);
          continue;
        }

        if (delay > 0)
        {
          printf("Waiting...\n");
          kvs_wait(delay);
        }
        break;

      case CMD_BACKUP:

        if (kvs_backup())
        {
          write(STDERR_FILENO, "Failed to perform backup.\n", 26);
        }
        break;

      case CMD_INVALID:
        write(STDERR_FILENO, "Invalid command. See HELP for usage\n", 36);
        break;

      case CMD_QUIT: 
          kvs_terminate();
          printf("Exiting program.\n");
          close(fd_in);
          closedir(dir);
          return 0;

      case CMD_HELP:
        printf(
            "Available commands:\n"
            "  WRITE [(key,value)(key2,value2),...]\n"
            "  READ [key,key2,...]\n"
            "  DELETE [key,key2,...]\n"
            "  SHOW\n"
            "  WAIT <delay_ms>\n"
            "  BACKUP\n" // Not implemented
            "  HELP\n");

        break;

      case CMD_EMPTY:
        break;

      case EOC:
        kvs_terminate();
        close(fd_in);
        break;
      }
    }
  }
  closedir(dir);
  kvs_terminate();
  return 0;
}
