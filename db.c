#include "db.h"
#include <stddef.h>
#include <stdio.h>

// index related stuff
#define INDEX_LEN_SIZE 4
#define SEPARATOR ':'
#define SPACE ' '
#define NEWLN '\n'

#define PTR_SIZE 7
#define PTR_MAX 9999999
#define NHASH_DEF 137
#define FREE_OFF 0
#define HASH_OFF POINTER_SIZE

typedef unsigned long DBHASH;
typedef unsigned long COUNT;

typedef struct {
  int index_fd;
  int data_fd;

  char *index_buffer;
  char *data_buffer;
  char *name;

  off_t index_offset;
  size_t index_length;

  off_t data_offset;
  size_t data_length;

  off_t ptr_val;
  off_t ptr_offset;
  off_t chain_offset;
  off_t hash_offset;
  DBHASH nhash;

  COUNT count_delete_ok;
  COUNT count_delete_error;
  COUNT count_get_ok;
  COUNT count_get_error;
  COUNT count_next_record;
  COUNT count_st1;
  COUNT count_st2;
  COUNT count_st3;
  COUNT count_st4;
  COUNT count_st_err;
} DB;

static void err_doit(int errnoflag, int error, const char *fmt, va_list ap) {
  char buf[4096];
  vsnprintf(buf, 4096 - 1, fmt, ap);
  if (errnoflag)
    snprintf(buf + strlen(buf), 4096 - strlen(buf) - 1, ": %s",
             strerror(error));
  strcat(buf, "\n");
  fflush(stdout);

  fputs(buf, stderr);
  fflush(NULL);
}

void error(const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  err_doit(1, errno, fmt, ap);

  va_end(ap);
  exit(1);
}

void err_quit(const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);

  err_doit(0, 0, fmt, ap);
  va_end(ap);

  exit(1);
}
