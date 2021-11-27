#include "db.h"
#include <stddef.h>
#include <stdio.h>
#include <sys/file.h>
#include <sys/types.h>

// index related stuff
#define INDEX_LEN_SIZE 4
#define SEPARATOR ':'
#define SPACE ' '
#define NEWLN '\n'

#define PTR_SIZE 7
#define PTR_MAX 9999999
#define NHASH_DEF 137
#define FREE_OFF 0
#define HASH_OFF PTR_SIZE

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

// internal functions
static DB *_db_alloc(int);
static void _db_dodelete(DB *);
static int _db_find_and_lock(DB *, const char *, int);
static int _db_find_free(DB *, int, int);
static void _db_free(DB *);
static DBHASH _db_hash(DB *, const char *);
static char *_db_readat(DB *);
static off_t _db_read_index(DB *, off_t);
static off_t _db_read_ptr(DB *, off_t);
static void _db_write_data(DB *, const char *, off_t, int);
static void _db_write_index(DB *, const char *, off_t, int, off_t);
static void _db_write_ptr(DB *, off_t, off_t);

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

void err_dump(const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  err_doit(1, errno, fmt, ap);
  va_end(ap);

  abort();
  exit(1);
}

void *db_open(const char *pathname, int oflag, ...) {
  DB *db;
  size_t i;
  int mode;
  char asciiptr[PTR_SIZE + 1], hash[(NHASH_DEF + 1) * PTR_SIZE + 2];

  struct stat stat_buffer;

  int len = strlen(pathname);
  if ((db = _db_alloc(len)) == NULL)
    err_dump("db_open: _db_alloc error for database.");

  db->nhash = NHASH_DEF;
  db->hash_offset = HASH_OFF;

  strcpy(db->name, pathname);
  strcat(db->name, ".idx");

  if (oflag & O_CREAT) {
    va_list ap;
    va_start(ap, oflag);
    mode = va_arg(ap, int);
    va_end(ap);

    db->index_fd = open(db->name, oflag, mode);
    strcpy(db->name + len, ".dat");
    db->data_fd = open(db->name, oflag, mode);
  } else {
    db->index_fd = open(db->name, oflag);
    strcpy(db->name + len, ".dat");
    db->data_fd = open(db->name, oflag);
  }

  if (db->index_fd < 0 || db->data_fd < 0) {
    _db_free(db);
    return NULL;
  }

  if ((oflag & (O_CREAT | O_TRUNC)) == (O_CREAT | O_TRUNC)) {
    // TODO
  }

  db_to_start(db);
  return db;
}

static DB *_db_alloc(int namelen) {
  DB *db;

  if ((db = calloc(1, sizeof(DB))) == NULL)
    err_dump("_db_alloc: calloc error for DB");
  db->index_fd = db->data_fd = -1;

  if ((db->name = malloc(namelen + 5)) == NULL)
    err_dump("_db_alloc: malloc error for name");

  if ((db->index_buffer = malloc(INDEX_LENGTH_MAXIMUM + 2)) == NULL)
    err_dump("_db_alloc: malloc error for index buffer.");

  if ((db->data_buffer = malloc(DATA_LENGTH_MAXIMUM + 2)) == NULL)
    err_dump("_db_alloc: malloc error for index buffer.");

  return db;
}

void db_close(void *h) {
  _db_free((DB*)h);
}

static void _db_free(DB *db) {
  if (db->index_fd >= 0)
    close(db->index_fd);

  if (db->data_fd >= 0)
    close(db->data_fd);

  if (db->index_buffer != NULL)
    free(db->index_buffer);

  if (db->data_buffer != NULL)
    free(db->data_buffer);

  if (db->name != NULL)
    free(db->name);
  free(db);
}
