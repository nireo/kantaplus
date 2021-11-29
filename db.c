#include "db.h"
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>

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

#define read_lock(fd, offset, whence, len)                                     \
  lock_reg((fd), F_SETLK, F_RDLCK, (offset), (whence), (len))

#define readw_lock(fd, offset, whence, len)                                    \
  lock_reg((fd), F_SETLKW, F_RDLCK, (offset), (whence), (len))

#define write_lock(fd, offset, whence, len)                                    \
  lock_reg((fd), F_SETLK, F_WRLCK, (offset), (whence), (len))

#define writew_lock(fd, offset, whence, len)                                   \
  lock_reg((fd), F_SETLKW, F_WRLCK, (offset), (whence), (len))

#define un_lock(fd, offset, whence, len)                                       \
  lock_reg((fd), F_SETLK, F_UNLCK, (offset), (whence), (len))

int lock_reg(int fd, int cmd, int type, off_t offset, int whence, off_t len) {
  struct flock lock;
  lock.l_type = type;
  lock.l_start = offset;
  lock.l_whence = whence;
  lock.l_len = len;

  return (fcntl(fd, cmd, &lock));
}

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
    if (writew_lock(db->index_fd, 0, SEEK_SET, 0) < 0)
      err_dump("db_open: writew_lock error");

    if (fstat(db->index_fd, &stat_buffer) < 0)
      error("db_open: fstat error");

    if (stat_buffer.st_size == 0) {
      sprintf(asciiptr, "%*d", PTR_SIZE, 0);
      hash[0] = 0;

      for (i = 0; i < NHASH_DEF + 1; ++i) {
        strcat(hash, asciiptr);
      }
      strcat(hash, "\n");

      i = strlen(hash);
      if (write(db->index_fd, hash, i) != i)
        err_dump("db_open: index file init write error");
    }

    if (un_lock(db->index_fd, 0, SEEK_SET, 0) < 0)
      err_dump("db_open: un_lock error");
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

void db_close(void *h) { _db_free((DB *)h); }

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

char *db_get(void *h, const char *key) {
  DB *db = h;
  char *ptr;

  if (_db_find_and_lock(db, key, 0) < 0) {
    ptr = NULL;
    ++db->count_get_error;
  } else {
    ptr = _db_readat(db);
    ++db->count_get_ok;
  }

  if (un_lock(db->index_fd, db->chain_offset, SEEK_SET, 1) < 0)
    err_dump("db_get: un_lock error");

  return ptr;
}

static int _db_find_and_lock(DB *db, const char *key, int writelock) {
  off_t offset, nextoffset;

  db->chain_offset = (_db_hash(db, key) * PTR_SIZE) + db->hash_offset;
  db->ptr_offset = db->chain_offset;

  if (writelock) {
    if (writew_lock(db->index_fd, db->chain_offset, SEEK_SET, 1) < 0)
      err_dump("_db_find_and_lock: writew_lock error");
  } else {
    if (readw_lock(db->index_fd, db->chain_offset, SEEK_SET, 1) < 0)
      err_dump("_db_find_and_lock: readw_lock error");
  }

  offset = _db_read_ptr(db, db->ptr_offset);

  while (offset != 0) {
    nextoffset = _db_read_index(db, offset);
    if (strcmp(db->index_buffer, key) == 0)
      break;

    db->ptr_offset = offset;
    offset = nextoffset;
  }

  return offset == 0 ? -1 : 0;
}

static DBHASH _db_hash(DB *db, const char *key) {
  DBHASH hval = 0;
  char c;

  for (int i = 1; (c = *key++) != 0; ++i) {
    hval += c * i;
  }

  return hval % db->nhash;
}

static off_t _db_read_ptr(DB *db, off_t offset) {
  char asciiptr[PTR_SIZE + 1];

  if (lseek(db->index_fd, offset, SEEK_SET) == -1)
    err_dump("_db_read_tr: lseek error to ptr field");

  if (read(db->index_fd, asciiptr, PTR_SIZE) != PTR_SIZE)
    err_dump("_db_readptr: read error of ptr field");

  asciiptr[PTR_SIZE] = 0;
  return atol(asciiptr);
}

static off_t _db_read_index(DB *db, off_t offset) {
  ssize_t i;
  char *ptr1, *ptr2;
  char asciiptr[PTR_SIZE + 1], asciilen[INDEX_LEN_SIZE + 1];
  struct iovec iov[2];

  if ((db->index_offset = lseek(db->index_fd, offset,
                                offset == 0 ? SEEK_CUR : SEEK_SET)) == -1)
    err_dump("_db_read_index: lseek error");

  iov[0].iov_base = asciiptr;
  iov[0].iov_len = PTR_SIZE;
  iov[1].iov_base = asciilen;
  iov[1].iov_len = INDEX_LEN_SIZE;

  if ((i = readv(db->index_fd, &iov[0], 2)) != PTR_SIZE + INDEX_LEN_SIZE) {
    if (i == 0 && offset == 0)
      return -1;

    err_dump("_db_read_index: readv error of index record");
  }

  asciiptr[PTR_SIZE] = 0;
  db->ptr_val = atol(asciiptr);

  asciilen[INDEX_LEN_SIZE] = 0;
  if ((db->index_length = atoi(asciilen)) < INDEX_LENGTH_MINIMUM ||
      db->index_length > INDEX_LENGTH_MAXIMUM)
    err_dump("_db_read_index: invalid length");

  if ((i = read(db->index_fd, db->index_buffer, db->index_length)) !=
      db->index_length)
    err_dump("_db_read_index: read error of index record.");

  if (db->index_buffer[db->index_length - 1] != NEWLN)
    err_dump("_db_readidx: missing newline");

  db->index_buffer[db->index_length - 1] = 0;

  if ((ptr1 = strchr(db->index_buffer, SEPARATOR)) == NULL)
    err_dump("_db_read_index: missing first separator");
  *ptr1++ = 0;

  if ((ptr2 = strchr(ptr1, SEPARATOR)) == NULL)
    err_dump("_db_read_index: missing second separator");
  *ptr2++ = 0;

  if (strchr(ptr2, SEPARATOR) != NULL)
    err_dump("_db_read_index: too many separators");

  if ((db->data_offset = atol(ptr1)) < 0)
    err_dump("_db_read_index: start offset < 0");

  if ((db->data_length = atol(ptr2)) <= 0 ||
      db->data_length > DATA_LENGTH_MAXIMUM)
    err_dump("_db_read_index: invalid length");

  return db->ptr_val;
}

static char *_db_readat(DB *db) {
  if (lseek(db->data_fd, db->data_offset, SEEK_SET) == -1)
    err_dump("_db_readat: lseek error");

  if (read(db->data_fd, db->data_buffer, db->data_length) != db->data_length)
    err_dump("_db_readat: read error");

  if (db->data_buffer[db->data_length - 1] != NEWLN)
    err_dump("_db_readat: missing newline");

  db->data_buffer[db->data_length - 1] = 0;
  return db->data_buffer;
}

int db_delete(void *h, const char *key) {
  DB *db = h;
  int rc = 0;

  if (_db_find_and_lock(db, key, 1) == 0) {
    _db_dodelete(db);
    ++db->count_delete_ok;
  } else {
    rc = -1;
    ++db->count_delete_error;
  }

  if (un_lock(db->index_fd, db->chain_offset, SEEK_SET, 1), 0)
    err_dump("db_delete: un_lock error");

  return rc;
}

static void _db_dodelete(DB *db) {
  int i;
  char *ptr;
  off_t freeptr, saveptr;

  for (ptr = db->data_buffer, i = 0; i < db->data_length - 1; ++i)
    *ptr++ = SPACE;
  *ptr = 0;

  ptr = db->index_buffer;

  while (*ptr)
    *ptr++ = SPACE;

  if (writew_lock(db->index_fd, FREE_OFF, SEEK_SET, 1) < 0)
    err_dump("_db_dodelete: writew_lock error");
  _db_write_data(db, db->data_buffer, db->data_offset, SEEK_SET);

  freeptr = _db_read_ptr(db, FREE_OFF);
  saveptr = db->ptr_val;

  _db_write_index(db, db->index_buffer, db->index_offset, SEEK_SET, freeptr);
  _db_write_ptr(db, FREE_OFF, db->index_offset);

  _db_write_ptr(db, db->ptr_offset, saveptr);
  if (un_lock(db->index_fd, FREE_OFF, SEEK_SET, 1) < 0)
    err_dump("_db_dodelete: un_lock error");
}
