#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>

#include "db.h"
#include "errors.h"
#include "lock.h"

#define INDEX_LEN_SIZE 4
#define SEPARATOR ':'
#define SPACE ' '
#define NEWLN '\n'

#define PTR_SIZE 7
#define PTR_MAX 9999999
#define NHASH_DEF 137
#define FREE_OFF 0
#define HASH_OFF PTR_SIZE

typedef unsigned long dbhash_t;
typedef unsigned long count_t;

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
  dbhash_t nhash;

  count_t count_delete_ok;
  count_t count_delete_error;
  count_t count_get_ok;
  count_t count_get_error;
  count_t count_next_record;
  count_t count_st1;
  count_t count_st2;
  count_t count_st3;
  count_t count_st4;
  count_t count_st_err;
} DB;

static DB *_db_alloc(int);
static void _db_dodelete(DB *);
static int _db_find_and_lock(DB *, const char *, int);
static int _db_find_free(DB *, int, int);
static void _db_free(DB *);
static dbhash_t _db_hash(DB *, const char *);
static char *_db_readat(DB *);
static off_t _db_read_index(DB *, off_t);
static off_t _db_read_ptr(DB *, off_t);
static void _db_write_data(DB *, const char *, off_t, int);
static void _db_write_index(DB *, const char *, off_t, int, off_t);
static void _db_write_ptr(DB *, off_t, off_t);

kantadb_t db_open(const char *pathname, int oflag, ...) {
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

void db_close(kantadb_t h) { _db_free((DB *)h); }

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

char *db_get(kantadb_t h, const char *key) {
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

static dbhash_t _db_hash(DB *db, const char *key) {
  dbhash_t hval = 0;
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

int db_delete(kantadb_t h, const char *key) {
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

static void _db_write_data(DB *db, const char *data, off_t offset, int whence) {
  struct iovec iov[2];
  static char newline = NEWLN;

  if (whence == SEEK_END)
    if (writew_lock(db->data_fd, 0, SEEK_SET, 0) < 0)
      err_dump("_db_write_data: writew_lock error");

  if ((db->data_offset = lseek(db->data_fd, offset, whence)) == -1)
    err_dump("_db_write_data: lseek error");

  db->data_length = strlen(data) + 1;

  iov[0].iov_base = (char *)data;
  iov[0].iov_len = db->data_length - 1;
  iov[1].iov_base = &newline;
  iov[1].iov_len = 1;

  if (writev(db->data_fd, &iov[0], 2) != db->data_length)
    err_dump("_db_write_data: writev error of data record");

  if (whence == SEEK_END) {
    if (un_lock(db->data_fd, 0, SEEK_SET, 0) < 0)
      err_dump("_db_write_data: un_lock error");
  }
}

static void _db_write_index(DB *db, const char *key, off_t offset, int whence,
                            off_t ptrval) {
  struct iovec iov[2];
  char asciiptrlen[PTR_SIZE + INDEX_LEN_SIZE + 1];
  int len;

  if ((db->ptr_val = ptrval) < 0 || ptrval > PTR_MAX)
    err_quit("_db_write_index: invalid ptr: %d", ptrval);

  sprintf(db->index_buffer, "%s%c%lld%c%ld\n", key, SEPARATOR,
          (long long)db->data_offset, SEPARATOR, (long)db->data_length);

  len = strlen(db->index_buffer);
  if (len < INDEX_LENGTH_MINIMUM || len > INDEX_LENGTH_MAXIMUM)
    err_dump("_db_write_index: invalid length");
  sprintf(asciiptrlen, "%*lld%*d", PTR_SIZE, (long long)ptrval, INDEX_LEN_SIZE,
          len);

  if (whence == SEEK_END) {
    if (writew_lock(db->index_fd, ((db->nhash + 1) * PTR_SIZE) + 1, SEEK_SET,
                    0) < 0)
      err_dump("_db_write_index: writew_lock error");
  }

  if ((db->index_offset = lseek(db->index_fd, offset, whence)) == -1)
    err_dump("_db_write_index: lseek error");
  iov[0].iov_base = asciiptrlen;
  iov[0].iov_len = PTR_SIZE + INDEX_LEN_SIZE;
  iov[1].iov_base = db->index_buffer;
  iov[1].iov_len = len;
  if (writev(db->index_fd, &iov[0], 2) != PTR_SIZE + INDEX_LEN_SIZE + len)
    err_dump("_db_write_index: writev error of index record");

  if (whence == SEEK_END)
    if (un_lock(db->index_fd, ((db->nhash + 1) * PTR_SIZE) + 1, SEEK_SET, 0) <
        0)
      err_dump("_db_write_index: un_lock error");
}

static void _db_write_ptr(DB *db, off_t offset, off_t ptrval) {
  char asciiptr[PTR_SIZE + 1];
  if (ptrval < 0 || ptrval > PTR_MAX)
    err_quit("_db_writeptr: invalid ptr: %d", ptrval);
  sprintf(asciiptr, "%*lld", PTR_SIZE, (long long)ptrval);

  if (lseek(db->index_fd, offset, SEEK_SET) == -1)
    err_dump("_db_writeptr: lseek error to ptr field");

  if (write(db->index_fd, asciiptr, PTR_SIZE) != PTR_SIZE)
    err_dump("_db_writeptr: write error of ptr field");
}

int db_store(kantadb_t h, const char *key, const char *data, int flag) {
  DB *db = h;
  int rc, keylen, datalen;
  off_t ptrval;

  if (flag != DB_INSERT && flag != DB_REPLACE && flag != DB_STORE) {
    errno = EINVAL;
    return -1;
  }

  keylen = strlen(key);
  datalen = strlen(data) + 1;

  if (datalen < DATA_LENGTH_MINIMUM || datalen > DATA_LENGTH_MAXIMUM)
    err_dump("db_store: invalid data length");

  if (_db_find_and_lock(db, key, 1) < 0) {
    if (flag == DB_REPLACE) {
      rc = -1;
      ++db->count_st_err;
      errno = ENOENT;
      goto doreturn;
    }

    ptrval = _db_read_ptr(db, db->chain_offset);
    if (_db_find_free(db, keylen, datalen) < 0) {
      _db_write_data(db, data, 0, SEEK_END);
      _db_write_index(db, key, 0, SEEK_END, ptrval);

      _db_write_ptr(db, db->chain_offset, db->index_offset);
      ++db->count_st1;
    } else {
      _db_write_data(db, data, db->data_offset, SEEK_SET);
      _db_write_index(db, key, db->index_offset, SEEK_SET, ptrval);
      _db_write_ptr(db, db->chain_offset, db->index_offset);
      ++db->count_st2;
    }
  } else {
    if (flag == DB_INSERT) {
      rc = 1;
      ++db->count_st_err;
      goto doreturn;
    }

    if (datalen != db->data_length) {
      _db_dodelete(db);
      ptrval = _db_read_ptr(db, db->chain_offset);
      _db_write_data(db, data, 0, SEEK_END);
      _db_write_index(db, key, 0, SEEK_END, ptrval);

      _db_write_ptr(db, db->chain_offset, db->index_offset);
      ++db->count_st3;
    } else {
      _db_write_data(db, data, db->data_offset, SEEK_SET);
      ++db->count_st4;
    }
  }

  rc = 0;
doreturn:
  if (un_lock(db->index_fd, db->chain_offset, SEEK_SET, 1) < 0)
    err_dump("db_store: un_lock error");
  return rc;
}

static int _db_find_free(DB *db, int keylen, int datalen) {
  int rc;
  off_t offset, nextoffset, saveoffset;

  if (writew_lock(db->index_fd, FREE_OFF, SEEK_SET, 1) < 0) {
    err_dump("_db_find_free: writew_lock error");
  }

  saveoffset = FREE_OFF;
  offset = _db_read_ptr(db, saveoffset);

  while (offset != 0) {
    nextoffset = _db_read_index(db, offset);
    if (strlen(db->index_buffer) == keylen && db->data_length == datalen) {
      break;
    }

    saveoffset = offset;
    offset = nextoffset;
  }

  if (offset == 0) {
    rc = -1;
  } else {
    _db_write_ptr(db, saveoffset, db->ptr_val);
    rc = 0;
  }

  if (un_lock(db->index_fd, FREE_OFF, SEEK_SET, 1) < 0)
    err_dump("_db_find_free: un_lock error");

  return rc;
}

void db_to_start(kantadb_t h) {
  DB *db = h;
  off_t offset;

  offset = (db->nhash + 1) * PTR_SIZE;

  if ((db->index_offset = lseek(db->index_fd, offset + 1, SEEK_SET)) == -1)
    err_dump("db_rewind: lseek error");
}

char *db_next_record(kantadb_t h, char *key) {
  DB *db = h;
  char c;
  char *ptr;

  if (readw_lock(db->index_fd, FREE_OFF, SEEK_SET, 1) < 0)
    err_dump("db_next_record: readw_lock error");

  do {
    if (_db_read_index(db, 0) < 0) {
      ptr = NULL;
      goto doreturn;
    }

    ptr = db->index_buffer;
    while ((c = *ptr++) != 0 && c == SPACE)
      ;
  } while (c == 0);

  if (key != NULL) {
    strcpy(key, db->index_buffer);
  }
  ptr = _db_readat(db);

  ++db->count_next_record;

doreturn:
  if (un_lock(db->index_fd, FREE_OFF, SEEK_SET, 1) < 0)
    err_dump("db_next_record: un_lock error");

  return ptr;
}
