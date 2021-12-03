#ifndef _DB_H
#define _DB_H

#include <errno.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define DB_INSERT 1
#define DB_REPLACE 2
#define DB_STORE 3

#define INDEX_LENGTH_MINIMUM 6
#define INDEX_LENGTH_MAXIMUM 1024
#define DATA_LENGTH_MINIMUM 2
#define DATA_LENGTH_MAXIMUM 1024

enum {
  KANTA_OK = 0,
  KANTA_ERR = 1,
};

typedef void *kantadb_t;

kantadb_t db_open(const char *, int, ...);
void db_close(kantadb_t);
int db_store(kantadb_t, const char *, const char *, int);
int db_delete(kantadb_t, const char *);
char *db_get(kantadb_t, const char *);
char *db_next_record(kantadb_t, char *key);
void db_to_start(kantadb_t);

void error(const char *, ...);
void error_quit(const char *, ...);

#endif
