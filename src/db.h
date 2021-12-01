#ifndef _DB_H
#define _DB_H

#include <stdarg.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <unistd.h>
#include <errno.h>

#define DB_INSERT 1
#define DB_REPLACE 2
#define DB_STORE 3

#define INDEX_LENGTH_MINIMUM 6
#define INDEX_LENGTH_MAXIMUM 1024
#define DATA_LENGTH_MINIMUM 2
#define DATA_LENGTH_MAXIMUM 1024

void *db_open(const char*, int , ...);
void db_close(void *);
int db_store(void *, const char *, const char *, int);
int db_delete(void *, const char *);
char *db_get(void *, const char*);
char *db_next_record(void *, char *key);
void db_to_start(void *);

void error(const char *, ...);
void error_quit(const char *, ...);


#endif
