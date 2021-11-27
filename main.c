#include "db.h"
#include <fcntl.h>

int main(void) {
    void *db;
    if ((db = db_open("db_file", O_RDWR | O_CREAT | O_TRUNC, (S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH))) == NULL) {
      error("error when db_open");
    }

    if (db_store(db, "test1", "data1", 1) != 0)
      error_quit("db_store error when storing test1");

    if (db_store(db, "test2", "data2", 2) != 0)
      error_quit("db_store error when storing test2");

    if (db_store(db, "test2", "data3", 3) != 0)
      error_quit("db_store error when storing test3");

    db_close(db);
    exit(0);
}
