#include "errors.h"
#include <stdarg.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>

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

void err_quit(const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);

  err_doit(0, 0, fmt, ap);
  va_end(ap);

  exit(1);
}

void err(const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  err_doit(1, errno, fmt, ap);

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
