#ifndef _DB_LOCK_H

#include <sys/types.h>

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

int lock_reg(int, int, int, off_t, int, off_t);

#endif
