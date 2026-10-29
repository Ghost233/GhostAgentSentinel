#include <sys/clonefile.h>
#include <sys/stat.h>
#include <unistd.h>
#include <poll.h>
#include <stdio.h>
#include <errno.h>

/* Only inherited read-only source fd 3 and private destination directory fd 4. */
int main(void) {
  struct stat source, directory;
  if (fstat(3, &source) || fstat(4, &directory) || !S_ISREG(source.st_mode) ||
      !S_ISDIR(directory.st_mode) || source.st_uid != getuid() ||
      directory.st_uid != getuid() || (directory.st_mode & 077)) return 2;
  if (fclonefileat(3, 4, "record", CLONE_NOOWNERCOPY)) {
    fprintf(stderr, "%d\n", errno);
    return 3;
  }
  /* The parent opens the clone, acknowledges, and keeps an anonymous read fd.
     EOF or timeout also unlinks it if the parent dies during this handoff. */
  int result = write(1, "ready\n", 6) == 6 ? 0 : 4;
  struct pollfd control = { .fd = 0, .events = POLLIN | POLLHUP };
  char acknowledgement;
  if (!result && poll(&control, 1, 10000) > 0) (void)read(0, &acknowledgement, 1);
  if (unlinkat(4, "record", 0)) return 5;
  return result;
}
