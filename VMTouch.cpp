#include "libstuff/libstuff.h"
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <linux/fs.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <unistd.h>

class VMTouch {
    static long pagesize;

    static int64_t bytes2pages(int64_t bytes) {
        return (bytes + pagesize - 1) / pagesize;
    }

    static int aligned_p(void* p) {
        return 0 == ((long)p & (pagesize - 1));
    }

    static void vmtouch_file(char* path, bool o_touch) {
        int fd = -1;
        void* mem = NULL;
        struct stat sb;
        int64_t len_of_file = 0;
        int64_t len_of_range = 0;
        int64_t pages_in_range;
        int i;
        int res;
        int open_flags;

        open_flags = O_RDONLY;
        open_flags |= O_NOATIME;

        try {
            fd = open(path, open_flags, 0);

            if (fd == -1 && errno == EPERM) {
                open_flags &= ~O_NOATIME;
                fd = open(path, open_flags, 0);
            }

            if (fd == -1) {
                SWARN("unable to open " << path << " (" << strerror(errno) << "), skipping");
                throw SException("Unable to open");
            }

            res = fstat(fd, &sb);
            if (res) {
                SWARN("unable to fstat " << path << " (" << strerror(errno) << "), skipping");
                throw SException("unable to fstat");
            }

            if (S_ISBLK(sb.st_mode)) {
                if (ioctl(fd, BLKGETSIZE64, &len_of_file)) {
                    SWARN("unable to ioctl " << path << " (" << strerror(errno) << "), skipping");
                    throw SException("unable to ioctl");
                }
            } else {
                len_of_file = sb.st_size;
            }

            if (len_of_file == 0) {
                throw exception();
            }

            mem = mmap(NULL, len_of_file, PROT_READ, MAP_SHARED, fd, 0);

            if (mem == MAP_FAILED) {
                SWARN("unable to mmap file " << path << " (" << strerror(errno) << "), skipping");
                throw SException("unable to mmap");
            }

            if (!aligned_p(mem)) {
                SERROR("mmap(" << path << ") wasn't page aligned");
            }

            pages_in_range = bytes2pages(len_of_range);

            char* mincore_array = (char*)malloc(pages_in_range);
            if (mincore_array == NULL) {
                SWARN("Failed to allocate memory for mincore array (" << strerror(errno) << ")");
            }

            if (mincore(mem, len_of_range, (unsigned char*)mincore_array)) {
                SERROR("mincore " << path << " (" << strerror(errno) << ")");
            }

            if (o_touch) {
                for (i = 0; i < pages_in_range; i++) {
                    mincore_array[i] = 1;
                }
            }

            free(mincore_array);
        } catch (const SException& e) {
            if (mem) {
                if (munmap(mem, len_of_range)) {
                    SWARN("unable to munmap file " << path << " (" << strerror(errno) << ")");
                }
            }

            if (fd != -1) {
                close(fd);
            }
        }
    }
};

long VMTouch::pagesize = sysconf(_SC_PAGESIZE);