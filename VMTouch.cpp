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

/*
This is based on VMTouch by Doug Hoyte (see copyright notice)
*/

/************************************************************************

Copyright 2009-2023 Doug Hoyte and contributors

Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.

3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS “AS IS” AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

***********************************************************************/

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