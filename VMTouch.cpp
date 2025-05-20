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
#include <thread>
#include <unistd.h>

#include <iostream>

#include <VMTouch.h>

long VMTouch::pagesize = sysconf(_SC_PAGESIZE);

int64_t VMTouch::bytes2pages(int64_t bytes) { return (bytes + pagesize - 1) / pagesize; }

int VMTouch::aligned_p(void* p) { return 0 == ((long)p & (pagesize - 1)); }

bool VMTouch::is_mincore_page_resident(char p) { return p & 0x1; }

void VMTouch::do_nothing(unsigned int nothing) { return; }

void VMTouch::vmtouch_file(char* path, bool o_touch, bool verbose) {
    int fd = -1;
    void* mem = NULL;
    struct stat sb;
    int64_t len_of_file = 0;
    int64_t len_of_range = 0;
    int64_t pages_in_range;
    int i;
    int res;
    int open_flags;

    unsigned int junk_counter = 0; // just to prevent any compiler optimizations

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
            throw SException("Empty file");
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

        if (verbose) {
            cout << "Checking " << pages_in_range << " pages in file " << path << endl;
        }

        // Parallel counting.
        list<thread> counters;
        atomic<size_t> total_pages_resident(0);
        const size_t thread_count = thread::hardware_concurrency();
        const size_t pages_per_thread = (pages_in_range / thread_count) + 1;
        for (size_t i = 0; i < thread_count; i++) {
            counters.emplace_back([i, &pages_per_thread, &total_pages_resident, &mincore_array]() {
                size_t first_page = pages_per_thread * i;
                size_t pages_in_range = 0;
                for (size_t j = first_page; j < first_page + pages_per_thread; j++) {
                    if (is_mincore_page_resident(mincore_array[j])) {
                        pages_in_range++;
                    }
                }
                total_pages_resident += pages_in_range;
            });
        }
        for (auto& t : counters) {
            t.join();
        }

        double percentage_resident = ((double)total_pages_resident / (double)pages_in_range) * 100.0;
        if (verbose) {
            printf("Pages resident: %li (%.2f%%)", total_pages_resident.load(), percentage_resident);
        }

        // This doesn't work yet.
        if (o_touch) {
            for (i = 0; i < pages_in_range; i++) {
                junk_counter += ((char*)mem)[i * pagesize];
                mincore_array[i] = 1;
            }

            // Passing junk_counter here avoids a "set but not used" error.
            do_nothing(junk_counter);
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
