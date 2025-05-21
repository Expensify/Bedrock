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

#include <libstuff/libstuff.h>
#include <VMTouch.h>

long VMTouch::pagesize = sysconf(_SC_PAGESIZE);

int64_t VMTouch::bytes2pages(int64_t bytes) {
    int64_t pages = bytes / pagesize;
    if (bytes % pagesize) {
        pages++;
    }
    return pages;
}

int VMTouch::aligned_p(void* p) {
    return 0 == ((long)p & (pagesize - 1));
}


bool VMTouch::is_mincore_page_resident(char page) {
    // The least significant bit (0x1) indicates if the corresponding page is currently resident (present in physical memory).
    return page & 0x1;
}

void VMTouch::do_nothing(unsigned int nothing) {
    return;
}

void VMTouch::vmtouch_file(const char* path, bool o_touch, bool verbose) {
    int fd = -1;
    void* mem = NULL;
    struct stat sb;
    int64_t len_of_file = 0;
    int64_t pages_in_range;
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

        pages_in_range = bytes2pages(len_of_file);

        char* mincore_array = (char*)malloc(pages_in_range);
        if (mincore_array == NULL) {
            SWARN("Failed to allocate memory for mincore array (" << strerror(errno) << ")");
        }

        if (mincore(mem, len_of_file, (unsigned char*)mincore_array)) {
            SERROR("mincore " << path << " (" << strerror(errno) << ")");
        }

        if (o_touch) {
            if (verbose) {
                cout << "Touching memory." << endl;
            }
            list<thread> counters;
            const size_t thread_count = thread::hardware_concurrency();
            int64_t pages_per_thread = (pages_in_range / thread_count) + 1;
            for (size_t i = 0; i < thread_count; i++) {
                counters.emplace_back([i, &pages_per_thread, &pages_in_range, &mincore_array, &mem]() {
                    unsigned int junk_counter = 0;
                    int64_t first_page = pages_per_thread * i;
                    int64_t last_page = min(first_page + pages_per_thread, pages_in_range - 1);
                    for (int64_t j = first_page; j < last_page; j++) {
                        if (!is_mincore_page_resident(mincore_array[j])) {
                            junk_counter += ((char*)mem)[j * pagesize];
                        }
                    }

                    // Passing junk_counter here avoids a "set but not used" error.
                    do_nothing(junk_counter);
                });
            }
            for (auto& t : counters) {
                t.join();
            }
            cout << "Reloading map." << endl;
            if (mincore(mem, len_of_file, (unsigned char*)mincore_array)) {
                SERROR("mincore " << path << " (" << strerror(errno) << ")");
            }
            if (verbose) {
                cout << "Done touching memory." << endl;
            }
        }

        if (verbose) {
            cout << "Checking " << pages_in_range << " pages in file " << path << endl;
        }

        // Parallel counting.
        list<thread> counters;
        atomic<size_t> total_pages_resident(0);
        const size_t thread_count = thread::hardware_concurrency();
        int64_t pages_per_thread = (pages_in_range / thread_count) + 1;
        for (size_t i = 0; i < thread_count; i++) {
            counters.emplace_back([i, &pages_per_thread, &total_pages_resident, &pages_in_range, &mincore_array]() {
                int64_t first_page = pages_per_thread * i;
                int64_t last_page = min(first_page + pages_per_thread, pages_in_range);
                int64_t page_in_range_count = 0;
                for (int64_t j = first_page; j < last_page; j++) {
                    if (is_mincore_page_resident(mincore_array[j])) {
                        page_in_range_count++;
                    }
                }
                total_pages_resident += page_in_range_count;
            });
        }
        for (auto& t : counters) {
            t.join();
        }

        double percentage_resident = ((double)total_pages_resident / (double)pages_in_range) * 100.0;
        if (verbose) {
            printf("Pages resident: %li (%.2f%%)\n", total_pages_resident.load(), percentage_resident);
        }

        free(mincore_array);
    } catch (const SException& e) {
        if (mem) {
            if (munmap(mem, len_of_file)) {
                SWARN("unable to munmap file " << path << " (" << strerror(errno) << ")");
            }
        }

        if (fd != -1) {
            close(fd);
        }
    }
}
