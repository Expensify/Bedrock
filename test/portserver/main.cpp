#include <test/lib/PortServer.h>

#include <cstdio>
#include <exception>
#include <sys/resource.h>
#include <unistd.h>

#ifdef __linux__
#include <dirent.h>
#include <cstdlib>
#endif

int main(int argc, char* argv[])
{
    if (argc != 2) {
        fprintf(stderr, "This helper is launched automatically by PortServerClient.\n");
        return 1;
    }
    // Only the bootstrap connection (3) and singleton lock (4) belong to the helper.
    // Do not keep test-suite pipes or Bedrock sockets alive through inherited descriptors.
#ifdef __linux__
    DIR* descriptors = opendir("/proc/self/fd");
    if (!descriptors) {
        return 1;
    }
    while (const dirent* entry = readdir(descriptors)) {
        const int fd = atoi(entry->d_name);
        if (fd > 4 && fd != dirfd(descriptors)) {
            close(fd);
        }
    }
    closedir(descriptors);
#else
    rlimit limit;
    if (getrlimit(RLIMIT_NOFILE, &limit) < 0) {
        return 1;
    }
    for (rlim_t fd = 5; fd < limit.rlim_cur; ++fd) {
        close(fd);
    }
#endif
    try {
        return runTestPortServer(4, 3, argv[1]);
    } catch (const std::exception& exception) {
        fprintf(stderr, "%s\n", exception.what());
        return 1;
    }
}
