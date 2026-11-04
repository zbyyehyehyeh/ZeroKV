#pragma once
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <string>
#include "Status.h"

namespace FileUtils {

inline Status WriteFull(int fd, const void* buf, size_t count) {
    const char* ptr = static_cast<const char*>(buf);
    while (count > 0) {
        ssize_t written = ::write(fd, ptr, count);
        if (written < 0) {
            if (errno == EINTR) continue; 
            return Status::IOError(std::string("write failed: ") + strerror(errno));
        }
        ptr += written;
        count -= written;
    }
    return Status::OK();
}

inline Status PwriteFull(int fd, const void* buf, size_t count, off_t offset) {
    const char* ptr = static_cast<const char*>(buf);
    while (count > 0) {
        ssize_t written = ::pwrite(fd, ptr, count, offset);
        if (written < 0) {
            if (errno == EINTR) continue;
            return Status::IOError(std::string("pwrite failed: ") + strerror(errno));
        }
        ptr += written;
        count -= written;
        offset += written;
    }
    return Status::OK();
}

inline Status SyncFile(int fd) {
    if (::fdatasync(fd) < 0) {
        return Status::IOError(std::string("fdatasync failed: ") + strerror(errno));
    }
    return Status::OK();
}

} // namespace FileUtils