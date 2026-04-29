#include "imgformatlib/aff_stream.h"
#include <fcntl.h>
#include <iostream>

#ifdef HAVE_LIBAFFLIB
#include <afflib/afflib.h>
#else
// Mock declarations if libafflib is missing during compile, 
// though CMake should prevent this file from being compiled if not found.
typedef struct _AFFILE AFFILE;
extern "C" {
    AFFILE* af_open(const char*, int, int) { return nullptr; }
    int af_close(AFFILE*) { return -1; }
    int64_t af_get_imagesize(AFFILE*) { return -1; }
    ssize_t af_read(AFFILE*, unsigned char*, ssize_t) { return -1; }
    int af_write(AFFILE*, unsigned char*, size_t) { return -1; }
    uint64_t af_seek(AFFILE*, int64_t, int) { return -1; }
}
#endif

namespace imgformatlib {

AffStream::AffStream(aff4::DataStore* resolver, aff4::URN urn)
    : aff4::AFF4Stream(resolver, urn) {
    properties.writable = false;
    properties.seekable = true;
    properties.sizeable = true;
}

AffStream::~AffStream() {
    if (af_file_) {
        af_close(af_file_);
        af_file_ = nullptr;
    }
}

aff4::AFF4Status AffStream::OpenRead(const std::vector<std::string>& paths) {
    if (paths.empty()) {
        return aff4::GENERIC_ERROR;
    }

    af_file_ = af_open(paths[0].c_str(), O_RDONLY, 0644);
    if (!af_file_) {
        std::cerr << "Failed to open legacy AFF file: " << paths[0] << std::endl;
        return aff4::GENERIC_ERROR;
    }

    return aff4::STATUS_OK;
}

aff4::AFF4Status AffStream::OpenWrite(const std::string& path, uint64_t logical_size, uint64_t split_size) {
    // Open legacy .aff container for writing, creating it if it doesn't exist
    // O_RDWR | O_CREAT | O_TRUNC corresponds to creating a new empty file
    af_file_ = af_open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (!af_file_) {
        std::cerr << "Failed to create legacy AFF file: " << path << std::endl;
        return aff4::GENERIC_ERROR;
    }

    if (split_size > 0) {
        af_set_maxsize(af_file_, split_size);
    }

    properties.writable = true;
    return aff4::STATUS_OK;
}

aff4::AFF4Status AffStream::ReadBuffer(char* data, size_t* length) {
    if (!af_file_) {
        *length = 0;
        return aff4::GENERIC_ERROR;
    }

    ssize_t bytes_read = af_read(af_file_, reinterpret_cast<unsigned char*>(data), *length);
    if (bytes_read < 0) {
        *length = 0;
        return aff4::GENERIC_ERROR;
    }

    *length = static_cast<size_t>(bytes_read);
    return aff4::STATUS_OK;
}

aff4::AFF4Status AffStream::Write(const char* data, size_t length) {
    if (!af_file_) {
        return aff4::GENERIC_ERROR;
    }

    // af_write expects a non-const unsigned char*, but doesn't actually mutate it
    int res = af_write(af_file_, (unsigned char*)data, length);
    if (res < 0) {
        return aff4::GENERIC_ERROR;
    }

    return aff4::STATUS_OK;
}

aff4::AFF4Status AffStream::Seek(aff4::aff4_off_t offset, int whence) {
    if (!af_file_) {
        return aff4::GENERIC_ERROR;
    }

    uint64_t res = af_seek(af_file_, offset, whence);
    if (res == static_cast<uint64_t>(-1)) {
        return aff4::GENERIC_ERROR;
    }

    return aff4::STATUS_OK;
}

aff4::aff4_off_t AffStream::Size() const {
    if (!af_file_) {
        return 0;
    }

    int64_t size = af_get_imagesize(af_file_);
    if (size < 0) {
        return 0;
    }

    return static_cast<aff4::aff4_off_t>(size);
}

} // namespace imgformatlib
