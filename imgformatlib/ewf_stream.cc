#include "imgformatlib/ewf_stream.h"
#include <iostream>
#include <vector>

namespace imgformatlib {

EwfStream::EwfStream(aff4::DataStore* resolver, aff4::URN urn) 
    : aff4::AFF4Stream(resolver, urn) {}

EwfStream::~EwfStream() {
    if (ewf_handle_) {
        libewf_error_t* error = nullptr;
        if (is_writing_) {
            libewf_handle_write_finalize(ewf_handle_, &error);
        }
        libewf_handle_close(ewf_handle_, &error);
        libewf_handle_free(&ewf_handle_, &error);
        if (error) {
            libewf_error_free(&error);
        }
    }
}

std::vector<std::string> EwfStream::GlobSegments(const std::string& base_file) {
    std::vector<std::string> result;
    char** filenames = nullptr;
    int num_filenames = 0;
    libewf_error_t* error = nullptr;

    if (libewf_glob(base_file.c_str(), base_file.length(), LIBEWF_FORMAT_UNKNOWN, &filenames, &num_filenames, &error) == 1) {
        for (int i = 0; i < num_filenames; ++i) {
            if (filenames[i]) {
                result.push_back(std::string(filenames[i]));
            }
        }
        libewf_glob_free(filenames, num_filenames, &error);
    } else {
        if (error) {
            libewf_error_free(&error);
        }
        result.push_back(base_file);
    }
    return result;
}

aff4::AFF4Status EwfStream::OpenRead(const std::vector<std::string>& paths) {
    if (paths.empty()) return aff4::GENERIC_ERROR;

    libewf_error_t* error = nullptr;
    if (libewf_handle_initialize(&ewf_handle_, &error) != 1) {
        if (error) libewf_error_free(&error);
        return aff4::GENERIC_ERROR;
    }

    std::vector<char*> c_filenames;
    for (const auto& path : paths) {
        c_filenames.push_back(const_cast<char*>(path.c_str()));
    }

    if (libewf_handle_open(ewf_handle_, c_filenames.data(), c_filenames.size(), LIBEWF_OPEN_READ, &error) != 1) {
        if (error) libewf_error_free(&error);
        return aff4::GENERIC_ERROR;
    }

    size64_t size = 0;
    if (libewf_handle_get_media_size(ewf_handle_, &size, &error) != 1) {
        if (error) libewf_error_free(&error);
        return aff4::GENERIC_ERROR;
    }
    logical_size_ = static_cast<uint64_t>(size);
    current_offset_ = 0;
    is_writing_ = false;
    properties.seekable = true;  // libewf_handle_read_buffer_at_offset supports arbitrary offsets
    properties.sizeable = true;

    return aff4::STATUS_OK;
}

aff4::AFF4Status EwfStream::OpenWrite(const std::string& path, uint64_t logical_size) {
    libewf_error_t* error = nullptr;
    if (libewf_handle_initialize(&ewf_handle_, &error) != 1) {
        if (error) libewf_error_free(&error);
        return aff4::GENERIC_ERROR;
    }

    char* filenames[1];
    filenames[0] = const_cast<char*>(path.c_str());
    if (libewf_handle_open(ewf_handle_, filenames, 1, LIBEWF_OPEN_WRITE, &error) != 1) {
        if (error) libewf_error_free(&error);
        return aff4::GENERIC_ERROR;
    }

    // EWF streams default to EnCase6 formatting.
    uint8_t ewf_format = LIBEWF_FORMAT_ENCASE6;

    // Format MUST be set before segment size and compression (per libewf docs)
    if (libewf_handle_set_format(ewf_handle_, ewf_format, &error) != 1) {
        if (error) libewf_error_free(&error);
        return aff4::GENERIC_ERROR;
    }
    if (libewf_handle_set_bytes_per_sector(ewf_handle_, 512, &error) != 1) {
        if (error) libewf_error_free(&error);
        return aff4::GENERIC_ERROR;
    }
    if (libewf_handle_set_sectors_per_chunk(ewf_handle_, 64, &error) != 1) {
        if (error) libewf_error_free(&error);
        return aff4::GENERIC_ERROR;
    }
    if (logical_size > 0) {
        if (libewf_handle_set_media_size(ewf_handle_, logical_size, &error) != 1) {
            if (error) libewf_error_free(&error);
            return aff4::GENERIC_ERROR;
        }
    }

    logical_size_ = 0;
    current_offset_ = 0;
    is_writing_ = true;
    return aff4::STATUS_OK;
}

std::string EwfStream::FindInfoTxt(const std::string& base_file) {
    std::filesystem::path p(base_file);
    std::filesystem::path txt = p.parent_path() / (p.stem().string() + ".txt");
    if (std::filesystem::exists(txt)) {
        return txt.string();
    }
    return "";
}

aff4::AFF4Status EwfStream::ReadBuffer(char* data, size_t* length) {
    if (!ewf_handle_ || is_writing_) {
        *length = 0;
        return aff4::GENERIC_ERROR;
    }

    if (current_offset_ >= logical_size_) {
        *length = 0;
        return aff4::STATUS_OK; // EOF
    }

    libewf_error_t* error = nullptr;
    size_t to_read = *length;
    if (current_offset_ + to_read > logical_size_) {
        to_read = logical_size_ - current_offset_;
    }

    ssize_t bytes_read = libewf_handle_read_buffer_at_offset(
        ewf_handle_, data, to_read, current_offset_, &error);

    if (bytes_read < 0) {
        if (error) libewf_error_free(&error);
        *length = 0;
        return aff4::GENERIC_ERROR;
    }

    *length = static_cast<size_t>(bytes_read);
    current_offset_ += *length;
    return aff4::STATUS_OK;
}

aff4::AFF4Status EwfStream::Write(const char* data, size_t length) {
    if (!ewf_handle_ || !is_writing_) {
        return aff4::GENERIC_ERROR;
    }

    libewf_error_t* error = nullptr;
    ssize_t bytes_written = libewf_handle_write_buffer(
        ewf_handle_, data, length, &error);

    if (bytes_written < 0 || static_cast<size_t>(bytes_written) != length) {
        if (error) libewf_error_free(&error);
        return aff4::GENERIC_ERROR;
    }

    logical_size_ += length;
    return aff4::STATUS_OK;
}

aff4::AFF4Status EwfStream::Seek(aff4::aff4_off_t offset, int whence) {
    aff4::AFF4Status status = aff4::AFF4Stream::Seek(offset, whence);
    if (status != aff4::STATUS_OK) return status;
    // Sync our own offset tracker with the base class readptr.
    current_offset_ = static_cast<uint64_t>(readptr);
    return aff4::STATUS_OK;
}

aff4::AFF4Status EwfStream::Truncate() {
    return aff4::NOT_IMPLEMENTED;
}

aff4::aff4_off_t EwfStream::Size() const {
    return static_cast<aff4::aff4_off_t>(logical_size_);
}

} // namespace imgformatlib
