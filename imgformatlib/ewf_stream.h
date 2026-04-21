#ifndef IMGFORMATLIB_EWF_STREAM_H_
#define IMGFORMATLIB_EWF_STREAM_H_

#include "imgformatlib/aff4/aff4_io.h"
#include "imgformatlib/aff4/aff4_base.h"
#include <libewf.h>
#include <string>
#include <vector>
#include <filesystem>

namespace imgformatlib {

class EwfStream : public aff4::AFF4Stream {
public:
    EwfStream(aff4::DataStore* resolver, aff4::URN urn);
    virtual ~EwfStream();

    static std::vector<std::string> GlobSegments(const std::string& base_file);
    static std::string FindInfoTxt(const std::string& base_file);

    aff4::AFF4Status OpenRead(const std::vector<std::string>& paths);
    aff4::AFF4Status OpenWrite(const std::string& path, uint64_t logical_size);

    // AFF4Stream overrides
    virtual aff4::AFF4Status ReadBuffer(char* data, size_t* length) override;
    virtual aff4::AFF4Status Write(const char* data, size_t length) override;
    virtual aff4::AFF4Status Truncate() override;
    virtual aff4::AFF4Status Seek(aff4::aff4_off_t offset, int whence) override;
    virtual aff4::aff4_off_t Size() const override;

private:
    libewf_handle_t* ewf_handle_ = nullptr;
    uint64_t logical_size_ = 0;
    uint64_t current_offset_ = 0;
    bool is_writing_ = false;
};

} // namespace imgformatlib

#endif // IMGFORMATLIB_EWF_STREAM_H_
