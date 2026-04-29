#ifndef SRC_IMGFORMATLIB_AFF_STREAM_H_
#define SRC_IMGFORMATLIB_AFF_STREAM_H_

#include "imgformatlib/aff4/aff4_io.h"
#include <string>
#include <vector>
#include <filesystem>
#include <algorithm>

// Forward declaration for AFFILE pointer to avoid including afflib.h in the header
struct _AFFILE;
typedef struct _AFFILE AFFILE;

namespace imgformatlib {

class AffStream : public aff4::AFF4Stream {
public:
    AffStream(aff4::DataStore* resolver, aff4::URN urn);
    ~AffStream() override;

    // Opens an existing legacy .aff file for reading
    aff4::AFF4Status OpenRead(const std::vector<std::string>& paths);

    // Creates a new legacy .aff file for writing
    aff4::AFF4Status OpenWrite(const std::string& path, uint64_t logical_size, uint64_t split_size = 0);

    // Reads from the uncompressed stream
    aff4::AFF4Status ReadBuffer(char* data, size_t* length) override;

    // Writes to the uncompressed stream
    aff4::AFF4Status Write(const char* data, size_t length) override;

    // Stream operations
    aff4::AFF4Status Seek(aff4::aff4_off_t offset, int whence) override;
    aff4::aff4_off_t Size() const override;

private:
    AFFILE* af_file_ = nullptr;
};

} // namespace imgformatlib

#endif // SRC_IMGFORMATLIB_AFF_STREAM_H_
