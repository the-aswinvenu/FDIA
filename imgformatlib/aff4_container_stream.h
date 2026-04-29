#ifndef SRC_IMGFORMATLIB_AFF4_CONTAINER_STREAM_H_
#define SRC_IMGFORMATLIB_AFF4_CONTAINER_STREAM_H_

#include "imgformatlib/aff4/aff4_image.h"
#include "imgformatlib/aff4/volume_group.h"
#include <string>

namespace imgformatlib {

class Aff4ContainerStream : public aff4::AFF4Stream {
public:
    Aff4ContainerStream(aff4::DataStore* resolver, aff4::URN urn);
    ~Aff4ContainerStream() override;

    // Opens an existing AFF4 container and locates the logical/physical stream
    aff4::AFF4Status OpenRead(const std::vector<std::string>& paths);

    // Creates a new AFF4 container and initializes an AFF4Image stream writer
    aff4::AFF4Status OpenWrite(const std::string& path, uint64_t logical_size);

    // Reads from the uncompressed stream
    aff4::AFF4Status ReadBuffer(char* data, size_t* length) override;

    // Writes into the uncompressed stream, which is then recompressed into the container
    aff4::AFF4Status Write(const char* data, size_t length) override;

    // Stream operations
    aff4::AFF4Status Seek(aff4::aff4_off_t offset, int whence) override;
    aff4::AFF4Status Truncate() override;
    aff4::aff4_off_t Size() const override;

    // Extracts the information.turtle contents
    aff4::AFF4Status GetTurtleMetadata(std::string* out_turtle_content);
    
    // Injects the information.turtle contents before closing
    aff4::AFF4Status SetTurtleMetadata(const std::string& turtle_content);

private:
    aff4::AFF4Flusher<aff4::AFF4Stream> internal_stream_;
    aff4::VolumeGroup volumes_;
    bool is_writing_ = false;
    std::string turtle_cache_;
};

} // namespace imgformatlib

#endif // SRC_IMGFORMATLIB_AFF4_CONTAINER_STREAM_H_
