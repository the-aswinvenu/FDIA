#ifndef IMGFORMATLIB_AFF4_STREAM_H_
#define IMGFORMATLIB_AFF4_STREAM_H_

#include "imgformatlib/aff4/aff4_file.h"
#include "imgformatlib/aff4/aff4_image.h"
#include "imgformatlib/aff4/aff4_io.h"
#include "imgformatlib/aff4/data_store.h"
#include "imgformatlib/aff4/volume_group.h"
#include "imgformatlib/aff4/zip.h"

#include <memory>
#include <string>

namespace imgformatlib {

class Aff4Stream : public aff4::AFF4Stream {
public:
    Aff4Stream(aff4::DataStore* resolver, aff4::URN urn);
    ~Aff4Stream() override;

    aff4::AFF4Status OpenRead(const std::string& path);
    aff4::AFF4Status OpenWrite(const std::string& path, uint64_t logical_size);

    const std::string& SelectedStreamUrn() const { return selected_stream_urn_; }
    const std::string& VolumeUrn() const { return volume_urn_; }

    aff4::AFF4Status ReadBuffer(char* data, size_t* length) override;
    aff4::AFF4Status Write(const char* data, size_t length) override;
    aff4::AFF4Status Truncate() override;
    aff4::AFF4Status Seek(aff4::aff4_off_t offset, int whence) override;
    aff4::aff4_off_t Size() const override;

private:
    void ResetState();
    std::string MakeImageStreamName(const std::string& path) const;
    aff4::AFF4Status SelectBestReadableStream(aff4::URN* out_urn, uint64_t* out_size);

    aff4::MemoryDataStore aff4_resolver_;
    std::unique_ptr<aff4::VolumeGroup> read_volumes_;
    aff4::AFF4Flusher<aff4::ZipFile> zip_volume_;
    aff4::AFF4Flusher<aff4::AFF4Stream> active_stream_;

    uint64_t logical_size_ = 0;
    uint64_t current_offset_ = 0;
    bool is_writing_ = false;

    std::string selected_stream_urn_;
    std::string volume_urn_;
};

}  // namespace imgformatlib

#endif  // IMGFORMATLIB_AFF4_STREAM_H_
