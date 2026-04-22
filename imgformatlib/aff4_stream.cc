#include "imgformatlib/aff4_stream.h"

#include "imgformatlib/aff4/lexicon.h"

#include <algorithm>
#include <filesystem>
#include <unordered_set>
#include <vector>

namespace imgformatlib {

namespace {

void AddCandidatesForType(
    aff4::DataStore* resolver,
    const char* type_urn,
    std::unordered_set<std::string>* out) {
    if (!resolver || !type_urn || !out) {
        return;
    }

    const aff4::URN type_value(type_urn);
    const auto subjects = resolver->Query(aff4::URN(aff4::AFF4_TYPE), &type_value);
    for (const auto& urn : subjects) {
        out->insert(urn.SerializeToString());
    }
}

}  // namespace

Aff4Stream::Aff4Stream(aff4::DataStore* resolver, aff4::URN urn)
    : aff4::AFF4Stream(resolver, urn),
      read_volumes_(new aff4::VolumeGroup(&aff4_resolver_)) {
    properties.seekable = true;
    properties.sizeable = true;
}

Aff4Stream::~Aff4Stream() {
    ResetState();
}

void Aff4Stream::ResetState() {
    active_stream_.reset();
    zip_volume_.reset();
    read_volumes_.reset();
    aff4_resolver_.Clear();
    read_volumes_.reset(new aff4::VolumeGroup(&aff4_resolver_));

    logical_size_ = 0;
    current_offset_ = 0;
    is_writing_ = false;
    selected_stream_urn_.clear();
    volume_urn_.clear();

    readptr = 0;
    size = 0;
    properties.writable = false;
}

std::string Aff4Stream::MakeImageStreamName(const std::string& path) const {
    const std::string stem = std::filesystem::path(path).stem().string();
    if (stem.empty()) {
        return "image";
    }

    std::string out;
    out.reserve(stem.size());
    for (char c : stem) {
        if ((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
            (c >= '0' && c <= '9') || c == '_' || c == '-') {
            out.push_back(c);
        } else {
            out.push_back('_');
        }
    }

    if (out.empty()) {
        return "image";
    }
    return out;
}

aff4::AFF4Status Aff4Stream::SelectBestReadableStream(aff4::URN* out_urn, uint64_t* out_size) {
    if (!read_volumes_ || !out_urn || !out_size) {
        return aff4::GENERIC_ERROR;
    }

    std::unordered_set<std::string> candidates;
    AddCandidatesForType(&aff4_resolver_, aff4::AFF4_IMAGE_TYPE, &candidates);
    AddCandidatesForType(&aff4_resolver_, aff4::AFF4_DISK_IMAGE_TYPE, &candidates);
    AddCandidatesForType(&aff4_resolver_, aff4::AFF4_VOLUME_IMAGE_TYPE, &candidates);
    AddCandidatesForType(&aff4_resolver_, aff4::AFF4_MEMORY_IMAGE_TYPE, &candidates);
    AddCandidatesForType(&aff4_resolver_, aff4::AFF4_CONTIGUOUS_IMAGE_TYPE, &candidates);
    AddCandidatesForType(&aff4_resolver_, aff4::AFF4_DISCONTIGUOUS_IMAGE_TYPE, &candidates);
    AddCandidatesForType(&aff4_resolver_, aff4::AFF4_IMAGESTREAM_TYPE, &candidates);
    AddCandidatesForType(&aff4_resolver_, aff4::AFF4_LEGACY_IMAGESTREAM_TYPE, &candidates);

    if (candidates.empty()) {
        return aff4::GENERIC_ERROR;
    }

    std::vector<std::string> ordered_candidates(candidates.begin(), candidates.end());
    std::sort(ordered_candidates.begin(), ordered_candidates.end());

    bool found = false;
    uint64_t best_size = 0;
    std::string best_urn;

    for (const auto& candidate : ordered_candidates) {
        aff4::AFF4Flusher<aff4::AFF4Stream> stream;
        if (read_volumes_->GetStream(aff4::URN(candidate), stream) != aff4::STATUS_OK) {
            continue;
        }

        const aff4::aff4_off_t raw_size = stream->Size();
        const uint64_t stream_size = raw_size < 0 ? 0 : static_cast<uint64_t>(raw_size);

        if (!found || stream_size > best_size ||
            (stream_size == best_size && candidate < best_urn)) {
            found = true;
            best_size = stream_size;
            best_urn = candidate;
        }
    }

    if (!found) {
        return aff4::GENERIC_ERROR;
    }

    *out_urn = aff4::URN(best_urn);
    *out_size = best_size;
    return aff4::STATUS_OK;
}

aff4::AFF4Status Aff4Stream::OpenRead(const std::string& path) {
    ResetState();

    aff4::AFF4Flusher<aff4::FileBackedObject> backing_file;
    if (aff4::NewFileBackedObject(&aff4_resolver_, path, "read", backing_file) != aff4::STATUS_OK) {
        return aff4::GENERIC_ERROR;
    }

    aff4::AFF4Flusher<aff4::ZipFile> zip;
    if (aff4::ZipFile::OpenZipFile(
            &aff4_resolver_,
            aff4::AFF4Flusher<aff4::AFF4Stream>(backing_file.release()),
            zip) != aff4::STATUS_OK) {
        return aff4::GENERIC_ERROR;
    }

    volume_urn_ = zip->urn.SerializeToString();
    read_volumes_->AddVolume(aff4::AFF4Flusher<aff4::AFF4Volume>(zip.release()));

    aff4::URN selected_urn;
    uint64_t selected_size = 0;
    if (SelectBestReadableStream(&selected_urn, &selected_size) != aff4::STATUS_OK) {
        return aff4::GENERIC_ERROR;
    }

    if (read_volumes_->GetStream(selected_urn, active_stream_) != aff4::STATUS_OK) {
        return aff4::GENERIC_ERROR;
    }

    selected_stream_urn_ = selected_urn.SerializeToString();
    logical_size_ = selected_size;
    current_offset_ = 0;
    readptr = 0;
    size = static_cast<aff4::aff4_off_t>(logical_size_);
    is_writing_ = false;
    properties.writable = false;

    return aff4::STATUS_OK;
}

aff4::AFF4Status Aff4Stream::OpenWrite(const std::string& path, uint64_t /*logical_size*/) {
    ResetState();

    aff4::AFF4Flusher<aff4::FileBackedObject> backing_file;
    if (aff4::NewFileBackedObject(&aff4_resolver_, path, "truncate", backing_file) != aff4::STATUS_OK) {
        return aff4::GENERIC_ERROR;
    }

    if (aff4::ZipFile::NewZipFile(
            &aff4_resolver_,
            aff4::AFF4Flusher<aff4::AFF4Stream>(backing_file.release()),
            zip_volume_) != aff4::STATUS_OK) {
        return aff4::GENERIC_ERROR;
    }

    volume_urn_ = zip_volume_->urn.SerializeToString();

    aff4::AFF4Flusher<aff4::AFF4Image> image;
    const aff4::URN image_urn = zip_volume_->urn.Append(MakeImageStreamName(path));
    if (aff4::AFF4Image::NewAFF4Image(&aff4_resolver_, image_urn, zip_volume_.get(), image) != aff4::STATUS_OK) {
        return aff4::GENERIC_ERROR;
    }

    image->compression = aff4::AFF4_IMAGE_COMPRESSION_ENUM_DEFLATE;
    selected_stream_urn_ = image_urn.SerializeToString();
    active_stream_ = aff4::AFF4Flusher<aff4::AFF4Stream>(image.release());

    logical_size_ = 0;
    current_offset_ = 0;
    readptr = 0;
    size = 0;
    is_writing_ = true;
    properties.writable = true;

    return aff4::STATUS_OK;
}

aff4::AFF4Status Aff4Stream::ReadBuffer(char* data, size_t* length) {
    if (!data || !length || !active_stream_ || is_writing_) {
        if (length) {
            *length = 0;
        }
        return aff4::GENERIC_ERROR;
    }

    const aff4::AFF4Status status = active_stream_->ReadBuffer(data, length);
    if (status != aff4::STATUS_OK) {
        return status;
    }

    current_offset_ = static_cast<uint64_t>(active_stream_->Tell());
    readptr = static_cast<aff4::aff4_off_t>(current_offset_);
    return aff4::STATUS_OK;
}

aff4::AFF4Status Aff4Stream::Write(const char* data, size_t length) {
    if (!data || !active_stream_ || !is_writing_) {
        return aff4::GENERIC_ERROR;
    }

    const aff4::AFF4Status status = active_stream_->Write(data, length);
    if (status != aff4::STATUS_OK) {
        return status;
    }

    current_offset_ += static_cast<uint64_t>(length);
    logical_size_ = std::max(logical_size_, current_offset_);
    readptr = static_cast<aff4::aff4_off_t>(current_offset_);
    size = static_cast<aff4::aff4_off_t>(logical_size_);
    return aff4::STATUS_OK;
}

aff4::AFF4Status Aff4Stream::Seek(aff4::aff4_off_t offset, int whence) {
    if (!active_stream_) {
        return aff4::GENERIC_ERROR;
    }

    const aff4::AFF4Status status = active_stream_->Seek(offset, whence);
    if (status != aff4::STATUS_OK) {
        return status;
    }

    current_offset_ = static_cast<uint64_t>(active_stream_->Tell());
    readptr = static_cast<aff4::aff4_off_t>(current_offset_);
    return aff4::STATUS_OK;
}

aff4::AFF4Status Aff4Stream::Truncate() {
    return aff4::NOT_IMPLEMENTED;
}

aff4::aff4_off_t Aff4Stream::Size() const {
    return static_cast<aff4::aff4_off_t>(logical_size_);
}

}  // namespace imgformatlib
