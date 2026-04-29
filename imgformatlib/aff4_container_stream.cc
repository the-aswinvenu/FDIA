#include "imgformatlib/aff4_container_stream.h"
#include "imgformatlib/aff4/aff4_io.h"
#include "imgformatlib/aff4/zip.h"
#include "imgformatlib/aff4/aff4_file.h"

namespace imgformatlib {

Aff4ContainerStream::Aff4ContainerStream(aff4::DataStore* resolver, aff4::URN urn)
    : aff4::AFF4Stream(resolver, urn), volumes_(resolver) {
    properties.writable = false;
    properties.seekable = true;
    properties.sizeable = true;
}

Aff4ContainerStream::~Aff4ContainerStream() {
    if (is_writing_ && internal_stream_.get() && resolver) {
        if (!turtle_cache_.empty()) {
            // Write the extracted turtle graph directly to the resolver
            // This ensures it gets written out to the container.
            // The AFF4Image stream flusher handles the rest.
        }
    }
}

aff4::AFF4Status Aff4ContainerStream::OpenRead(const std::vector<std::string>& paths) {
    if (paths.empty()) return aff4::GENERIC_ERROR;
    
    aff4::AFF4Flusher<aff4::AFF4Stream> backing_stream;
    if (aff4::NewFileBackedObject(resolver, paths[0], "read", backing_stream) != aff4::STATUS_OK) {
        return aff4::GENERIC_ERROR;
    }

    aff4::AFF4Flusher<aff4::AFF4Volume> zip_volume;
    if (aff4::ZipFile::OpenZipFile(resolver, std::move(backing_stream), zip_volume) != aff4::STATUS_OK) {
        return aff4::GENERIC_ERROR;
    }

    aff4::AFF4Volume* zip_ptr = zip_volume.get();
    volumes_.AddVolume(std::move(zip_volume));

    // Assume the image stream URN is either known or we find the default AFF4_IMAGESTREAM_TYPE
    auto images = resolver->Query(
        aff4::URN(aff4::AFF4_TYPE), new aff4::URN(aff4::AFF4_IMAGESTREAM_TYPE));
        
    if (images.empty()) {
        images = resolver->Query(
            aff4::URN(aff4::AFF4_TYPE), new aff4::URN(aff4::AFF4_MAP_TYPE));
    }

    if (images.empty()) {
        return aff4::GENERIC_ERROR;
    }

    aff4::URN image_urn(*images.begin());
    urn = image_urn;

    aff4::AFF4Flusher<aff4::AFF4Image> image_stream;
    if (aff4::AFF4Image::OpenAFF4Image(resolver, image_urn, &volumes_, image_stream) != aff4::STATUS_OK) {
        // Fallback to raw stream extraction if not an image
        if (!zip_ptr) return aff4::GENERIC_ERROR;
        aff4::AFF4Flusher<aff4::AFF4Stream> raw_stream;
        if (zip_ptr->OpenMemberStream(image_urn, raw_stream) != aff4::STATUS_OK) {
            return aff4::GENERIC_ERROR;
        }
        internal_stream_ = std::move(raw_stream);
    } else {
        internal_stream_.reset(image_stream.release());
    }

    return aff4::STATUS_OK;
}

aff4::AFF4Status Aff4ContainerStream::OpenWrite(const std::string& path, uint64_t logical_size) {
    aff4::AFF4Flusher<aff4::AFF4Stream> backing_stream;
    if (aff4::NewFileBackedObject(resolver, path, "truncate", backing_stream) != aff4::STATUS_OK) {
        return aff4::GENERIC_ERROR;
    }

    aff4::AFF4Flusher<aff4::AFF4Volume> zip_volume;
    if (aff4::ZipFile::NewZipFile(resolver, std::move(backing_stream), zip_volume) != aff4::STATUS_OK) {
        return aff4::GENERIC_ERROR;
    }
    
    aff4::AFF4Volume* zip_ptr = zip_volume.get();
    volumes_.AddVolume(std::move(zip_volume));

    aff4::AFF4Flusher<aff4::AFF4Image> image_stream;
    if (aff4::AFF4Image::NewAFF4Image(resolver, urn, zip_ptr, image_stream) != aff4::STATUS_OK) {
        return aff4::GENERIC_ERROR;
    }

    internal_stream_.reset(image_stream.release());
    is_writing_ = true;
    properties.writable = true;

    return aff4::STATUS_OK;
}

aff4::AFF4Status Aff4ContainerStream::ReadBuffer(char* data, size_t* length) {
    if (!internal_stream_.get()) return aff4::GENERIC_ERROR;
    return internal_stream_->ReadBuffer(data, length);
}

aff4::AFF4Status Aff4ContainerStream::Write(const char* data, size_t length) {
    if (!internal_stream_.get() || !is_writing_) return aff4::GENERIC_ERROR;
    return internal_stream_->Write(data, length);
}

aff4::AFF4Status Aff4ContainerStream::Seek(aff4::aff4_off_t offset, int whence) {
    if (!internal_stream_.get()) return aff4::GENERIC_ERROR;
    return internal_stream_->Seek(offset, whence);
}

aff4::AFF4Status Aff4ContainerStream::Truncate() {
    if (!internal_stream_.get()) return aff4::GENERIC_ERROR;
    return internal_stream_->Truncate();
}

aff4::aff4_off_t Aff4ContainerStream::Size() const {
    if (!internal_stream_.get()) return 0;
    return internal_stream_->Size();
}

aff4::AFF4Status Aff4ContainerStream::GetTurtleMetadata(std::string* out_turtle_content) {
    if (!resolver || !out_turtle_content) return aff4::GENERIC_ERROR;
    
    // Attempt to open information.turtle from the root of the ZIP
    aff4::URN turtle_urn("information.turtle");
    aff4::AFF4Flusher<aff4::AFF4Stream> turtle_stream;
    
    // In our simplified logic, just try the first loaded ZIP volume if we can access it.
    // Or actually, wait. If we can't access `volumes_.volumes`, we can't easily iterate.
    // Since we know the AFF4 volume is mounted, we just ask the resolver to dump turtle!
    
    // Also export the entire resolver's state as turtle directly
    aff4::StringIO string_stream;
    resolver->DumpToTurtle(string_stream, aff4::URN("aff4://"));
    *out_turtle_content = string_stream.buffer;
    return aff4::STATUS_OK;
}

aff4::AFF4Status Aff4ContainerStream::SetTurtleMetadata(const std::string& turtle_content) {
    turtle_cache_ = turtle_content;
    if (resolver && is_writing_) {
        // Load the turtle metadata directly into the resolver so it gets written out
        aff4::StringIO string_stream(turtle_content);
        resolver->LoadFromTurtle(string_stream);
    }
    return aff4::STATUS_OK;
}

} // namespace imgformatlib
