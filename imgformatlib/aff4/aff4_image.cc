/*
Copyright 2014 Google Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License.  You may obtain a copy of the
License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied.  See the License for the
specific language governing permissions and limitations under the License.
*/

#include "imgformatlib/aff4/lexicon.h"
#include "imgformatlib/aff4/aff4_image.h"
#include "imgformatlib/aff4/aff4_hash.h"
#include "imgformatlib/aff4/libaff4.h"
#include <zlib.h>
#include <snappy.h>
#include <lz4.h>
#include "imgformatlib/aff4/aff4_utils.h"
#include "imgformatlib/aff4/volume_group.h"

namespace aff4 {


AFF4Status CompressZlib_(const std::string &input, std::string* output) {
    uLongf c_length = compressBound(input.size()) + 1;
    output->resize(c_length);

    if (compress2(reinterpret_cast<Bytef*>(&(*output)[0]),
                  &c_length,
                  reinterpret_cast<Bytef*>(const_cast<char*>(input.data())),
                  input.size(), 1) != Z_OK) {
        return MEMORY_ERROR;
    }

    output->resize(c_length);

    return STATUS_OK;
}


AFF4Status DeCompressZlib_(const std::string &input, std::string* output) {
    uLongf buffer_size = output->size();

    if (uncompress(reinterpret_cast<Bytef*>(&(*output)[0]),
                   &buffer_size,
                   (const Bytef*)input.data(), input.size()) == Z_OK) {
        output->resize(buffer_size);
        return STATUS_OK;
    }

    return IO_ERROR;
}


AFF4Status CompressDeflate_(const std::string &input, std::string* output) {
    z_stream zs{};
    // Use raw deflate format (no zlib header) with -15 window bits
    if (deflateInit2(&zs, Z_DEFAULT_COMPRESSION, Z_DEFLATED, -15,
                     8, Z_DEFAULT_STRATEGY) != Z_OK) {
        return MEMORY_ERROR;
    }

    zs.next_in = (Bytef *) input.c_str();
    zs.avail_in = input.length();

    auto size = deflateBound(&zs, input.length());

    // Allocate space for compressed output
    output->resize(size);

    zs.avail_out = size;
    zs.next_out = reinterpret_cast<Bytef*>(&(*output)[0]);

    int ret = deflate(&zs, Z_FINISH);
    if (ret != Z_STREAM_END) {
        deflateEnd(&zs);
        return IO_ERROR;
    }

    // Resize output to actual compressed size
    output->resize(zs.total_out);
    deflateEnd(&zs);

    return STATUS_OK;
}


AFF4Status DeCompressDeflate_(const std::string &input, std::string* output) {
    z_stream zs{};
    // Use raw deflate format (no zlib header) with -15 window bits
    if (inflateInit2(&zs, -15) != Z_OK) {
        return MEMORY_ERROR;
    }

    // Use the pre-allocated buffer size as output capacity
    size_t buffer_size = output->size();

    zs.next_in = (Bytef *) input.c_str();
    zs.avail_in = input.length();
    zs.avail_out = buffer_size;
    zs.next_out = reinterpret_cast<Bytef*>(&(*output)[0]);

    int ret = inflate(&zs, Z_FINISH);
    
    output->resize(zs.total_out);
    inflateEnd(&zs);

    return (ret == Z_STREAM_END) ? STATUS_OK : IO_ERROR;
}


AFF4Status CompressSnappy_(const std::string &input, std::string* output) {
    snappy::Compress(input.data(), input.size(), output);

    return STATUS_OK;
}


AFF4Status DeCompressSnappy_(const std::string &input, std::string* output) {
    if (!snappy::Uncompress(input.data(), input.size(), output)) {
        return GENERIC_ERROR;
    }

    return STATUS_OK;
}

AFF4Status CompressLZ4_(const std::string &input, std::string* output) {
    output->resize(LZ4_compressBound(input.size()));

    int size = LZ4_compress_default(input.data(), &(*output)[0],
                                    input.size(), output->size());
    if (size == 0) {
        return GENERIC_ERROR;
    }

    output->resize(size);

    return STATUS_OK;
}


AFF4Status DeCompressLZ4_(const std::string &input, std::string* output) {
    int size = LZ4_decompress_safe(input.data(), &(*output)[0],
                                   input.size(), output->size());
    if (size == 0) {
        return GENERIC_ERROR;
    }

    output->resize(size);

    return STATUS_OK;
}


class _BevyWriter {
public:
    _BevyWriter(DataStore *resolver,
                AFF4_IMAGE_COMPRESSION_ENUM compression,
                size_t chunk_size, int chunks_per_segment)
        : resolver(resolver),
          compression(compression), chunk_size(chunk_size),
          bevy_index_data(chunks_per_segment + 1),
          chunks_per_segment(chunks_per_segment) {
        bevy.buffer.reserve(chunk_size * chunks_per_segment);
    }

    AFF4Stream &bevy_stream() {
        return bevy;
    }

    // Generate the index stream.
    std::string index_stream() {
        std::unique_lock<std::mutex> lock(mutex);
        std::string result;
        result.reserve(chunks_per_segment * sizeof(BevyIndex));

        for(const BevyIndex& index_entry: bevy_index_data) {
            // The first empty chunk means the end of the chunks. This
            // happens if the bevy is not full.
            if (index_entry.length == 0) break;

            result += std::string(
                reinterpret_cast<const char *>(&index_entry),
                sizeof(index_entry));
        }

        return result;
    }

    // Receives a single chunk's data. The chunk is compressed on the
    // thread pool and concatenated to the bevy. NOTE: Since this is
    // done asyncronously, the chunks are not stored in the bevy
    // contiguously. Instead, the index is sorted by chunk id and
    // refer to chunks in the bevy in any order.
    void EnqueueCompressChunk(int chunk_id, const std::string& data) {
        AFF4Status status = _CompressChunk(chunk_id, data);

        std::unique_lock<std::mutex> lock(mutex);
        results.push_back(status);
    }

    int chunks_written() {
        std::unique_lock<std::mutex> lock(mutex);
        return chunks_written_;
    }

    AFF4Status Finalize() {
        std::unique_lock<std::mutex> lock(mutex);

        for (auto& status: results) {
            RETURN_IF_ERROR(status);
        }
        results.clear();
        return STATUS_OK;
    }

private:
    std::mutex mutex;        // Protects the result vector.
    std::mutex bevy_mutex;   // Protects writing on the bevy.
    StringIO bevy;
    DataStore *resolver;
    AFF4_IMAGE_COMPRESSION_ENUM compression;
    size_t chunk_size;
    std::vector<BevyIndex> bevy_index_data;
    size_t chunks_per_segment;

    // A counter of how many chunks were written.
    int chunks_written_ = 0;

    std::vector<AFF4Status> results;

    AFF4Status _CompressChunk(int chunk_id, const std::string data) {
        std::string c_data;

        switch (compression) {
        case AFF4_IMAGE_COMPRESSION_ENUM_ZLIB: {
            RETURN_IF_ERROR(CompressZlib_(data, &c_data));
        }
            break;

        case AFF4_IMAGE_COMPRESSION_ENUM_DEFLATE: {
            RETURN_IF_ERROR(CompressDeflate_(data, &c_data));
        }
            break;

        case AFF4_IMAGE_COMPRESSION_ENUM_SNAPPY: {
            RETURN_IF_ERROR(CompressSnappy_(data, &c_data));
        }
            break;

        case AFF4_IMAGE_COMPRESSION_ENUM_LZ4: {
            RETURN_IF_ERROR(CompressLZ4_(data, &c_data));
        }
            break;

        case AFF4_IMAGE_COMPRESSION_ENUM_STORED: {
            c_data = data;
        }
            break;

        // Should never happen because the object should never accept this
        // compression URN.
        default:
            resolver->logger->critical("Unexpected compression type set {}",
                                       (int)compression);
            return NOT_IMPLEMENTED;
        }

        RETURN_IF_ERROR(WriteChunk(data, c_data, chunk_id));
        return STATUS_OK;
    }


    AFF4Status WriteChunk(const std::string &data,
                          const std::string &c_data,
                          uint32_t chunk_id) {
        if (chunk_id > chunks_per_segment) {
            return IO_ERROR;
        }

        std::unique_lock<std::mutex> lock(bevy_mutex);

        BevyIndex &index = bevy_index_data[chunk_id];
        index.offset = bevy.Tell();

        // If by attempting to compress the chunk, we actually made it
        // bigger, we just store the chunk uncompressed. The
        // decompressor can figure that this is uncompressed by
        // comparing the chunk size to the compressed chunk size.

        // 3.2 Compression Storage of uncompressed chunks is supported
        //    by thise simple principle that if len(chunk) ==
        //    aff4:chunk_size then it is a stored chunk. Compression
        //    is not applied to stored chunks.

        // Chunk is compressible - store it compressed.
        if (c_data.size() < chunk_size - 16) {
            index.length = c_data.size();
            RETURN_IF_ERROR(bevy.Write(c_data));

            // Chunk is not compressible enough, store it uncompressed.
        } else {
            index.length = data.size();
            RETURN_IF_ERROR(bevy.Write(data));
        }
        chunks_written_++;

        return STATUS_OK;
    }
};


void _BevyWriterDeleter::operator()(_BevyWriter *p) { delete p; }


// A private class which manages image stream.
class _CompressorStream: public AFF4Stream {
  private:
    AFF4Stream* source;

    aff4_off_t initial_offset;
    size_t chunk_size;
    int chunks_per_segment;

    // Optional hasher for computing image hash
    MultiHasher* hasher_ = nullptr;

  public:
    _BevyWriter bevy_writer;

    _CompressorStream(DataStore* resolver,
                      AFF4_IMAGE_COMPRESSION_ENUM compression,
                      size_t chunk_size,
                      int chunks_per_segment, AFF4Stream* source,
                      MultiHasher* hasher = nullptr):
        AFF4Stream(resolver), source(source), initial_offset(source->Tell()),
        chunk_size(chunk_size), chunks_per_segment(chunks_per_segment),
        hasher_(hasher),
        bevy_writer(resolver, compression, chunk_size,
                    chunks_per_segment) {}

    // Populate the entire bevy into the writer at once.
    AFF4Status PrepareBevy() {
        for (int chunk_id = 0 ; chunk_id < chunks_per_segment; chunk_id++) {
            const std::string data = source->Read(chunk_size);

            // Ran out of source data - we are done early.
            if (data.size() == 0) {
                break;
            }

            // Update hash with uncompressed data if hasher is provided
            if (hasher_) {
                RETURN_IF_ERROR(hasher_->Update(data.data(), data.size()));
            }

            size += data.size();
            bevy_writer.EnqueueCompressChunk(chunk_id, data);
        }
        RETURN_IF_ERROR(bevy_writer.Finalize());
        return bevy_writer.bevy_stream().Seek(0, SEEK_SET);
    }

    // The progress reporting calls us to figure out how much work we
    // actually did. Since we compress asynchronously, a better
    // measure of how far along we are is to report how many chunks we
    // finished compressing right now.
    aff4_off_t Tell() override {
        return initial_offset + bevy_writer.chunks_written() * chunk_size;
    };


    // This stream returns an entire compressed bevy from any read
    // request.  We just read a full bevu worth of chunks from our
    // source stream, compress it and return the compressed data. On
    // the next read, we return an empty string which indicates the
    // bevy is complete. This allows us to seamlessly hook this class
    // into WriteStream() calls into the Bevy segment.
    std::string Read(size_t length) override {
        auto& bevy_stream = bevy_writer.bevy_stream();
        return bevy_stream.Read(length);
    };

    virtual ~_CompressorStream() {}
};


AFF4Status AFF4Image::OpenAFF4Image(
    DataStore* resolver,
    URN image_urn,
    VolumeGroup *volumes,
    AFF4Flusher<AFF4Image> &result) {

    auto new_obj = make_flusher<AFF4Image>(resolver);
    new_obj->urn.Set(image_urn);
    new_obj->volumes = volumes;

    // Determine if this is an AFF4:ImageStream (AFF4 Standard) or
    // a aff4:stream (AFF4 Legacy)
    new_obj->isAFF4Legacy = resolver->HasURNWithAttributeAndValue(
        new_obj->urn, AFF4_TYPE, URN(AFF4_LEGACY_IMAGESTREAM_TYPE));

    // Configure the stream parameters.
    XSDInteger value;

    if(!new_obj->isAFF4Legacy){
        // AFF4 Standard
        if (resolver->Get(new_obj->urn, AFF4_STREAM_CHUNK_SIZE, value) == STATUS_OK) {
            new_obj->chunk_size = value.value;
        }

        if (resolver->Get(new_obj->urn, AFF4_STREAM_CHUNKS_PER_SEGMENT, value) == STATUS_OK) {
            new_obj->chunks_per_segment = value.value;
        }

        if (resolver->Get(new_obj->urn, AFF4_STREAM_SIZE, value) == STATUS_OK) {
            new_obj->size = value.value;

        } else {
            resolver->logger->error(
                "ImageStream {} does not specify a size. "
                "Is this part of a split image set?", new_obj->urn.SerializeToString());
        }

    } else {
        // AFF4 Legacy
        if (resolver->Get(new_obj->urn, AFF4_LEGACY_STREAM_CHUNK_SIZE, value) == STATUS_OK) {
            new_obj->chunk_size = value.value;
        }

        if (resolver->Get(new_obj->urn, AFF4_LEGACY_STREAM_CHUNKS_PER_SEGMENT, value) == STATUS_OK) {
            new_obj->chunks_per_segment = value.value;
        }

        if (resolver->Get(new_obj->urn, AFF4_LEGACY_STREAM_SIZE, value) == STATUS_OK) {
            new_obj->size = value.value;
        }
    }

    // By default we cache 32 MiB of bevys
    new_obj->chunk_cache_size = (32 * 1024 * 1024) / new_obj->chunk_size;

    // Load the compression scheme. If it is not set we just default to ZLIB.
    URN compression_urn;
    if (STATUS_OK == resolver->Get(
            new_obj->urn, AFF4_IMAGE_COMPRESSION, compression_urn)) {
        new_obj->compression = CompressionMethodFromURN(compression_urn);
        if (new_obj->compression == AFF4_IMAGE_COMPRESSION_ENUM_UNKNOWN) {
            resolver->logger->error(
                "Compression method {} is not supported by this implementation.",
                compression_urn.SerializeToString());
            return NOT_IMPLEMENTED;
        }

    } else if (STATUS_OK == resolver->Get(
                   new_obj->urn, AFF4_LEGACY_IMAGE_COMPRESSION, compression_urn)) {
        new_obj->compression = CompressionMethodFromURN(compression_urn);
        if (new_obj->compression == AFF4_IMAGE_COMPRESSION_ENUM_UNKNOWN) {
            resolver->logger->error(
                "Compression method {} is not supported by this implementation.",
                compression_urn.SerializeToString());
            return NOT_IMPLEMENTED;
        }
    }

    result = std::move(new_obj);

    return STATUS_OK;
}

AFF4Status AFF4Image::NewAFF4Image(
    DataStore* resolver,
    URN image_urn,
    AFF4Volume *volume,
    AFF4Flusher<AFF4Stream> &result) {

    AFF4Flusher<AFF4Image> image;
    RETURN_IF_ERROR(
        AFF4Image::NewAFF4Image(
            resolver, image_urn, volume, image));

    result = std::move(image);

    return STATUS_OK;
}


AFF4Status AFF4Image::NewAFF4Image(
    DataStore* resolver,
    URN image_urn,
    AFF4Volume *volume,
    AFF4Flusher<AFF4Image> &result) {

    auto new_obj = make_flusher<AFF4Image>(resolver);
    new_obj->urn = image_urn;
    new_obj->current_volume = volume;

    resolver->Set(image_urn, AFF4_TYPE, new URN(AFF4_IMAGESTREAM_TYPE),
                  /* replace = */ false);
    resolver->Set(image_urn, AFF4_STORED, new URN(volume->urn));
    if(!resolver->HasURNWithAttribute(image_urn, AFF4_STREAM_SIZE)) {
        resolver->Set(image_urn, AFF4_STREAM_SIZE, new XSDInteger((uint64_t)0));
    }

    result = std::move(new_obj);

    return STATUS_OK;
}

// Bevy is full - flush it to the image and start the next one. Takes
// and disposes of the bevy_writer. Next Write() will make a new
// writer.
AFF4Status AFF4Image::FlushBevy() {
    RETURN_IF_ERROR(bevy_writer->Finalize());

    AFF4Stream &bevy_stream = bevy_writer->bevy_stream();
    RETURN_IF_ERROR(bevy_stream.Seek(0, SEEK_SET));

    // If the bevy is empty nothing else to do.
    if (bevy_stream.Size() == 0) {
        return STATUS_OK;
    }

    URN bevy_urn(urn.Append(aff4_sprintf("%08d", bevy_number)));
    URN bevy_index_urn(bevy_urn.value +(".index"));

    // Create the new segments in this zip file.
    AFF4Flusher<AFF4Stream> bevy_index_member;
    RETURN_IF_ERROR(current_volume->CreateMemberStream(
                        bevy_index_urn, bevy_index_member));

    AFF4Flusher<AFF4Stream> bevy_member;
    RETURN_IF_ERROR(current_volume->CreateMemberStream(bevy_urn, bevy_member));

    std::string index_stream;
    bevy_index_member->reserve(chunks_per_segment * sizeof(BevyIndex));
    RETURN_IF_ERROR(bevy_index_member->Write(
                        bevy_writer->index_stream()));

    ProgressContext empty_progress(resolver);
    bevy_index_member->reserve(chunks_per_segment * chunk_size);
    RETURN_IF_ERROR(bevy_member->WriteStream(&bevy_stream, &empty_progress));

    // Done with this bevy - make a new writer.
    bevy_writer.reset(new _BevyWriter(
                          resolver, compression, chunk_size,
                          chunks_per_segment));
    bevy_number++;
    chunk_count_in_bevy = 0;

    return STATUS_OK;
}


AFF4Status AFF4Image::Write(const char* data, size_t length) {
    if (length <= 0) return STATUS_OK;

    // Prepare a bevy writer to collect the first bevy.
    if (bevy_writer == nullptr) {
        bevy_writer.reset(new _BevyWriter(resolver, compression, chunk_size,
                                          chunks_per_segment));
    }

    // This object is now dirty.
    MarkDirty();

    // Update hash with incoming data if hashing is enabled
    if (hashing_enabled_ && image_hasher_) {
        RETURN_IF_ERROR(image_hasher_->Update(data, length));
    }

    // Append small reads to the buffer.
    buffer.append(data, length);
    size_t offset = 0;

    // Consume full chunks from the buffer and send them to the bevy
    // writer.
    while (buffer.size() - offset >= chunk_size) {
        bevy_writer->EnqueueCompressChunk(
            chunk_count_in_bevy,
            buffer.substr(offset, chunk_size));

        chunk_count_in_bevy++;

        if (chunk_count_in_bevy >= chunks_per_segment) {
            RETURN_IF_ERROR(FlushBevy());
        }

        offset += chunk_size;
    }

    // Keep the last part of the buffer which is smaller than a chunk size.
    buffer.erase(0, offset);

    readptr += length;
    if (readptr > size) {
        size = readptr;
    }

    return STATUS_OK;
}


AFF4Image::AFF4Image(DataStore* resolver, URN urn):
    AFF4Stream(resolver, urn) {}

AFF4Image::AFF4Image(DataStore* resolver):
    AFF4Stream(resolver) {}


// ============================================================================
// Hash Integration
// ============================================================================

AFF4Status AFF4Image::EnableHashing(const std::vector<HashType>& types) {
    if (hashing_enabled_) {
        // Already enabled - don't reinitialize
        return STATUS_OK;
    }

    image_hasher_ = std::make_unique<MultiHasher>();

    std::vector<HashType> hash_types = types;
    if (hash_types.empty()) {
        // Default to SHA256 if no types specified
        hash_types.push_back(HashType::HASH_SHA256);
    }

    RETURN_IF_ERROR(image_hasher_->AddHashTypes(hash_types));
    RETURN_IF_ERROR(image_hasher_->Init());

    hashing_enabled_ = true;
    return STATUS_OK;
}

AFF4Status AFF4Image::EnableHashing(HashType type) {
    return EnableHashing(std::vector<HashType>{type});
}

AFF4Status AFF4Image::GetComputedHashes(std::vector<AFF4Hash>& hashes) const {
    if (!hashing_enabled_) {
        return NOT_FOUND;
    }

    hashes = computed_hashes_;
    return STATUS_OK;
}


AFF4Status AFF4Image::WriteStream(AFF4Stream* source,
                                  ProgressContext* progress) {
    DefaultProgress default_progress(resolver);
    if (!progress) {
        progress = &default_progress;
    }

    // Write a bevy at a time.
    while (1) {
        // This looks like a stream but can only read a bevy at a time.
        // Pass the hasher if hashing is enabled.
        _CompressorStream stream(resolver, compression, chunk_size,
                                 chunks_per_segment, source,
                                 hashing_enabled_ ? image_hasher_.get() : nullptr);

        // Read and compress the bevy into memory.
        RETURN_IF_ERROR(stream.PrepareBevy());

        URN bevy_urn(urn.Append(aff4_sprintf("%08d", bevy_number)));
        URN bevy_index_urn(bevy_urn.value + (".index"));

        // Between here and below, we can not switch the volume since
        // bevies are inconsistent.
        checkpointed = false;

        // First write the bevy.
        {
            AFF4Flusher<AFF4Stream> bevy;

            RETURN_IF_ERROR(
                current_volume->CreateMemberStream(bevy_urn, bevy));

            bevy->reserve(chunks_per_segment * chunk_size);

            RETURN_IF_ERROR(bevy->WriteStream(&stream, progress));
        }

        // Now write the index.
        {
            AFF4Flusher<AFF4Stream> bevy_index;
            RETURN_IF_ERROR(
                current_volume->CreateMemberStream(
                    bevy_index_urn, bevy_index));

            RETURN_IF_ERROR(bevy_index->Write(
                                stream.bevy_writer.index_stream()));

        }

        checkpointed = true;

        // Report the data read from the source.
        if (!progress->Report(source->Tell())) {
            return ABORTED;
        }

        bevy_number++;
        size += stream.Size();

        // The bevy is not full - this means we reached the end of the
        // input.
        if (stream.Size() < chunks_per_segment * chunk_size) {
            break;
        }
    }
    _write_metadata();

    return STATUS_OK;
}



/**
 * Read a single chunk from the bevy and append it to result.
 *
 * @param result: A string which will receive the chunk data.
 * @param bevy: The bevy to read from.
 * @param bevy_index: A bevy index array - the is the offset of each chunk in
 *        the bevy.
 * @param index_size: The length of the bevy index array.
 *
 * @return number of bytes read, or AFF4Status for error.
 */
AFF4Status AFF4Image::ReadChunkFromBevy(
    std::string& result, unsigned int chunk_id, AFF4Flusher<AFF4Stream>& bevy,
    BevyIndex bevy_index[], uint32_t index_size) {
    unsigned int chunk_id_in_bevy = chunk_id % chunks_per_segment;
    BevyIndex entry;

    // Check first to see if the chunk is in the cache
    const auto it = chunk_cache.find(chunk_id);
    if (it != chunk_cache.end()) {
        result += it->second;
        return STATUS_OK;
    }

    if (index_size == 0) {
        resolver->logger->error("Index empty in {} : chunk {}",
                               urn.SerializeToString(), chunk_id);
        return IO_ERROR;
    }

    // The segment is not completely full.
    if (chunk_id_in_bevy >= index_size) {
        resolver->logger->error("Bevy index too short in {} : {}",
                               urn.SerializeToString(), chunk_id);
        return IO_ERROR;

    } else {
        entry = (bevy_index[chunk_id_in_bevy]);
    }


    bevy->Seek(entry.offset, SEEK_SET);
    std::string cbuffer = bevy->Read(entry.length);

    std::string buffer;
    // We expect the decompressed buffer to be maximum chunk_size. If
    // it ends up decompressing to longer we error out.
    buffer.resize(chunk_size);

    AFF4Status res;

    if(entry.length == chunk_size) {
        // Chunk not compressed.
        buffer = cbuffer;
        res = STATUS_OK;
    } else {
        switch (compression) {
        case AFF4_IMAGE_COMPRESSION_ENUM_ZLIB:
            res = DeCompressZlib_(cbuffer, &buffer);
            break;

        case AFF4_IMAGE_COMPRESSION_ENUM_DEFLATE:
            res = DeCompressDeflate_(cbuffer, &buffer);
            break;

        case AFF4_IMAGE_COMPRESSION_ENUM_SNAPPY:
            res = DeCompressSnappy_(cbuffer, &buffer);
            break;

        case AFF4_IMAGE_COMPRESSION_ENUM_LZ4:
            res = DeCompressLZ4_(cbuffer, &buffer);
            break;

        case AFF4_IMAGE_COMPRESSION_ENUM_STORED:
            buffer = cbuffer;
            res = STATUS_OK;
            break;

            // Should never happen because the object should never accept this
            // compression URN.
        default:
            resolver->logger->critical("Unexpected compression type set");
            res = NOT_IMPLEMENTED;
        }
    }

    if (res != STATUS_OK) {
        resolver->logger->error(" {} : Unable to uncompress chunk {}",
                                urn, chunk_id);
        return res;
    }

    // Empty the cache if it's full
    if (chunk_cache.size() >= chunk_cache_size) {
        chunk_cache.clear();
    }

    // Add the decompressed chunk to the cache
    chunk_cache[chunk_id] = buffer;

    result += buffer;
    return STATUS_OK;
}

int AFF4Image::_ReadPartial(unsigned int chunk_id, int chunks_to_read,
                            std::string& result) {
    int chunks_read = 0;

    while (chunks_to_read > 0) {
        unsigned int bevy_id = chunk_id / chunks_per_segment;
        URN bevy_urn = urn.Append(aff4_sprintf("%08d", bevy_id));
        URN bevy_index_urn;

        if  (isAFF4Legacy) {
            bevy_index_urn = bevy_urn.value + ("/index");
        } else {
            bevy_index_urn = bevy_urn.value + (".index");
        }

        AFF4Flusher<AFF4Stream> bevy_index;
        AFF4Flusher<AFF4Stream> bevy;

        if (volumes->GetStream(bevy_index_urn, bevy_index) != STATUS_OK) {
            return -1;
        }

        if (volumes->GetStream(bevy_urn, bevy) != STATUS_OK) {
            return -1;
        }

        uint32_t index_size = bevy_index->Size() / sizeof(BevyIndex);
        std::string bevy_index_data = bevy_index->Read(bevy_index->Size());

        if (isAFF4Legacy) {
            // Massage the bevvy data format from the old into the new.
            bevy_index_data = _FixupBevyData(&bevy_index_data);
            index_size = bevy_index->Size() / sizeof(uint32_t);
        }

        BevyIndex* bevy_index_array = reinterpret_cast<BevyIndex*>(
            &bevy_index_data[0]);

        while (chunks_to_read > 0) {
            // Read a full chunk from the bevy.
            if (ReadChunkFromBevy(
                    result, chunk_id, bevy,
                    bevy_index_array, index_size) < 0) {
                return IO_ERROR;
            }

            chunks_to_read--;
            chunk_id++;
            chunks_read++;

            // This bevy is exhausted, get the next one.
            if (bevy_id < chunk_id / chunks_per_segment) {
                break;
            }
        }
    }

    return chunks_read;
}

AFF4Status AFF4Image::ReadBuffer(char* data, size_t* length) {
    if (*length > AFF4_MAX_READ_LEN) {
        *length = 0;
        return STATUS_OK; // FIXME?
    }

    *length = std::min((aff4_off_t)*length,
                       std::max((aff4_off_t)0, (aff4_off_t)Size() - readptr));

    int initial_chunk_offset = readptr % chunk_size;
    unsigned int initial_chunk_id = readptr / chunk_size;
    unsigned int final_chunk_id = (readptr + *length - 1) / chunk_size;

    // We read this many full chunks at once.
    int chunks_to_read = final_chunk_id - initial_chunk_id + 1;

    // TODO: write to the buffer, not a std::string
    unsigned int chunk_id = initial_chunk_id;
    std::string result;

    // Make sure we have enough room for output.
    result.reserve(chunks_to_read * chunk_size);

    while (chunks_to_read > 0) {
        int chunks_read = _ReadPartial(chunk_id, chunks_to_read, result);
        // Error occured.
        if (chunks_read < 0) {
            *length = 0;
            return STATUS_OK; // FIXME?
        } else if (chunks_read == 0) {
            break;
        }

        chunks_to_read -= chunks_read;
    }

    if (initial_chunk_offset) {
        result.erase(0, initial_chunk_offset);
    }

    std::memcpy(data, result.data(), *length);
    readptr = std::min((aff4_off_t)(readptr + *length), Size());
    return STATUS_OK;
}

AFF4Status AFF4Image::_write_metadata() {
    resolver->Set(urn, AFF4_TYPE, new URN(AFF4_IMAGESTREAM_TYPE),
                  /* replace = */ false);

    resolver->Set(urn, AFF4_STREAM_CHUNK_SIZE, new XSDInteger(chunk_size));

    resolver->Set(urn, AFF4_STREAM_CHUNKS_PER_SEGMENT,
                  new XSDInteger(chunks_per_segment));

    resolver->Set(urn, AFF4_STREAM_SIZE, new XSDInteger(size));

    resolver->Set(urn, AFF4_IMAGE_COMPRESSION, new URN(
                      CompressionMethodToURN(compression)));

    // Finalize and store hashes if hashing is enabled
    if (hashing_enabled_ && image_hasher_) {
        // Only finalize once - check if we have results already
        if (computed_hashes_.empty()) {
            AFF4Status status = image_hasher_->Finalize(computed_hashes_);
            if (status != STATUS_OK) {
                resolver->logger->warn("Failed to finalize hash computation");
                return status;
            }
        }

        // Store each computed hash in the metadata
        for (const AFF4Hash& hash : computed_hashes_) {
            AFF4Status status = StoreImageHash(resolver, urn, hash);
            if (status != STATUS_OK) {
                resolver->logger->warn("Failed to store {} hash",
                                       HashTypeToString(hash.type));
            } else {
                resolver->logger->info("Stored {} hash: {}",
                                       HashTypeToString(hash.type),
                                       hash.HexDigest());
            }
        }
    }

    return STATUS_OK;
}

bool AFF4Image::CanSwitchVolume() {
    return checkpointed;
}

AFF4Status AFF4Image::SwitchVolume(AFF4Volume *volume) {
    current_volume = volume;

    return STATUS_OK;
}


AFF4Status AFF4Image::Flush() {
    if (IsDirty()) {
        // Flush the last chunk.
        bevy_writer->EnqueueCompressChunk(chunk_count_in_bevy, buffer);
        RETURN_IF_ERROR(FlushBevy());

        _write_metadata();
        buffer.resize(0);
    }

    // Always call the baseclass to ensure the object is marked non dirty.
    return AFF4Stream::Flush();
}

std::string AFF4Image::_FixupBevyData(std::string* data){
    const uint32_t index_size = data->length() / sizeof(uint32_t);
    std::unique_ptr<BevyIndex[]> bevy_index_array(new BevyIndex[index_size]);
    uint32_t* bevy_index_data = reinterpret_cast<uint32_t*>(&(*data)[0]);

    uint64_t cOffset = 0;
    uint32_t cLength = 0;

    for(off_t offset = 0; offset < index_size; offset++){
        cLength = bevy_index_data[offset];
        bevy_index_array[offset].offset = cOffset;
        bevy_index_array[offset].length = cLength - cOffset;
        cOffset += bevy_index_array[offset].length;
    }

    return std::string(
        reinterpret_cast<char*>(bevy_index_array.get()),
        index_size * sizeof(BevyIndex)
    );
}


// ============================================================================
// Hash Verification
// ============================================================================

AFF4Status AFF4Image::GetStoredHashes(std::vector<AFF4Hash>& hashes) const {
    hashes.clear();

    // Try to get image-level hashes (linearHash* attributes)
    AFF4Status status = GetAllImageHashes(resolver, urn, hashes);
    if (status != STATUS_OK) {
        return status;
    }

    // If no image hashes found, try standard hash attributes
    if (hashes.empty()) {
        status = GetAllHashes(resolver, urn, hashes);
    }

    if (hashes.empty()) {
        return NOT_FOUND;
    }

    return STATUS_OK;
}

AFF4Status AFF4Image::VerifyHash(ImageVerifyResult& result,
                                  ProgressContext* progress) {
    result = ImageVerifyResult();
    result.image_urn = urn;

    // Get stored hashes from metadata
    std::vector<AFF4Hash> stored_hashes;
    AFF4Status status = GetStoredHashes(stored_hashes);
    if (status != STATUS_OK) {
        if (status == NOT_FOUND) {
            result.error_message = "No hashes stored in image metadata";
        } else {
            result.error_message = "Failed to retrieve stored hashes";
        }
        result.all_passed = false;
        return status;
    }

    if (stored_hashes.empty()) {
        result.error_message = "No hashes found for verification";
        result.all_passed = false;
        return NOT_FOUND;
    }

    // Create a multi-hasher with the same types as stored
    MultiHasher hasher;
    for (const auto& h : stored_hashes) {
        status = hasher.AddHashType(h.type);
        if (status != STATUS_OK) {
            result.error_message = "Failed to initialize hasher for " +
                                   HashTypeToString(h.type);
            result.all_passed = false;
            return status;
        }
    }

    status = hasher.Init();
    if (status != STATUS_OK) {
        result.error_message = "Failed to initialize hash computation";
        result.all_passed = false;
        return status;
    }

    // Read entire image and compute hashes
    DefaultProgress default_progress(resolver);
    if (!progress) {
        progress = &default_progress;
    }

    // Save and restore read pointer
    aff4_off_t original_readptr = readptr;
    Seek(0, SEEK_SET);

    const size_t buffer_size = chunk_size * 16;  // Read multiple chunks at once
    std::unique_ptr<char[]> buffer(new char[buffer_size]);

    aff4_off_t total_size = Size();
    aff4_off_t bytes_read = 0;

    while (bytes_read < total_size) {
        size_t to_read = std::min(static_cast<size_t>(total_size - bytes_read), buffer_size);
        size_t actually_read = to_read;

        status = ReadBuffer(buffer.get(), &actually_read);
        if (status != STATUS_OK) {
            result.error_message = "Failed to read image data at offset " +
                                   std::to_string(bytes_read);
            result.all_passed = false;
            Seek(original_readptr, SEEK_SET);
            return status;
        }

        if (actually_read == 0) {
            break;  // EOF
        }

        status = hasher.Update(buffer.get(), actually_read);
        if (status != STATUS_OK) {
            result.error_message = "Failed to update hash at offset " +
                                   std::to_string(bytes_read);
            result.all_passed = false;
            Seek(original_readptr, SEEK_SET);
            return status;
        }

        bytes_read += actually_read;

        if (!progress->Report(bytes_read)) {
            result.error_message = "Verification aborted by user";
            result.all_passed = false;
            Seek(original_readptr, SEEK_SET);
            return ABORTED;
        }
    }

    // Restore read pointer
    Seek(original_readptr, SEEK_SET);

    // Finalize and get computed hashes
    std::vector<AFF4Hash> computed_hashes;
    status = hasher.Finalize(computed_hashes);
    if (status != STATUS_OK) {
        result.error_message = "Failed to finalize hash computation";
        result.all_passed = false;
        return status;
    }

    // Compare stored vs computed hashes
    result.all_passed = true;
    for (const auto& stored : stored_hashes) {
        HashVerifyResult hr;
        hr.type = stored.type;
        hr.expected = stored;

        // Find matching computed hash
        bool found = false;
        for (const auto& computed : computed_hashes) {
            if (computed.type == stored.type) {
                hr.computed = computed;
                // Use the global VerifyHash function (not the method)
                hr.matches = aff4::VerifyHash(stored, computed);
                found = true;
                break;
            }
        }

        if (!found) {
            hr.matches = false;
            result.error_message = "Could not compute " +
                                   HashTypeToString(stored.type) + " hash";
        }

        if (!hr.matches) {
            result.all_passed = false;
        }

        result.hash_results.push_back(std::move(hr));
    }

    return STATUS_OK;
}


} // namespace aff4
