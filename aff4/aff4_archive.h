#ifndef SRC_AFF4_ARCHIVE_H_
#define SRC_AFF4_ARCHIVE_H_

// #include "aff4/config.h"
#include "aff4_io.h"
#include "data_store.h"
#include <rocksdb/db.h>
#include <vector>
#include <string>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>

namespace aff4 {

/**
 * @brief AFF4 Deduplicated Archival Subsystem
 *
 * This module implements a chunk-based deduplicating forensic archive component 
 * for the AFF4 library. It transforms the conventional stream-oriented storage 
 * into a global content-defined chunking (CDC) repository.
 *
 * Architectural Components:
 * 1. FastCDC Logic: Dynamically segments incoming forensic disk streams into 
 *    variable-sized chunks (targeting ~2MB) based on internal binary layout.
 * 2. BLAKE3 Hashing: Generates cryptographic identities for each chunk.
 * 3. ChunkCorpus (RocksDB): A fast, external key-value store mapping BLAKE3 
 *    hashes to physical 'Bevy' offsets to enable massive cross-image deduplication.
 * 4. BevyWriter: Appends unique, deduplicated chunks (LZ4-compressed) to bulk 
 *    storage files ("Bevies") which rotate at ~2GB caps.
 * 5. ArchiveMapStream: Stores the logical layout of the original image as a 
 *    sequence of chunk reference pointers, meaning identical segments across 
 *    different disk images use zero additional storage.
 */

#pragma pack(push, 1)

// Value stored in RocksDB
struct ChunkIndexValueV1 {
    uint32_t bevy_id;
    uint64_t offset;
    uint32_t compressed_size;
    uint32_t uncompressed_size;
    uint32_t flags;
};

// Chunk header stored in Bevy
struct ChunkRecordHeaderV1 {
    uint32_t compressed_size;
    uint32_t uncompressed_size;
    uint8_t compression_type; // 0=raw, 1=lz4
    uint8_t reserved[3];
    uint8_t blake3_hash[32];
};

// Bevy footer
struct BevyFooterV1 {
    uint64_t chunk_count;
    uint64_t bevy_size;
    uint64_t index_offset;
    uint8_t sha256[32];
};

// Map stream segment header
struct SegmentHeaderV1 {
    uint64_t logical_offset;
    uint32_t chunk_count;
};

// Map stream chunk reference
struct ChunkRefV1 {
    uint32_t bevy_id;
    uint64_t offset;
    uint32_t uncompressed_size;
};

struct MapStreamHeaderV1 {
    uint8_t magic[8];
    uint64_t logical_size;
    uint32_t segment_count;
    uint32_t reserved;
};

struct MapStreamFooterV1 {
    uint64_t chunk_count;
    uint64_t logical_size;
    uint64_t data_size;
    uint8_t sha256[32];
};

#pragma pack(pop)

class ChunkCorpus {
public:
    ChunkCorpus(const std::string& db_path);
    ~ChunkCorpus();

    AFF4Status Initialize();
    AFF4Status Put(const uint8_t hash[32], const ChunkIndexValueV1& value);
    AFF4Status Get(const uint8_t hash[32], ChunkIndexValueV1* value);
    AFF4Status PutMetadata(const std::string& key, const std::string& value);
    AFF4Status GetMetadata(const std::string& key, std::string* value);
    AFF4Status ListMetadataPrefix(const std::string& prefix,
                                  std::vector<std::pair<std::string, std::string>>* entries);
    void Close();

private:
    std::string db_path_;
    rocksdb::DB* db_ = nullptr;
    rocksdb::ColumnFamilyHandle* chunk_index_cf_ = nullptr;
    rocksdb::ColumnFamilyHandle* metadata_cf_ = nullptr;
    std::vector<rocksdb::ColumnFamilyHandle*> handles_;
};

class BevyWriter {
public:
    BevyWriter(const std::string& dir_path, uint32_t initial_id);
    ~BevyWriter();

    AFF4Status Initialize();
    AFF4Status AppendChunk(const ChunkRecordHeaderV1& header, const uint8_t* data, uint32_t* out_bevy_id, uint64_t* out_offset);
    AFF4Status FinalizeBevy();

    uint64_t current_size() const { return current_size_; }
    uint32_t current_bevy_id() const { return bevy_id_; }

private:
    std::string dir_path_;
    uint32_t bevy_id_;
    FILE* file_ = nullptr;
    uint64_t current_size_ = 0;
    std::vector<uint64_t> chunk_offsets_;
};

class ArchiveMapStream : public AFF4Stream {
public:
    ArchiveMapStream(DataStore* resolver, URN urn);
    virtual ~ArchiveMapStream();

    AFF4Status AppendRef(const ChunkRefV1& ref);
    AFF4Status FinalizeMap(const std::string& path);
    // Inherited
    virtual AFF4Status ReadBuffer(char* data, size_t* length) override;
    virtual AFF4Status Write(const char* data, size_t length) override;
    virtual AFF4Status Truncate() override;
    virtual aff4_off_t Size() const override;

private:
    std::vector<ChunkRefV1> current_segment_refs_;
    uint64_t logical_offset_ = 0;
};

class ArchiveChunkStore {
public:
    ArchiveChunkStore(DataStore* resolver, const std::string& archive_dir);
    ~ArchiveChunkStore();

    AFF4Status Initialize();
    AFF4Status IngestStream(AFF4Stream* input_stream, URN image_urn,
                            const std::string& expected_input_sha256_hex);

private:
    DataStore* resolver_;
    std::string archive_dir_;
    std::unique_ptr<ChunkCorpus> corpus_;
    std::unique_ptr<BevyWriter> bevy_writer_;
    std::mutex write_mutex_;
    AFF4Status RecoverChunkJournal();
};

class ArchiveExtractor {
public:
    ArchiveExtractor(const std::string& archive_dir);
    ~ArchiveExtractor();

    AFF4Status ExtractMap(const std::string& map_name, const std::string& output_path);

private:
    std::string archive_dir_;
    std::unordered_map<uint32_t, FILE*> open_bevies_;
    std::unordered_set<uint32_t> validated_bevies_;
    FILE* GetBevyFile(uint32_t bevy_id);
    AFF4Status ValidateBevy(FILE* bevy_file, uint32_t bevy_id);
};

} // namespace aff4

#endif // SRC_AFF4_ARCHIVE_H_
