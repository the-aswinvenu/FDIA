#include "aff4/aff4_archive.h"

#include <blake3.h>
#include <lz4.h>
#include <openssl/evp.h>

#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

namespace {

using aff4::BevyFooterV1;
using aff4::ChunkRecordHeaderV1;
using aff4::ChunkRefV1;

#pragma pack(push, 1)
struct MapStreamHeaderV1 {
    uint8_t magic[8];
    uint64_t logical_size;
    uint32_t segment_count;
    uint32_t reserved;
};

struct SegmentHeaderV1 {
    uint64_t logical_offset;
    uint32_t chunk_count;
};

struct MapStreamFooterV1 {
    uint64_t chunk_count;
    uint64_t logical_size;
    uint64_t data_size;
    uint8_t sha256[32];
};
#pragma pack(pop)

std::string BytesToHex(const uint8_t* data, size_t len) {
    std::ostringstream out;
    out << std::hex << std::setfill('0');
    for (size_t i = 0; i < len; ++i) {
        out << std::setw(2) << static_cast<unsigned int>(data[i]);
    }
    return out.str();
}

bool HexToBytes(const std::string& hex, uint8_t* out, size_t out_len) {
    if (hex.size() != out_len * 2) {
        return false;
    }

    auto nibble = [](char ch) -> int {
        if (ch >= '0' && ch <= '9') return ch - '0';
        if (ch >= 'a' && ch <= 'f') return 10 + (ch - 'a');
        if (ch >= 'A' && ch <= 'F') return 10 + (ch - 'A');
        return -1;
    };

    for (size_t i = 0; i < out_len; ++i) {
        int high = nibble(hex[i * 2]);
        int low = nibble(hex[i * 2 + 1]);
        if (high < 0 || low < 0) {
            return false;
        }
        out[i] = static_cast<uint8_t>((high << 4) | low);
    }

    return true;
}

bool ComputeSha256(const uint8_t* data, size_t length, uint8_t out[32]) {
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    if (!ctx) {
        return false;
    }

    bool ok = EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) == 1 &&
              EVP_DigestUpdate(ctx, data, length) == 1;
    unsigned int digest_length = 0;
    if (ok) {
        ok = EVP_DigestFinal_ex(ctx, out, &digest_length) == 1 && digest_length == 32;
    }
    EVP_MD_CTX_free(ctx);
    return ok;
}

bool ReadEntireFile(const std::filesystem::path& path, std::vector<uint8_t>* data) {
    std::ifstream file(path, std::ios::binary);
    if (!file) {
        return false;
    }

    file.seekg(0, std::ios::end);
    std::streamsize size = file.tellg();
    file.seekg(0, std::ios::beg);

    if (size < 0) {
        return false;
    }

    data->resize(static_cast<size_t>(size));
    if (size > 0 && !file.read(reinterpret_cast<char*>(data->data()), size)) {
        return false;
    }
    return true;
}

bool LoadMapFile(const std::filesystem::path& path,
                 MapStreamHeaderV1* header,
                 SegmentHeaderV1* segment,
                 MapStreamFooterV1* footer,
                 std::vector<ChunkRefV1>* refs,
                 std::vector<uint8_t>* uncompressed,
                 std::string* error) {
    std::ifstream file(path, std::ios::binary);
    if (!file) {
        if (error) *error = "unable to open map file";
        return false;
    }

    uint64_t compressed_prefix_size = 0;
    file.read(reinterpret_cast<char*>(&compressed_prefix_size), sizeof(compressed_prefix_size));
    if (!file) {
        if (error) *error = "unable to read map size prefix";
        return false;
    }

    file.seekg(0, std::ios::end);
    std::streamsize compressed_size = file.tellg() - static_cast<std::streamsize>(sizeof(uint64_t));
    file.seekg(sizeof(uint64_t), std::ios::beg);

    if (compressed_size < 0 || compressed_prefix_size == 0) {
        if (error) *error = "invalid map size information";
        return false;
    }

    std::vector<uint8_t> compressed(static_cast<size_t>(compressed_size));
    if (compressed_size > 0 && !file.read(reinterpret_cast<char*>(compressed.data()), compressed_size)) {
        if (error) *error = "unable to read compressed map payload";
        return false;
    }

    uncompressed->resize(static_cast<size_t>(compressed_prefix_size));
    int res = LZ4_decompress_safe(
        reinterpret_cast<const char*>(compressed.data()),
        reinterpret_cast<char*>(uncompressed->data()),
        static_cast<int>(compressed_size),
        static_cast<int>(compressed_prefix_size));
    if (res < 0) {
        if (error) *error = "unable to decompress map payload";
        return false;
    }

    if (compressed_prefix_size < sizeof(MapStreamHeaderV1) + sizeof(SegmentHeaderV1) + sizeof(MapStreamFooterV1)) {
        if (error) *error = "map payload too small";
        return false;
    }

    memcpy(header, uncompressed->data(), sizeof(MapStreamHeaderV1));
    memcpy(segment, uncompressed->data() + sizeof(MapStreamHeaderV1), sizeof(SegmentHeaderV1));
    memcpy(footer, uncompressed->data() + compressed_prefix_size - sizeof(MapStreamFooterV1), sizeof(MapStreamFooterV1));

    refs->clear();
    const size_t refs_offset = sizeof(MapStreamHeaderV1) + sizeof(SegmentHeaderV1);
    const size_t refs_size = static_cast<size_t>(segment->chunk_count) * sizeof(ChunkRefV1);
    if (refs_offset + refs_size + sizeof(MapStreamFooterV1) != compressed_prefix_size) {
        if (error) *error = "map structure size mismatch";
        return false;
    }

    refs->resize(segment->chunk_count);
    if (segment->chunk_count > 0) {
        memcpy(refs->data(), uncompressed->data() + refs_offset, refs_size);
    }

    uint8_t computed_sha[32] = {};
    if (!ComputeSha256(uncompressed->data(), compressed_prefix_size - sizeof(MapStreamFooterV1), computed_sha)) {
        if (error) *error = "unable to compute map checksum";
        return false;
    }
    if (memcmp(computed_sha, footer->sha256, sizeof(computed_sha)) != 0) {
        if (error) *error = "map checksum mismatch";
        return false;
    }

    return true;
}

bool ValidateChunkAtRef(const std::filesystem::path& archive_dir, const ChunkRefV1& ref, std::string* error) {
    std::filesystem::path bevy_path = archive_dir / "bevies" / ("bevy_" + std::to_string(ref.bevy_id) + ".bev");
    FILE* bevy_file = fopen(bevy_path.c_str(), "rb");
    if (!bevy_file) {
        if (error) *error = "missing bevy file";
        return false;
    }

    if (fseek(bevy_file, 0, SEEK_END) != 0) {
        fclose(bevy_file);
        if (error) *error = "failed to seek bevy";
        return false;
    }
    long file_size_long = ftell(bevy_file);
    if (file_size_long < 0) {
        fclose(bevy_file);
        if (error) *error = "failed to size bevy";
        return false;
    }

    uint64_t file_size = static_cast<uint64_t>(file_size_long);
    if (file_size < 16 + sizeof(BevyFooterV1)) {
        fclose(bevy_file);
        if (error) *error = "bevy too small";
        return false;
    }

    if (fseek(bevy_file, static_cast<long>(file_size - sizeof(BevyFooterV1)), SEEK_SET) != 0) {
        fclose(bevy_file);
        if (error) *error = "failed to seek footer";
        return false;
    }

    BevyFooterV1 footer{};
    if (fread(&footer, 1, sizeof(footer), bevy_file) != sizeof(footer)) {
        fclose(bevy_file);
        if (error) *error = "failed to read bevy footer";
        return false;
    }

    if (footer.bevy_size != file_size || footer.index_offset < 16 || footer.index_offset >= file_size - sizeof(BevyFooterV1)) {
        fclose(bevy_file);
        if (error) *error = "invalid bevy footer bounds";
        return false;
    }

    if (fseek(bevy_file, static_cast<long>(ref.offset), SEEK_SET) != 0) {
        fclose(bevy_file);
        if (error) *error = "failed to seek chunk offset";
        return false;
    }

    ChunkRecordHeaderV1 header{};
    if (fread(&header, 1, sizeof(header), bevy_file) != sizeof(header)) {
        fclose(bevy_file);
        if (error) *error = "failed to read chunk header";
        return false;
    }

    std::vector<uint8_t> payload(header.compressed_size);
    if (header.compression_type == 0) {
        if (header.compressed_size != header.uncompressed_size) {
            fclose(bevy_file);
            if (error) *error = "raw chunk size mismatch";
            return false;
        }
        if (fread(payload.data(), 1, header.uncompressed_size, bevy_file) != header.uncompressed_size) {
            fclose(bevy_file);
            if (error) *error = "failed to read raw chunk payload";
            return false;
        }
    } else if (header.compression_type == 1) {
        if (fread(payload.data(), 1, header.compressed_size, bevy_file) != header.compressed_size) {
            fclose(bevy_file);
            if (error) *error = "failed to read compressed chunk payload";
            return false;
        }
    } else {
        fclose(bevy_file);
        if (error) *error = "unknown chunk compression type";
        return false;
    }

    fclose(bevy_file);

    std::vector<uint8_t> raw_data;
    if (header.compression_type == 1) {
        raw_data.resize(header.uncompressed_size);
        int dec_res = LZ4_decompress_safe(
            reinterpret_cast<const char*>(payload.data()),
            reinterpret_cast<char*>(raw_data.data()),
            header.compressed_size,
            header.uncompressed_size);
        if (dec_res < 0) {
            if (error) *error = "chunk payload decompression failed";
            return false;
        }
    } else {
        raw_data = payload;
    }

    blake3_hasher hasher;
    blake3_hasher_init(&hasher);
    blake3_hasher_update(&hasher, raw_data.data(), raw_data.size());
    uint8_t test_hash[BLAKE3_OUT_LEN];
    blake3_hasher_finalize(&hasher, test_hash, BLAKE3_OUT_LEN);

    if (memcmp(test_hash, header.blake3_hash, BLAKE3_OUT_LEN) != 0) {
        if (error) *error = "chunk hash mismatch";
        return false;
    }

    if (ref.uncompressed_size != header.uncompressed_size) {
        if (error) *error = "reference size mismatch";
        return false;
    }

    return true;
}

void PrintMapSummary(const std::filesystem::path& map_path) {
    MapStreamHeaderV1 header{};
    SegmentHeaderV1 segment{};
    MapStreamFooterV1 footer{};
    std::vector<ChunkRefV1> refs;
    std::vector<uint8_t> uncompressed;
    std::string error;

    if (!LoadMapFile(map_path, &header, &segment, &footer, &refs, &uncompressed, &error)) {
        std::cout << "Map: " << map_path.filename().string() << " - " << error << "\n";
        return;
    }

    std::cout << "Map: " << map_path.filename().string() << "\n";
    std::cout << "  Logical size: " << header.logical_size << " bytes\n";
    std::cout << "  Segment count: " << header.segment_count << "\n";
    std::cout << "  Chunk count: " << segment.chunk_count << "\n";
    std::cout << "  Footer logical size: " << footer.logical_size << "\n";
    std::cout << "  Footer chunk count: " << footer.chunk_count << "\n";
    std::cout << "  Footer data size: " << footer.data_size << "\n";

    uint64_t logical_sum = 0;
    for (const auto& ref : refs) {
        logical_sum += ref.uncompressed_size;
    }
    std::cout << "  Chunk size sum: " << logical_sum << "\n";
    std::cout << "  Status: " << ((logical_sum == header.logical_size && footer.logical_size == header.logical_size && footer.chunk_count == segment.chunk_count) ? "consistent" : "mismatch") << "\n";
}

void PrintBevySummary(const std::filesystem::path& bevy_path) {
    FILE* bevy_file = fopen(bevy_path.c_str(), "rb");
    if (!bevy_file) {
        std::cout << "Bevy: " << bevy_path.filename().string() << " - unreadable\n";
        return;
    }

    if (fseek(bevy_file, 0, SEEK_END) != 0) {
        fclose(bevy_file);
        std::cout << "Bevy: " << bevy_path.filename().string() << " - seek failed\n";
        return;
    }
    long file_size_long = ftell(bevy_file);
    if (file_size_long < 0) {
        fclose(bevy_file);
        std::cout << "Bevy: " << bevy_path.filename().string() << " - sizing failed\n";
        return;
    }

    uint64_t file_size = static_cast<uint64_t>(file_size_long);
    if (file_size < 16 + sizeof(BevyFooterV1)) {
        fclose(bevy_file);
        std::cout << "Bevy: " << bevy_path.filename().string() << " - too small\n";
        return;
    }

    if (fseek(bevy_file, static_cast<long>(file_size - sizeof(BevyFooterV1)), SEEK_SET) != 0) {
        fclose(bevy_file);
        std::cout << "Bevy: " << bevy_path.filename().string() << " - footer seek failed\n";
        return;
    }

    BevyFooterV1 footer{};
    if (fread(&footer, 1, sizeof(footer), bevy_file) != sizeof(footer)) {
        fclose(bevy_file);
        std::cout << "Bevy: " << bevy_path.filename().string() << " - footer read failed\n";
        return;
    }

    std::cout << "Bevy: " << bevy_path.filename().string() << "\n";
    std::cout << "  File size: " << file_size << " bytes\n";
    std::cout << "  Chunk count: " << footer.chunk_count << "\n";
    std::cout << "  Bevy size: " << footer.bevy_size << "\n";
    std::cout << "  Index offset: " << footer.index_offset << "\n";

    uint8_t computed_sha[32] = {};
    if (fseek(bevy_file, 0, SEEK_SET) == 0) {
        std::vector<uint8_t> prefix(static_cast<size_t>(file_size - sizeof(BevyFooterV1)));
        if (fread(prefix.data(), 1, prefix.size(), bevy_file) == prefix.size() &&
            ComputeSha256(prefix.data(), prefix.size(), computed_sha)) {
            std::cout << "  SHA-256 match: "
                      << (memcmp(computed_sha, footer.sha256, sizeof(computed_sha)) == 0 ? "yes" : "no")
                      << "\n";
        }
    }

    fclose(bevy_file);
}

bool VerifyArchive(const std::filesystem::path& archive_dir) {
    const std::filesystem::path maps_dir = archive_dir / "maps";
    if (!std::filesystem::exists(maps_dir)) {
        std::cout << "No maps directory found.\n";
        return false;
    }

    bool all_ok = true;
    for (const auto& entry : std::filesystem::directory_iterator(maps_dir)) {
        if (!entry.is_regular_file()) {
            continue;
        }

        MapStreamHeaderV1 header{};
        SegmentHeaderV1 segment{};
        MapStreamFooterV1 footer{};
        std::vector<ChunkRefV1> refs;
        std::vector<uint8_t> uncompressed;
        std::string error;

        if (!LoadMapFile(entry.path(), &header, &segment, &footer, &refs, &uncompressed, &error)) {
            std::cout << "Map " << entry.path().filename().string() << ": " << error << "\n";
            all_ok = false;
            continue;
        }

        for (const auto& ref : refs) {
            std::string chunk_error;
            if (!ValidateChunkAtRef(archive_dir, ref, &chunk_error)) {
                std::cout << "Map " << entry.path().filename().string() << ": chunk validation failed - "
                          << chunk_error << "\n";
                all_ok = false;
                break;
            }
        }
    }

    return all_ok;
}

void PrintUsage(const char* argv0) {
    std::cout << "Usage:\n"
              << "  " << argv0 << " bevy <bevy_file>\n"
              << "  " << argv0 << " map <map_file>\n"
              << "  " << argv0 << " chunk <archive_dir> <sha256_hex>\n"
              << "  " << argv0 << " verify <archive_dir>\n";
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 3) {
        PrintUsage(argv[0]);
        return 1;
    }

    const std::string command = argv[1];
    if (command == "bevy") {
        PrintBevySummary(argv[2]);
        return 0;
    }

    if (command == "map") {
        PrintMapSummary(argv[2]);
        return 0;
    }

    if (command == "chunk") {
        if (argc < 4) {
            PrintUsage(argv[0]);
            return 1;
        }

        aff4::ChunkCorpus corpus((std::filesystem::path(argv[2]) / "chunk_corpus").string());
        if (corpus.Initialize() != aff4::STATUS_OK) {
            std::cerr << "Unable to open chunk corpus\n";
            return 1;
        }

        uint8_t hash[32] = {};
        if (!HexToBytes(argv[3], hash, sizeof(hash))) {
            std::cerr << "Invalid SHA-256 hex string\n";
            return 1;
        }

        aff4::ChunkIndexValueV1 value{};
        if (corpus.Get(hash, &value) != aff4::STATUS_OK) {
            std::cout << "Chunk not found\n";
            return 1;
        }

        std::cout << "Chunk found\n"
                  << "  Bevy id: " << value.bevy_id << "\n"
                  << "  Offset: " << value.offset << "\n"
                  << "  Compressed size: " << value.compressed_size << "\n"
                  << "  Uncompressed size: " << value.uncompressed_size << "\n"
                  << "  Flags: " << value.flags << "\n";
        return 0;
    }

    if (command == "verify") {
        bool ok = VerifyArchive(argv[2]);
        return ok ? 0 : 1;
    }

    PrintUsage(argv[0]);
    return 1;
}
