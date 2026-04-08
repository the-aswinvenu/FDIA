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

/*
  Archive-only command line entry point.
*/
#include "aff4/aff4_archive.h"
#include "aff4/aff4_file.h"
#include "aff4/libaff4.h"

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <lz4.h>
#include <iostream>
#include <openssl/evp.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <sstream>
#include <string>
#include <vector>

namespace {

struct CliOptions {
    bool show_help = false;
    std::string archive_dir;
    std::string extract_dir;
    std::string info_dir;
    std::string map_name;
    std::string output_path;
    std::vector<std::string> inputs;
};

std::string SanitizeComponent(std::string value) {
    std::replace(value.begin(), value.end(), '/', '_');
    std::replace(value.begin(), value.end(), ':', '_');
    return value;
}

bool ComputeFileSha256Hex(const std::string& path, std::string* out_hex) {
    if (!out_hex) {
        return false;
    }

    std::ifstream file(path, std::ios::binary);
    if (!file) {
        return false;
    }

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr);

    std::vector<char> buffer(1024 * 1024);
    while (file) {
        file.read(buffer.data(), buffer.size());
        std::streamsize count = file.gcount();
        if (count > 0) {
            EVP_DigestUpdate(ctx, buffer.data(), static_cast<size_t>(count));
        }
    }

    if (!file.eof()) {
        EVP_MD_CTX_free(ctx);
        return false;
    }

    uint8_t digest[32] = {};
    unsigned int digest_length = 0;
    EVP_DigestFinal_ex(ctx, digest, &digest_length);
    EVP_MD_CTX_free(ctx);

    if (digest_length != 32) {
        return false;
    }

    std::ostringstream out;
    out << std::hex << std::setfill('0');
    for (uint8_t byte : digest) {
        out << std::setw(2) << static_cast<unsigned int>(byte);
    }

    *out_hex = out.str();
    return true;
}

void PrintUsage(const char* argv0) {
    std::cerr << "Usage:\n";
    std::cerr << "  " << argv0 << " --archive-img|-a <dir> <inputs...>\n";
    std::cerr << "  " << argv0 << " --extract-img|-x <dir> <map_name> <output_path>\n";
    std::cerr << "  " << argv0 << " --info <dir>\n";
    std::cerr << "  " << argv0 << " --help|-h\n";
}

bool ParseArgs(int argc, char** argv, CliOptions* options, std::string* error) {
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        auto require_value = [&](const char* flag) -> std::string {
            if (i + 1 >= argc) {
                *error = std::string("Missing value for ") + flag;
                return {};
            }
            return argv[++i];
        };

        if (arg == "--archive-img" || arg == "-a") {
            options->archive_dir = require_value("--archive-img");
            // options->archive_dir = "/media/unknown/WDBlue1/Archive/"; //Testing only
        } else if (arg == "--extract-img" || arg == "-x") {
            options->extract_dir = require_value("--extract-img");
            // options->extract_dir = "/media/unknown/WDBlue1/Archive/"; //Testing only
        } else if (arg == "--info") {
            options->info_dir = require_value("--info");
            // options->info_dir = "/media/unknown/WDBlue1/Archive/"; //Testing only 
        } else if (arg == "--help" || arg == "-h") {
            options->show_help = true;
        } else if (arg.rfind("--", 0) == 0 || (arg.size() > 1 && arg[0] == '-')) {
            *error = std::string("Unknown flag: ") + arg;
            return false;
        } else {
            options->inputs.push_back(arg);
        }
    }

    const bool ingest_mode = !options->archive_dir.empty();
    const bool extract_mode = !options->extract_dir.empty();
    const bool info_mode = !options->info_dir.empty();

    if (options->show_help) {
        return true;
    }

    if ((ingest_mode + extract_mode + info_mode) != 1) {
        *error = "Specify exactly one of --archive-img, --extract-img, or --info.";
        return false;
    }

    if (ingest_mode && options->inputs.empty()) {
        *error = "Archive ingestion requires at least one input path.";
        return false;
    }

    if (extract_mode) {
        if (options->inputs.size() != 2) {
            *error = "Archive extraction requires <map_name> and <output_path>.";
            return false;
        }
        options->map_name = options->inputs[0];
        options->output_path = options->inputs[1];
        options->inputs.clear();
    }

    if (info_mode && !options->inputs.empty()) {
        *error = "Archive metadata reporting does not accept input files.";
        return false;
    }

    return true;
}

aff4::URN BuildImageUrn(const std::string& input_path) {
    std::filesystem::path path(input_path);
    std::string urn_suffix = path.lexically_normal().string();
    urn_suffix = SanitizeComponent(urn_suffix);
    return aff4::URN("aff4://archive/").Append(urn_suffix);
}

int RunIngest(const CliOptions& options) {
    aff4::MemoryDataStore resolver;
    aff4::ArchiveChunkStore archive_store(&resolver, options.archive_dir);
    if (archive_store.Initialize() != aff4::STATUS_OK) {
        std::cerr << "Failed to initialize archive directory: " << options.archive_dir << "\n";
        return 1;
    }

    for (const std::string& input_path : options.inputs) {
        std::string input_sha256_hex;
        if (!ComputeFileSha256Hex(input_path, &input_sha256_hex)) {
            std::cerr << "Failed to compute SHA-256 before ingest: " << input_path << "\n";
            return 1;
        }

        aff4::AFF4Flusher<aff4::FileBackedObject> input_stream;
        if (aff4::NewFileBackedObject(&resolver, input_path, "read", input_stream) != aff4::STATUS_OK) {
            std::cerr << "Failed to open input: " << input_path << "\n";
            return 1;
        }

        aff4::URN image_urn = BuildImageUrn(input_path);
        if (archive_store.IngestStream(input_stream.get(), image_urn, input_sha256_hex) != aff4::STATUS_OK) {
            std::cerr << "Failed to ingest input: " << input_path << "\n";
            return 1;
        }
    }

    return 0;
}

int RunExtract(const CliOptions& options) {
    aff4::ArchiveExtractor extractor(options.extract_dir);
    if (extractor.ExtractMap(options.map_name, options.output_path) != aff4::STATUS_OK) {
        std::cerr << "Failed to extract map: " << options.map_name << "\n";
        return 1;
    }
    return 0;
}

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

struct ChunkRefV1 {
    uint32_t bevy_id;
    uint64_t offset;
    uint32_t uncompressed_size;
};

struct MapStreamFooterV1 {
    uint64_t chunk_count;
    uint64_t logical_size;
    uint64_t data_size;
    uint8_t sha256[32];
};
#pragma pack(pop)

void ReportArchiveMap(const std::filesystem::path& map_path) {
    std::ifstream map_file(map_path, std::ios::binary);
    if (!map_file) {
        std::cout << "  - Map: " << map_path.filename().string() << " (unreadable)\n";
        return;
    }

    uint64_t uncompressed_size = 0;
    map_file.read(reinterpret_cast<char*>(&uncompressed_size), sizeof(uncompressed_size));
    if (!map_file || uncompressed_size < sizeof(MapStreamHeaderV1) + sizeof(SegmentHeaderV1) + sizeof(MapStreamFooterV1)) {
        std::cout << "  - Map: " << map_path.filename().string() << " (invalid map header)\n";
        return;
    }

    map_file.seekg(0, std::ios::end);
    const std::streamsize compressed_size = map_file.tellg() - static_cast<std::streamsize>(sizeof(uint64_t));
    map_file.seekg(sizeof(uint64_t), std::ios::beg);

    std::vector<uint8_t> compressed(static_cast<size_t>(compressed_size));
    map_file.read(reinterpret_cast<char*>(compressed.data()), compressed_size);
    if (!map_file) {
        std::cout << "  - Map: " << map_path.filename().string() << " (failed to read data)\n";
        return;
    }

    std::vector<uint8_t> uncompressed(static_cast<size_t>(uncompressed_size));
    int res = LZ4_decompress_safe(
        reinterpret_cast<const char*>(compressed.data()),
        reinterpret_cast<char*>(uncompressed.data()),
        static_cast<int>(compressed_size),
        static_cast<int>(uncompressed_size));
    if (res < 0) {
        std::cout << "  - Map: " << map_path.filename().string() << " (failed to decompress)\n";
        return;
    }

    const auto* header = reinterpret_cast<const MapStreamHeaderV1*>(uncompressed.data());
    const auto* segment = reinterpret_cast<const SegmentHeaderV1*>(uncompressed.data() + sizeof(MapStreamHeaderV1));
    const auto* footer = reinterpret_cast<const MapStreamFooterV1*>(uncompressed.data() + uncompressed_size - sizeof(MapStreamFooterV1));
    const auto* refs = reinterpret_cast<const ChunkRefV1*>(uncompressed.data() + sizeof(MapStreamHeaderV1) + sizeof(SegmentHeaderV1));

    uint64_t logical_sum = 0;
    for (uint32_t i = 0; i < segment->chunk_count; ++i) {
        logical_sum += refs[i].uncompressed_size;
    }

    std::cout << "  - Disk Image: " << map_path.filename().string() << "\n"
              << "    Logical Extracted Size : " << header->logical_size << " bytes (" << (header->logical_size / 1024 / 1024) << " MB)\n"
              << "    Assigned Chunks        : " << segment->chunk_count << "\n"
              << "    Header Logical Size     : " << header->logical_size << " bytes\n"
              << "    Footer Logical Size     : " << footer->logical_size << " bytes\n"
              << "    Chunk Size Sum          : " << logical_sum << " bytes\n"
              << "    Footer Chunk Count      : " << footer->chunk_count << "\n";

    if (header->logical_size != footer->logical_size || logical_sum != header->logical_size || segment->chunk_count != footer->chunk_count) {
        std::cout << "    Status                  : metadata mismatch detected\n";
    } else {
        std::cout << "    Status                  : metadata consistent\n";
    }

    std::cout << "\n";
}

int RunInfo(const CliOptions& options) {
    const std::filesystem::path archive_dir(options.info_dir);

    std::cout << "\n============================================\n";
    std::cout << "    AFF4 ARCHIVE GLOBAL METADATA REPORT     \n";
    std::cout << "============================================\n\n";

    uint64_t physical_size = 0;
    const std::filesystem::path bevies_dir = archive_dir / "bevies";
    if (std::filesystem::exists(bevies_dir)) {
        for (const auto& entry : std::filesystem::directory_iterator(bevies_dir)) {
            if (entry.is_regular_file()) {
                physical_size += std::filesystem::file_size(entry.path());
            }
        }
    }

    rocksdb::Options db_options;
    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    column_families.push_back(rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName, rocksdb::ColumnFamilyOptions()));
    column_families.push_back(rocksdb::ColumnFamilyDescriptor("chunk_index_cf", rocksdb::ColumnFamilyOptions()));
    column_families.push_back(rocksdb::ColumnFamilyDescriptor("metadata_cf", rocksdb::ColumnFamilyOptions()));

    std::vector<rocksdb::ColumnFamilyHandle*> handles;
    rocksdb::DB* db = nullptr;
    uint64_t unique_chunks = 0;
    rocksdb::Status status = rocksdb::DB::OpenForReadOnly(db_options, (archive_dir / "chunk_corpus").string(), column_families, &handles, &db);
    if (status.ok()) {
        std::unique_ptr<rocksdb::Iterator> it(db->NewIterator(rocksdb::ReadOptions(), handles[1]));
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            ++unique_chunks;
        }
    }

    std::cout << "[GLOBAL STORAGE STATS]\n";
    std::cout << "Total Globally Unique CDC Chunks : " << unique_chunks << "\n";
    std::cout << "Total Physical Bevy Disk Size    : " << physical_size << " bytes (" << (physical_size / 1024 / 1024) << " MB)\n\n";

    std::cout << "[INGESTED LOGICAL DISK MAPPINGS]\n";
    const std::filesystem::path maps_dir = archive_dir / "maps";
    if (std::filesystem::exists(maps_dir)) {
        for (const auto& entry : std::filesystem::directory_iterator(maps_dir)) {
            if (entry.is_regular_file()) {
                ReportArchiveMap(entry.path());
            }
        }
    }

    std::cout << "============================================\n\n";

    if (db) {
        for (auto* handle : handles) {
            db->DestroyColumnFamilyHandle(handle);
        }
        delete db;
    }

    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    CliOptions options;
    std::string error;
    if (!ParseArgs(argc, argv, &options, &error)) {
        std::cerr << error << "\n";
        PrintUsage(argv[0]);
        return 1;
    }

    if (options.show_help) {
        PrintUsage(argv[0]);
        return 0;
    }

    if (!options.archive_dir.empty()) {
        return RunIngest(options);
    }

    if (!options.info_dir.empty()) {
        return RunInfo(options);
    }

    return RunExtract(options);
}
