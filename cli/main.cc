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
#include "archive/archive.h"
#include "imgformatlib/aff4/aff4_file.h"
#include "imgformatlib/aff4/libaff4.h"
#include "imgformatlib/ewf_stream.h"

#include <algorithm>
#include <cctype>
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
#include <unistd.h>
#include <vector>

namespace {

struct CliOptions {
    bool show_help = false;
    bool auto_all_segments = false;
    std::string archive_dir;
    std::string extract_dir;
    std::string info_dir;
    std::string map_name;
    std::string output_path;
    std::vector<std::string> inputs;
};

constexpr const char* kLayoutEwf = "ewf";
constexpr const char* kLayoutAff4 = "aff4";
constexpr const char* kLayoutRawSingle = "raw-single";
constexpr const char* kLayoutRawMultipart = "raw-multipart";

class MultipartInputStream : public aff4::AFF4Stream {
public:
    MultipartInputStream(aff4::DataStore* resolver, aff4::URN urn)
        : aff4::AFF4Stream(resolver, urn) {
        properties.seekable = true;
        properties.sizeable = true;
    }

    ~MultipartInputStream() override {
        CloseAll();
    }

    aff4::AFF4Status Open(const std::vector<std::string>& parts) {
        CloseAll();
        if (parts.empty()) return aff4::GENERIC_ERROR;

        uint64_t total = 0;
        for (const auto& path : parts) {
            FILE* f = fopen(path.c_str(), "rb");
            if (!f) {
                CloseAll();
                return aff4::GENERIC_ERROR;
            }
            if (fseek(f, 0, SEEK_END) != 0) {
                fclose(f);
                CloseAll();
                return aff4::GENERIC_ERROR;
            }
            long end = ftell(f);
            if (end < 0 || fseek(f, 0, SEEK_SET) != 0) {
                fclose(f);
                CloseAll();
                return aff4::GENERIC_ERROR;
            }
            files_.push_back(f);
            sizes_.push_back(static_cast<uint64_t>(end));
            total += static_cast<uint64_t>(end);
        }

        size = static_cast<aff4::aff4_off_t>(total);
        readptr = 0;
        current_part_ = 0;
        current_part_offset_ = 0;
        return aff4::STATUS_OK;
    }

    aff4::AFF4Status ReadBuffer(char* data, size_t* length) override {
        if (!data || !length) return aff4::GENERIC_ERROR;
        size_t requested = *length;
        *length = 0;

        while (requested > 0 && current_part_ < files_.size()) {
            uint64_t part_remaining = sizes_[current_part_] - current_part_offset_;
            if (part_remaining == 0) {
                ++current_part_;
                current_part_offset_ = 0;
                continue;
            }

            size_t to_read = static_cast<size_t>(std::min<uint64_t>(requested, part_remaining));
            size_t n = fread(data + *length, 1, to_read, files_[current_part_]);
            if (n == 0) {
                if (ferror(files_[current_part_])) return aff4::GENERIC_ERROR;
                ++current_part_;
                current_part_offset_ = 0;
                continue;
            }

            *length += n;
            requested -= n;
            readptr += static_cast<aff4::aff4_off_t>(n);
            current_part_offset_ += static_cast<uint64_t>(n);
        }

        return aff4::STATUS_OK;
    }

    aff4::AFF4Status Seek(aff4::aff4_off_t offset, int whence) override {
        int64_t base = 0;
        if (whence == SEEK_SET) {
            base = 0;
        } else if (whence == SEEK_CUR) {
            base = readptr;
        } else if (whence == SEEK_END) {
            base = size;
        } else {
            return aff4::GENERIC_ERROR;
        }

        int64_t target = base + offset;
        if (target < 0) return aff4::GENERIC_ERROR;

        uint64_t absolute = static_cast<uint64_t>(target);
        if (absolute > static_cast<uint64_t>(size)) {
            absolute = static_cast<uint64_t>(size);
        }

        uint64_t walked = 0;
        for (size_t i = 0; i < files_.size(); ++i) {
            uint64_t part_size = sizes_[i];
            if (absolute <= walked + part_size) {
                current_part_ = i;
                current_part_offset_ = absolute - walked;
                if (fseek(files_[i], static_cast<long>(current_part_offset_), SEEK_SET) != 0) {
                    return aff4::GENERIC_ERROR;
                }
                for (size_t j = i + 1; j < files_.size(); ++j) {
                    if (fseek(files_[j], 0, SEEK_SET) != 0) return aff4::GENERIC_ERROR;
                }
                readptr = static_cast<aff4::aff4_off_t>(absolute);
                return aff4::STATUS_OK;
            }
            walked += part_size;
        }

        current_part_ = files_.size();
        current_part_offset_ = 0;
        readptr = size;
        return aff4::STATUS_OK;
    }

    aff4::AFF4Status Write(const char*, size_t) override {
        return aff4::NOT_IMPLEMENTED;
    }

    aff4::aff4_off_t Size() const override {
        return size;
    }

private:
    void CloseAll() {
        for (FILE* f : files_) {
            if (f) fclose(f);
        }
        files_.clear();
        sizes_.clear();
        current_part_ = 0;
        current_part_offset_ = 0;
    }

    std::vector<FILE*> files_;
    std::vector<uint64_t> sizes_;
    size_t current_part_ = 0;
    uint64_t current_part_offset_ = 0;
};

class MultipartOutputStream : public aff4::AFF4Stream {
public:
    MultipartOutputStream(aff4::DataStore* resolver, aff4::URN urn)
        : aff4::AFF4Stream(resolver, urn) {
        properties.writable = true;
        properties.seekable = false;
        properties.sizeable = true;
    }

    ~MultipartOutputStream() override {
        CloseAll();
    }

    aff4::AFF4Status Open(const std::vector<std::string>& output_paths,
                          const std::vector<uint64_t>& part_sizes) {
        CloseAll();
        if (output_paths.empty() || output_paths.size() != part_sizes.size()) {
            return aff4::GENERIC_ERROR;
        }

        part_sizes_ = part_sizes;
        part_index_ = 0;
        part_written_ = 0;
        readptr = 0;
        size = 0;

        for (const auto& path : output_paths) {
            std::filesystem::path parent = std::filesystem::path(path).parent_path();
            if (!parent.empty()) {
                std::error_code ec;
                std::filesystem::create_directories(parent, ec);
            }
            FILE* f = fopen(path.c_str(), "wb");
            if (!f) {
                CloseAll();
                return aff4::GENERIC_ERROR;
            }
            files_.push_back(f);
        }

        return aff4::STATUS_OK;
    }

    aff4::AFF4Status Write(const char* data, size_t length) override {
        if (!data) return aff4::GENERIC_ERROR;
        size_t consumed = 0;

        while (consumed < length) {
            if (part_index_ >= files_.size()) {
                return aff4::GENERIC_ERROR;
            }

            uint64_t remaining_in_part = part_sizes_[part_index_] - part_written_;
            if (remaining_in_part == 0) {
                ++part_index_;
                part_written_ = 0;
                continue;
            }

            size_t to_write = static_cast<size_t>(
                std::min<uint64_t>(remaining_in_part, static_cast<uint64_t>(length - consumed)));
            size_t n = fwrite(data + consumed, 1, to_write, files_[part_index_]);
            if (n != to_write) {
                return aff4::GENERIC_ERROR;
            }

            consumed += n;
            part_written_ += static_cast<uint64_t>(n);
            size += static_cast<aff4::aff4_off_t>(n);
            readptr = size;
        }

        return aff4::STATUS_OK;
    }

    aff4::AFF4Status ReadBuffer(char*, size_t*) override {
        return aff4::NOT_IMPLEMENTED;
    }

    aff4::AFF4Status Seek(aff4::aff4_off_t, int) override {
        return aff4::NOT_IMPLEMENTED;
    }

    aff4::aff4_off_t Size() const override {
        return size;
    }

private:
    void CloseAll() {
        for (FILE* f : files_) {
            if (f) fclose(f);
        }
        files_.clear();
        part_sizes_.clear();
        part_index_ = 0;
        part_written_ = 0;
    }

    std::vector<FILE*> files_;
    std::vector<uint64_t> part_sizes_;
    size_t part_index_ = 0;
    uint64_t part_written_ = 0;
};

std::string SanitizeComponent(std::string value) {
    std::replace(value.begin(), value.end(), '/', '_');
    std::replace(value.begin(), value.end(), ':', '_');
    return value;
}

bool IsTerminalInteractive() {
    return isatty(STDIN_FILENO) == 1;
}

bool ParseNumericPartExtension(const std::string& path, int* part_number, int* width) {
    std::filesystem::path p(path);
    std::string ext = p.extension().string();
    if (ext.size() < 2 || ext[0] != '.') return false;
    std::string digits = ext.substr(1);
    if (digits.size() < 3) return false;
    for (char c : digits) {
        if (!std::isdigit(static_cast<unsigned char>(c))) return false;
    }
    *part_number = std::stoi(digits);
    *width = static_cast<int>(digits.size());
    return true;
}

bool HasEwfSegmentExtension(const std::string& path) {
    std::filesystem::path p(path);
    std::string ext = p.extension().string();
    if (ext.size() != 4 || ext[0] != '.') return false;
    char kind = static_cast<char>(std::tolower(static_cast<unsigned char>(ext[1])));
    if (kind != 'e') return false;
    return std::isdigit(static_cast<unsigned char>(ext[2])) &&
           std::isdigit(static_cast<unsigned char>(ext[3]));
}

bool HasAff4Extension(const std::string& path) {
    std::string ext = std::filesystem::path(path).extension().string();
    std::transform(ext.begin(), ext.end(), ext.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return ext == ".aff4";
}

std::string NormalizeEwfOutputBasePath(const std::string& output_path) {
    if (!HasEwfSegmentExtension(output_path)) {
        return output_path;
    }
    std::filesystem::path p(output_path);
    p.replace_extension();
    return p.string();
}

std::string FormatPartExtension(int part, int width) {
    std::ostringstream out;
    out << '.' << std::setw(width) << std::setfill('0') << part;
    return out.str();
}

std::vector<std::string> GlobRawNumericSegments(const std::string& input_path) {
    int part = 0;
    int width = 0;
    if (!ParseNumericPartExtension(input_path, &part, &width)) {
        return {input_path};
    }

    std::filesystem::path input(input_path);
    std::filesystem::path dir = input.parent_path();
    std::filesystem::path stem = input;
    stem.replace_extension();
    std::string base = stem.filename().string();

    std::vector<std::string> result;
    int current = part;
    while (true) {
        std::filesystem::path candidate = dir / (base + FormatPartExtension(current, width));
        if (!std::filesystem::exists(candidate)) {
            break;
        }
        result.push_back(candidate.string());
        ++current;
    }

    if (result.empty()) {
        result.push_back(input_path);
    }
    return result;
}

std::vector<uint64_t> GetFileSizes(const std::vector<std::string>& paths) {
    std::vector<uint64_t> sizes;
    sizes.reserve(paths.size());
    for (const auto& p : paths) {
        std::error_code ec;
        uint64_t sz = std::filesystem::file_size(p, ec);
        if (ec) return {};
        sizes.push_back(sz);
    }
    return sizes;
}

std::string MapNameForUrn(const aff4::URN& image_urn) {
    std::string clean_urn = image_urn.value;
    std::replace(clean_urn.begin(), clean_urn.end(), '/', '_');
    std::replace(clean_urn.begin(), clean_urn.end(), ':', '_');
    return "map_" + clean_urn + ".map.lz4";
}

std::string JoinLines(const std::vector<std::string>& values) {
    std::ostringstream out;
    for (size_t i = 0; i < values.size(); ++i) {
        out << values[i];
        if (i + 1 < values.size()) out << '\n';
    }
    return out.str();
}

std::vector<std::string> SplitLines(const std::string& data) {
    std::vector<std::string> lines;
    std::istringstream in(data);
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty()) lines.push_back(line);
    }
    return lines;
}

std::vector<uint64_t> ParseUint64Lines(const std::string& data) {
    std::vector<uint64_t> out;
    for (const auto& line : SplitLines(data)) {
        try {
            out.push_back(static_cast<uint64_t>(std::stoull(line)));
        } catch (...) {
            return {};
        }
    }
    return out;
}

std::vector<std::string> BasenamesOnly(const std::vector<std::string>& paths) {
    std::vector<std::string> names;
    names.reserve(paths.size());
    for (const auto& p : paths) {
        names.push_back(std::filesystem::path(p).filename().string());
    }
    return names;
}

std::string FindCompanionTxt(const std::string& input_path) {
    std::filesystem::path p(input_path);
    std::filesystem::path txt = p.parent_path() / (p.stem().string() + ".txt");
    if (std::filesystem::exists(txt)) {
        return txt.string();
    }
    return "";
}

std::vector<std::string> BuildMultipartOutputPaths(
    const std::string& output_path,
    const std::vector<std::string>& original_basenames) {
    std::vector<std::string> paths;
    if (original_basenames.empty()) return paths;

    std::filesystem::path out(output_path);
    bool output_is_dir = false;
    if (std::filesystem::exists(out) && std::filesystem::is_directory(out)) {
        output_is_dir = true;
    } else if (!output_path.empty() && output_path.back() == '/') {
        output_is_dir = true;
    }

    if (output_is_dir) {
        for (const auto& name : original_basenames) {
            paths.push_back((out / name).string());
        }
        return paths;
    }

    std::filesystem::path first_original(original_basenames.front());
    std::string first_ext = first_original.extension().string();

    if (!out.has_extension() && !first_ext.empty()) {
        paths.push_back(out.string() + first_ext);
    } else {
        paths.push_back(out.string());
    }
    std::filesystem::path stem = out;
    stem.replace_extension();
    for (size_t i = 1; i < original_basenames.size(); ++i) {
        std::filesystem::path original(original_basenames[i]);
        paths.push_back((stem.string() + original.extension().string()));
    }
    return paths;
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
    std::cerr << "  " << argv0 << " --archive-img|-a <dir> [--all-segments|-S] <inputs...>\n";
    std::cerr << "  " << argv0 << " --extract-img|-x <dir> <map_name> <output_path>\n";
    std::cerr << "  " << argv0 << " --info <dir>\n";
    std::cerr << "  " << argv0 << " --help|-h\n";
    std::cerr << "\nFlags:\n";
    std::cerr << "  --all-segments, -S    For multi-volume images,\n";
    std::cerr << "                        automatically load all segment files.\n";
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
        } else if (arg == "--all-segments" || arg == "-S") {
            options->auto_all_segments = true;
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
        std::string lower_path = input_path;
        std::transform(lower_path.begin(), lower_path.end(), lower_path.begin(), ::tolower);
        bool is_ewf = (lower_path.length() >= 4 && lower_path.substr(lower_path.length() - 4) == ".e01");
        bool is_aff4 = HasAff4Extension(input_path);
        int raw_part_number = 0;
        int raw_part_width = 0;
        bool is_raw_numeric_part = ParseNumericPartExtension(input_path, &raw_part_number, &raw_part_width);

        aff4::URN image_urn = BuildImageUrn(input_path);
        aff4::AFF4Stream* active_stream = nullptr;
        aff4::AFF4Flusher<aff4::FileBackedObject> file_stream;
        imgformatlib::EwfStream ewf_stream(&resolver, image_urn);
        MultipartInputStream raw_multipart_stream(&resolver, image_urn);

        std::string source_layout = kLayoutRawSingle;
        std::vector<std::string> source_parts = {input_path};
        std::vector<uint64_t> source_part_sizes;

        if (is_ewf) {
            std::cout << "Detected EWF file, routing through imgformatlib::EwfStream...\n";
            std::vector<std::string> paths = {input_path};

            std::vector<std::string> segments = imgformatlib::EwfStream::GlobSegments(input_path);
            if (segments.size() > 1) {
                if (options.auto_all_segments) {
                    std::cout << "Found " << segments.size() << " segments for " << input_path << ". Loading all automatically (--all-segments):\n";
                    for (const auto& seg : segments) std::cout << "  " << seg << "\n";
                    paths = segments;
                } else {
                    if (IsTerminalInteractive()) {
                        std::cout << "\nFound " << segments.size() - 1 << " continuing image segments for " << input_path << ":\n";
                        for (size_t i = 1; i < segments.size(); ++i) std::cout << "  " << segments[i] << "\n";
                        std::cout << "\nDo you want to load all segments? (y/n): ";
                        std::string response;
                        std::cin >> response;
                        if (response == "y" || response == "Y") {
                            paths = segments;
                            std::cout << "Loading all " << paths.size() << " segments...\n";
                        } else {
                            std::cout << "Loading only the primary file: " << input_path << "\n";
                        }
                    } else {
                        std::cout << "Found additional EWF segments but running non-interactive; loading only primary file: "
                                  << input_path << "\n";
                    }
                }
            }

            if (ewf_stream.OpenRead(paths) != aff4::STATUS_OK) {
                std::cerr << "Failed to open EWF: " << input_path << "\n";
                return 1;
            }
            active_stream = &ewf_stream;
            source_layout = kLayoutEwf;
            source_parts = paths;
            source_part_sizes.clear();
        } else if (is_aff4) {
            std::cout << "Detected AFF4 file, ingesting container bytes as raw stream...\n";
            if (aff4::NewFileBackedObject(&resolver, input_path, "read", file_stream) != aff4::STATUS_OK) {
                std::cerr << "Failed to open AFF4 input: " << input_path << "\n";
                return 1;
            }
            active_stream = file_stream.get();
            source_layout = kLayoutAff4;
            source_parts = {input_path};
            source_part_sizes.clear();
        } else if (is_raw_numeric_part && raw_part_number == 1) {
            std::vector<std::string> segments = GlobRawNumericSegments(input_path);
            std::vector<std::string> paths = {input_path};

            if (segments.size() > 1) {
                if (options.auto_all_segments) {
                    std::cout << "Found " << segments.size() << " raw image parts for " << input_path
                              << ". Loading all automatically (--all-segments):\n";
                    for (const auto& seg : segments) std::cout << "  " << seg << "\n";
                    paths = segments;
                } else if (IsTerminalInteractive()) {
                    std::cout << "\nFound " << segments.size() - 1 << " continuing raw image parts for "
                              << input_path << ":\n";
                    for (size_t i = 1; i < segments.size(); ++i) std::cout << "  " << segments[i] << "\n";
                    std::cout << "\nDo you want to load all parts? (y/n): ";
                    std::string response;
                    std::cin >> response;
                    if (response == "y" || response == "Y") {
                        paths = segments;
                        std::cout << "Loading all " << paths.size() << " parts...\n";
                    } else {
                        std::cout << "Loading only the primary file: " << input_path << "\n";
                    }
                } else {
                    std::cout << "Found additional raw parts but running non-interactive; loading only primary file: "
                              << input_path << "\n";
                }
            }

            if (paths.size() > 1) {
                if (raw_multipart_stream.Open(paths) != aff4::STATUS_OK) {
                    std::cerr << "Failed to open multipart raw input starting at: " << input_path << "\n";
                    return 1;
                }
                source_part_sizes = GetFileSizes(paths);
                if (source_part_sizes.size() != paths.size()) {
                    std::cerr << "Failed to stat one or more raw image parts for: " << input_path << "\n";
                    return 1;
                }
                active_stream = &raw_multipart_stream;
                source_layout = kLayoutRawMultipart;
                source_parts = paths;
            } else {
                if (aff4::NewFileBackedObject(&resolver, input_path, "read", file_stream) != aff4::STATUS_OK) {
                    std::cerr << "Failed to open input: " << input_path << "\n";
                    return 1;
                }
                active_stream = file_stream.get();
                source_layout = kLayoutRawSingle;
                source_parts = {input_path};
            }
        } else {
            if (aff4::NewFileBackedObject(&resolver, input_path, "read", file_stream) != aff4::STATUS_OK) {
                std::cerr << "Failed to open input: " << input_path << "\n";
                return 1;
            }
            active_stream = file_stream.get();
            source_layout = kLayoutRawSingle;
            source_parts = {input_path};
        }

        // IngestStream computes and stores SHA-256 in a single pass — no separate pre-read needed.
        std::string computed_sha256;
        if (archive_store.IngestStream(active_stream, image_urn, &computed_sha256) != aff4::STATUS_OK) {
            std::cerr << "Failed to ingest input: " << input_path << "\n";
            return 1;
        }
        std::cout << "Ingested SHA-256 (raw media): " << computed_sha256 << "\n";

        const std::string map_name = MapNameForUrn(image_urn);
        if (archive_store.StoreRawMetadata("source_layout:" + map_name, source_layout) != aff4::STATUS_OK) {
            std::cerr << "Failed to store source layout metadata for " << map_name << "\n";
            return 1;
        }
        if (archive_store.StoreRawMetadata("source_parts:" + map_name, JoinLines(BasenamesOnly(source_parts))) != aff4::STATUS_OK) {
            std::cerr << "Failed to store source parts metadata for " << map_name << "\n";
            return 1;
        }
        if (!source_part_sizes.empty()) {
            std::vector<std::string> sizes;
            sizes.reserve(source_part_sizes.size());
            for (uint64_t sz : source_part_sizes) sizes.push_back(std::to_string(sz));
            if (archive_store.StoreRawMetadata("source_part_sizes:" + map_name, JoinLines(sizes)) != aff4::STATUS_OK) {
                std::cerr << "Failed to store source part sizes metadata for " << map_name << "\n";
                return 1;
            }
        }

        // Store companion .txt metadata file (case details, examiner, etc.) if present.
        std::string txt_path;
        if (is_ewf) {
            txt_path = imgformatlib::EwfStream::FindInfoTxt(input_path);
        } else if (!is_aff4) {
            txt_path = FindCompanionTxt(input_path);
        }
        if (!txt_path.empty()) {
            std::ifstream txt_file(txt_path);
            std::string txt_contents((std::istreambuf_iterator<char>(txt_file)),
                                      std::istreambuf_iterator<char>());
            if (archive_store.StoreRawMetadata("sidecar_txt:" + map_name, txt_contents) == aff4::STATUS_OK) {
                std::cout << "Stored sidecar info file: " << txt_path << "\n";
            }
            if (is_ewf) {
                // Preserve compatibility with older EWF-specific key.
                archive_store.StoreRawMetadata("ewf_info_txt:" + map_name, txt_contents);
            }
        }
    }

    return 0;
}

// Extracts the original file extension embedded in a map name.
// Map names are of the form: map_<sanitized_path>.<ext>.map.lz4
// e.g. "map_aff4___archive__..._Test3E01.E01.map.lz4" → ".E01"
std::string ExtractExtensionFromMapName(const std::string& map_name) {
    const std::string suffix = ".map.lz4";
    if (map_name.size() <= suffix.size()) return "";
    std::string base = map_name.substr(0, map_name.size() - suffix.size());
    size_t dot = base.rfind('.');
    if (dot == std::string::npos) return "";
    return base.substr(dot); // e.g. ".E01"
}

int RunExtract(const CliOptions& options) {
    aff4::MemoryDataStore resolver;
    aff4::ArchiveExtractor extractor(options.extract_dir);

    std::string output_path = options.output_path;

    std::string source_layout;
    bool have_layout = (extractor.RetrieveRawMetadata("source_layout:" + options.map_name, &source_layout) == aff4::STATUS_OK);
    bool is_ewf = have_layout && source_layout == kLayoutEwf;
    bool is_aff4 = have_layout && source_layout == kLayoutAff4;
    bool is_raw_multipart = have_layout && source_layout == kLayoutRawMultipart;

    if (!have_layout) {
        std::string detected_ext = ExtractExtensionFromMapName(options.map_name);
        std::filesystem::path out_fs(output_path);
        if (out_fs.extension().empty() && !detected_ext.empty()) {
            output_path += detected_ext;
            std::cout << "Auto-detected original format extension '" << detected_ext
                      << "', writing to: " << output_path << "\n";
        }

        std::string lower_path = output_path;
        std::transform(lower_path.begin(), lower_path.end(), lower_path.begin(), ::tolower);
        is_ewf = (lower_path.length() >= 4 && lower_path.substr(lower_path.length() - 4) == ".e01");
        is_aff4 = HasAff4Extension(output_path);
    }

    aff4::AFF4Stream* active_stream = nullptr;
    aff4::AFF4Flusher<aff4::FileBackedObject> file_stream;
    imgformatlib::EwfStream ewf_stream(&resolver, aff4::URN("aff4://extract/"));
    MultipartOutputStream multipart_output_stream(&resolver, aff4::URN("aff4://extract/multipart"));

    std::vector<std::string> restored_part_paths;

    if (is_ewf) {
        const std::string ewf_output_base = NormalizeEwfOutputBasePath(output_path);
        if (ewf_output_base != output_path) {
            std::cout << "Detected explicit EWF segment extension; using base output path: "
                      << ewf_output_base << "\n";
        }

        // Get the logical size from the map so libewf can properly initialise the write handle.
        uint64_t logical_size = 0;
        if (extractor.GetMapLogicalSize(options.map_name, &logical_size) != aff4::STATUS_OK) {
            std::cerr << "Warning: could not read logical size from map — writing without media_size hint.\n";
        }

        if (ewf_stream.OpenWrite(ewf_output_base, logical_size) != aff4::STATUS_OK) {
            std::cerr << "Failed to open EWF for writing: " << ewf_output_base << "\n";
            return 1;
        }
        active_stream = &ewf_stream;
    } else if (is_aff4) {
        if (aff4::NewFileBackedObject(&resolver, output_path, "truncate", file_stream) != aff4::STATUS_OK) {
            std::cerr << "Failed to open AFF4 output file: " << output_path << "\n";
            return 1;
        }
        active_stream = file_stream.get();
    } else if (is_raw_multipart) {
        std::string parts_blob;
        std::string sizes_blob;
        if (extractor.RetrieveRawMetadata("source_parts:" + options.map_name, &parts_blob) != aff4::STATUS_OK ||
            extractor.RetrieveRawMetadata("source_part_sizes:" + options.map_name, &sizes_blob) != aff4::STATUS_OK) {
            std::cerr << "Failed to retrieve multipart metadata for map: " << options.map_name << "\n";
            return 1;
        }

        std::vector<std::string> part_names = SplitLines(parts_blob);
        std::vector<uint64_t> part_sizes = ParseUint64Lines(sizes_blob);
        if (part_names.empty() || part_names.size() != part_sizes.size()) {
            std::cerr << "Invalid multipart metadata stored for map: " << options.map_name << "\n";
            return 1;
        }

        restored_part_paths = BuildMultipartOutputPaths(output_path, part_names);
        if (restored_part_paths.size() != part_sizes.size()) {
            std::cerr << "Failed to construct multipart output paths for extraction\n";
            return 1;
        }

        if (multipart_output_stream.Open(restored_part_paths, part_sizes) != aff4::STATUS_OK) {
            std::cerr << "Failed to open multipart output files for extraction\n";
            return 1;
        }
        active_stream = &multipart_output_stream;
    } else {
        if (aff4::NewFileBackedObject(&resolver, output_path, "truncate", file_stream) != aff4::STATUS_OK) {
            std::cerr << "Failed to open output file: " << output_path << "\n";
            return 1;
        }
        active_stream = file_stream.get();
    }

    if (extractor.ExtractMap(options.map_name, active_stream) != aff4::STATUS_OK) {
        std::cerr << "Failed to extract map: " << options.map_name << "\n";
        return 1;
    }

    // Restore companion .txt metadata file alongside the extracted image.
    std::string txt_contents;
    bool have_sidecar = extractor.RetrieveRawMetadata("sidecar_txt:" + options.map_name, &txt_contents) == aff4::STATUS_OK;
    if (!have_sidecar && is_ewf) {
        have_sidecar = extractor.RetrieveRawMetadata("ewf_info_txt:" + options.map_name, &txt_contents) == aff4::STATUS_OK;
    }
    if (have_sidecar && !txt_contents.empty()) {
        std::filesystem::path txt_out;
        if (!restored_part_paths.empty()) {
            txt_out = std::filesystem::path(restored_part_paths.front()).replace_extension(".txt");
        } else {
            txt_out = std::filesystem::path(output_path).replace_extension(".txt");
        }
        std::ofstream ofs(txt_out);
        if (ofs) {
            ofs << txt_contents;
            std::cout << "Restored sidecar info file: " << txt_out.string() << "\n";
        }
    }

    return 0;
}



#pragma pack(push, 1)
struct MapStreamHeader {
    uint8_t magic[8];
    uint64_t logical_size;
    uint32_t segment_count;
    uint32_t reserved;
};

struct SegmentHeader {
    uint64_t logical_offset;
    uint32_t chunk_count;
};

struct ChunkRef {
    uint32_t bevy_id;
    uint64_t offset;
    uint32_t uncompressed_size;
};

struct MapStreamFooter {
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
    if (!map_file || uncompressed_size < sizeof(MapStreamHeader) + sizeof(SegmentHeader) + sizeof(MapStreamFooter)) {
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

    const auto* header = reinterpret_cast<const MapStreamHeader*>(uncompressed.data());
    const auto* segment = reinterpret_cast<const SegmentHeader*>(uncompressed.data() + sizeof(MapStreamHeader));
    const auto* footer = reinterpret_cast<const MapStreamFooter*>(uncompressed.data() + uncompressed_size - sizeof(MapStreamFooter));
    const auto* refs = reinterpret_cast<const ChunkRef*>(uncompressed.data() + sizeof(MapStreamHeader) + sizeof(SegmentHeader));

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
