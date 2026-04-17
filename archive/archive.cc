#include "archive/archive.h"
#include <blake3.h>
#include <lz4.h>
#include <openssl/evp.h>
#include <iostream>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <thread>
#include <algorithm>
#include <string>
#include <vector>
#include <sstream>
#include <iomanip>
#include <filesystem>
#include <rocksdb/options.h>

namespace aff4 {

// -- FastCDC Gear Matrix --
static const uint64_t GEAR_MATRIX[256] = {
    0xa7bfd7474eccec4eULL, 0x1ecb62dcdb3d79ecULL, 0x84f73e63de8c6aeeULL, 0x0b0302f2548bbac9ULL, 
    0x99fca7bf6a4da39fULL, 0x6f0f9d3e85eb600eULL, 0xcadc9574eb0570d6ULL, 0x8f79bb5c6be6f40cULL, 
    0x6ab087be0aa1f4bcULL, 0xf14a1958e0974c12ULL, 0x325bfbcb35b51755ULL, 0x97a18005e41b3741ULL, 
    0xcd80fdcd05f8194bULL, 0x6956e9a02af14a5aULL, 0x8accb79f865cc678ULL, 0x44f71c0c99f2a881ULL, 
    0xb9aa9309da2a8e54ULL, 0x1f0dc1674f9b0d37ULL, 0xf91ca8a373d274fcULL, 0x13e549c6b8c509e8ULL, 
    0x9f4d6159e7dbac14ULL, 0xdef7f8fff470e40eULL, 0xf48646609ede43e6ULL, 0x6068c2509f226644ULL, 
    0xca2b7edc5c16d3eaULL, 0xff6c1f5b35c200c3ULL, 0xacb35b68d9e82ec7ULL, 0xa8348716a61e9a08ULL, 
    0x48e4f61137f682f0ULL, 0x186ab074f33c2e09ULL, 0xed012935b6161238ULL, 0x5e5d365a88f5cc0eULL, 
    0x7c41b6f1d1ed99c7ULL, 0x7b38b5e92571e7f8ULL, 0x7d5d7dfcb3fd3dbcULL, 0x81067c0e56e20f10ULL, 
    0xdc1960654b803a97ULL, 0x2f537265dae91199ULL, 0x98278f234a2afc8fULL, 0xd91d82aa483e6af5ULL, 
    0x3892d4b526c3178fULL, 0x69964288dbd9926dULL, 0x8783022867adc715ULL, 0xa18ec017ec844aecULL, 
    0x51ff3185b220a8d0ULL, 0xdb81f9a85bdb8a10ULL, 0xd59282981c1ac382ULL, 0x005af2d6e5871054ULL, 
    0x734548cf55ecebe1ULL, 0x59297f798c11c0afULL, 0xc23a964d94467fa8ULL, 0x570cd10b59eba3f1ULL, 
    0x6d3562081fd3c4c3ULL, 0x2d2c06d2f286e82bULL, 0xee6b67b399c7224cULL, 0xc9d0eefd29d92e57ULL, 
    0x5f60acb21380805dULL, 0x03e94b9e9d418169ULL, 0xcf915edeae92cc8aULL, 0xd65c11390e17b81eULL, 
    0x7eb98255d5c5cc5dULL, 0xfc5c1262568df94aULL, 0x3b016a3e8ccf24c0ULL, 0x8fc85c9cf7a337c4ULL, 
    0x1e41fa8c55c81f04ULL, 0xf9ceeb77eb4df6a1ULL, 0x0f6ccc1d10f3e947ULL, 0x9cb0d9ffecad2f82ULL, 
    0x95ddd4a2188dbf85ULL, 0xfa97d4dd34463b09ULL, 0x86e94c7cc32f3a7aULL, 0x9d359cbd68ff2e89ULL, 
    0x3c6b58f317b359e3ULL, 0xaf954de953288ef3ULL, 0x9b39e4cb1925025aULL, 0x574b0a9031d1a479ULL, 
    0xc15c34785f8b4ed6ULL, 0xb8817aa958beceafULL, 0x3a92f34d62859d73ULL, 0x4cb6dec0bb58e26bULL, 
    0x4d1719365f310bc6ULL, 0x85f9177138efb920ULL, 0x899546a045bbf812ULL, 0x46a40a53effcafbcULL, 
    0x7befd0037609b257ULL, 0x64667253825f88b7ULL, 0xee77c8755f2dffeaULL, 0xa39349cd15f6210bULL, 
    0x41c15d5dd559c053ULL, 0x5ae996285a0b6b47ULL, 0xa7accfdc068fa757ULL, 0xb2f0289b44598549ULL, 
    0xc4885cf760c98306ULL, 0xfe7e3b26a6f805b8ULL, 0x86573b3e32a636ebULL, 0x529a9ebca8b24030ULL, 
    0x2c40b83d24a2d972ULL, 0x2befd04b83df0950ULL, 0x9ff8076235033ebdULL, 0xba10b1e8ce1ac5b8ULL, 
    0xcdbd19998c0a9e37ULL, 0x96e4cf8539aee9a9ULL, 0x61c0086f05c5401aULL, 0x3a1ec8bf17b023faULL, 
    0xb59014364dbd1ec7ULL, 0x26dfc491a2253e81ULL, 0xe1f52e6bc144f21fULL, 0x1e538b8afcace746ULL, 
    0x18fdc4308ed4a378ULL, 0xe1f8b1b1da062c04ULL, 0x3c18742af0ff7e31ULL, 0xba66041dc0bf9b8bULL, 
    0x5977130fa07cd0f8ULL, 0x1c788e8f66f63bb1ULL, 0x4b80e824949179e1ULL, 0x0de8039db80c1579ULL, 
    0xc54867e6d87181c4ULL, 0xd992eb6badd864d5ULL, 0x4ac6b251b85efd5bULL, 0x62bff12a48cb4de1ULL, 
    0x39ee30036c1c2907ULL, 0xb618117cf9a0ff9cULL, 0x732aad9503984ea9ULL, 0x6ec4e359f896d406ULL, 
    0xbc8c3b34ff5e5807ULL, 0x487381e58bba74a8ULL, 0xf5daf321430bcd97ULL, 0x99f1fa0ad2f5db86ULL, 
    0x2e82565f80d1625fULL, 0x13d673b614d23ea3ULL, 0x266bf5b14ac745e1ULL, 0x757108cf0c102429ULL, 
    0xc9c054e5d3c2b363ULL, 0xb1386d2ed45bc530ULL, 0x4ff216f6f7bc28a8ULL, 0x56fbcd175a6a3a71ULL, 
    0x5a0ecc5e5eb9ed44ULL, 0xd66cfb33117e9b9dULL, 0x1815c07fe2c09a00ULL, 0x5edc47e7fc3174b5ULL, 
    0x780411cead6e016eULL, 0x19ed57497f65d1ddULL, 0x6c855f07382c6c40ULL, 0x92cf9f97ba701241ULL, 
    0x104a71821383aa94ULL, 0xe93863998b2baefaULL, 0x31646608e20d5012ULL, 0xbbad0cbda218446dULL, 
    0x60a4b591d6c8e2d3ULL, 0x0b87f0b610086db8ULL, 0xfdbaa56491dd60d8ULL, 0x1e15c5f1c4a6e8ddULL, 
    0xeec90b602c92ad27ULL, 0x290a1d1bb6061875ULL, 0xe45823077475d5a8ULL, 0x435d3d729eaa863fULL, 
    0x987c741dfd7699d6ULL, 0xcc0e30c311c12649ULL, 0x6321c8e4f7ca25b0ULL, 0xa362f6ea52aad2ffULL, 
    0x749c9e62fe321fefULL, 0x023091fb12b7ef2bULL, 0x0f06078faae19158ULL, 0x7e52ef4cbd91b9b7ULL, 
    0x10b0a722dbb85cc5ULL, 0x7a44209dd866eff1ULL, 0xa57ed4ebcacd36c1ULL, 0xa8d5441c452d3d12ULL, 
    0x01b6a495e3a170adULL, 0x5defd9a9b989af90ULL, 0xa9e63526b7bdabc7ULL, 0x448e7f8ee2cfc9c1ULL, 
    0x166588d748150d57ULL, 0x5ab3a2d53d723975ULL, 0x5753cf65f6f92563ULL, 0x62a1b9becc950fe0ULL, 
    0xc4905d570c0b6f66ULL, 0x56600a2f3f6ce6d1ULL, 0x94c2c3d2c8b282b0ULL, 0x8f2358d5f612471dULL, 
    0x40ea507493076b9dULL, 0x68c5e7bd428d0e2aULL, 0x4dcebc2862113cbcULL, 0xfa65e42ff4687f4dULL, 
    0x736a2dc3e1ecfe44ULL, 0x611313e421fa23a8ULL, 0x0c100687bdcd0d9dULL, 0xb764cc2a475f2575ULL, 
    0x87b0050f645b5bf3ULL, 0x9323dc3a637eb975ULL, 0xace954ef96ef9b3bULL, 0x476c9ccaa84a1ef7ULL, 
    0x949c28d60de27b61ULL, 0xd233674cad5db45dULL, 0x5edf097180dfe05aULL, 0x7543446ab6263b0cULL, 
    0xb1afd01c747b60c4ULL, 0x049169c40c94dd05ULL, 0xab10776e17738f12ULL, 0x1774d0b5374b35a1ULL, 
    0x55c0c19cb2a247c5ULL, 0xd4b455cc4f79df92ULL, 0xf140110c2906983cULL, 0x518480db58b7b792ULL, 
    0x1b3776d4c17ae70eULL, 0x3fef67f72772d9d1ULL, 0x7ded801eaf26fadcULL, 0x3f94487cc9260e8eULL, 
    0xe5eb60cf02d68aabULL, 0x0a5e81554fe25161ULL, 0xa1188999156f1b9fULL, 0xddb92d31f778944aULL, 
    0x4d9faf9d15ec00d3ULL, 0x68057ecfbff6dd68ULL, 0x56ac85683d56b0ccULL, 0x79359860cfa3e654ULL, 
    0x421d10429bfc3e48ULL, 0xb79db33cedcce163ULL, 0x4a7cc727bedc2335ULL, 0xb0894b186ec7ed1fULL, 
    0x5f2f534677b19bf5ULL, 0x7df8a9a4f6de0245ULL, 0xa4167cba9cbeac7dULL, 0xe97f9c8d4b9b3b18ULL, 
    0x191b489a96655dd5ULL, 0x0b37fab62fb82b09ULL, 0x158f9bb31a7d6a98ULL, 0x18d485ce03170dbbULL, 
    0xac8c2b31eee2b87fULL, 0x7897498942159c5fULL, 0xbc86a02c6b2da441ULL, 0x60f04411346ac19eULL, 
    0x92af7a9361964f27ULL, 0x699835702eb9773fULL, 0xcaeeb1db468afa26ULL, 0xa3da52af5d552905ULL, 
    0x7bd2f9bc87a44529ULL, 0x9eeee3f66a9e269cULL, 0xb8959db6a708415eULL, 0x57c411e0b732bcb2ULL, 
    0x289c8e1517ed2a23ULL, 0x52ff65f84581da63ULL, 0xe7b11a1a53234050ULL, 0x0c6bc6db001f8ffaULL, 
    0x6fc7747c75063af9ULL, 0xaf3a3cd775f3b967ULL, 0xb30edab220b4d4b2ULL, 0xb60ad166036091a1ULL, 
    0x4614742def67e4b3ULL, 0x189d3897f1ca174dULL, 0xe664a47bb0117aeeULL, 0xde4211c7faa38af3ULL, 
    0xa1d8c675a133c6e2ULL, 0x268c2860229f520dULL, 0xd2c3eab3b219c3e8ULL, 0x6ac494eec81041aeULL
};

namespace {

constexpr uint32_t kCdcMinSize = 128 * 1024;
constexpr uint32_t kCdcTargetSize = 2 * 1024 * 1024;
constexpr uint32_t kCdcMaxSize = 4 * 1024 * 1024;
constexpr uint64_t kCdcEarlyMask = (1ULL << 20) - 1;
constexpr uint64_t kCdcLateMask = (1ULL << 21) - 1;

struct JournalEntry {
    std::string state;
    uint32_t bevy_id = 0;
    uint64_t offset = 0;
    uint32_t compressed_size = 0;
    uint32_t uncompressed_size = 0;
    uint32_t flags = 0;
    uint8_t hash[32] = {};
};

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

    for (size_t i = 0; i < out_len; ++i) {
        auto parse_nibble = [](char ch) -> int {
            if (ch >= '0' && ch <= '9') return ch - '0';
            if (ch >= 'a' && ch <= 'f') return 10 + (ch - 'a');
            if (ch >= 'A' && ch <= 'F') return 10 + (ch - 'A');
            return -1;
        };

        int high = parse_nibble(hex[i * 2]);
        int low = parse_nibble(hex[i * 2 + 1]);
        if (high < 0 || low < 0) {
            return false;
        }
        out[i] = static_cast<uint8_t>((high << 4) | low);
    }

    return true;
}

std::string BuildJournalKey(uint32_t bevy_id, uint64_t offset) {
    return "journal/" + std::to_string(bevy_id) + "/" + std::to_string(offset);
}

std::string BuildJournalValue(const JournalEntry& entry) {
    return entry.state + "|" + std::to_string(entry.bevy_id) + "|" +
           std::to_string(entry.offset) + "|" + std::to_string(entry.compressed_size) +
           "|" + std::to_string(entry.uncompressed_size) + "|" +
           std::to_string(entry.flags) + "|" + BytesToHex(entry.hash, sizeof(entry.hash));
}

bool ParseJournalValue(const std::string& value, JournalEntry* entry) {
    if (!entry) {
        return false;
    }

    std::vector<std::string> parts;
    size_t start = 0;
    while (true) {
        size_t pos = value.find('|', start);
        parts.push_back(value.substr(start, pos == std::string::npos ? std::string::npos : pos - start));
        if (pos == std::string::npos) {
            break;
        }
        start = pos + 1;
    }

    if (parts.size() != 7) {
        return false;
    }

    try {
        entry->state = parts[0];
        entry->bevy_id = static_cast<uint32_t>(std::stoul(parts[1]));
        entry->offset = static_cast<uint64_t>(std::stoull(parts[2]));
        entry->compressed_size = static_cast<uint32_t>(std::stoul(parts[3]));
        entry->uncompressed_size = static_cast<uint32_t>(std::stoul(parts[4]));
        entry->flags = static_cast<uint32_t>(std::stoul(parts[5]));
    } catch (...) {
        return false;
    }

    return HexToBytes(parts[6], entry->hash, sizeof(entry->hash));
}

bool IsCdcBoundary(uint64_t hash, uint32_t chunk_size) {
    if (chunk_size < kCdcTargetSize) {
        return (hash & kCdcEarlyMask) == 0;
    }
    return (hash & kCdcLateMask) == 0;
}

std::string JournalStateFromBool(bool committed) {
    return committed ? "COMMITTED" : "PREPARED";
}

bool ComputeSha256ForFilePath(const std::string& path, uint8_t out[32]) {
    FILE* file = fopen(path.c_str(), "rb");
    if (!file) {
        return false;
    }

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr);

    std::vector<uint8_t> buffer(1024 * 1024);
    while (true) {
        size_t read_bytes = fread(buffer.data(), 1, buffer.size(), file);
        if (read_bytes > 0) {
            EVP_DigestUpdate(ctx, buffer.data(), read_bytes);
        }

        if (read_bytes < buffer.size()) {
            if (ferror(file) != 0) {
                fclose(file);
                EVP_MD_CTX_free(ctx);
                return false;
            }
            break;
        }
    }

    unsigned int digest_length = 0;
    EVP_DigestFinal_ex(ctx, out, &digest_length);

    fclose(file);
    EVP_MD_CTX_free(ctx);
    return digest_length == 32;
}

std::string CleanUrnComponent(const URN& image_urn) {
    std::string clean_urn = image_urn.SerializeToString();
    std::replace(clean_urn.begin(), clean_urn.end(), '/', '_');
    std::replace(clean_urn.begin(), clean_urn.end(), ':', '_');
    return clean_urn;
}

std::string CleanUrnComponentFromMapName(const std::string& map_name) {
    std::string clean = map_name;
    if (clean.rfind("map_", 0) == 0) {
        clean = clean.substr(4);
    }

    const std::string suffix = ".map.lz4";
    if (clean.size() > suffix.size() &&
        clean.compare(clean.size() - suffix.size(), suffix.size(), suffix) == 0) {
        clean.resize(clean.size() - suffix.size());
    }

    return clean;
}

void ComputeSha256(const uint8_t* data, size_t length, uint8_t out[32]) {
    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr);
    EVP_DigestUpdate(ctx, data, length);
    unsigned int digest_length = 0;
    EVP_DigestFinal_ex(ctx, out, &digest_length);
    EVP_MD_CTX_free(ctx);
}

bool ComputeSha256ForFilePrefix(FILE* file, uint64_t length, uint8_t out[32]) {
    if (fflush(file) != 0) {
        return false;
    }
    if (fseek(file, 0, SEEK_SET) != 0) {
        return false;
    }

    EVP_MD_CTX* ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr);

    std::vector<uint8_t> buffer(1024 * 1024);
    uint64_t remaining = length;
    while (remaining > 0) {
        size_t to_read = static_cast<size_t>(std::min<uint64_t>(buffer.size(), remaining));
        size_t read_bytes = fread(buffer.data(), 1, to_read, file);
        if (read_bytes != to_read) {
            EVP_MD_CTX_free(ctx);
            return false;
        }
        EVP_DigestUpdate(ctx, buffer.data(), read_bytes);
        remaining -= read_bytes;
    }

    unsigned int digest_length = 0;
    EVP_DigestFinal_ex(ctx, out, &digest_length);
    EVP_MD_CTX_free(ctx);
    return true;
}

void AppendBytes(std::vector<uint8_t>& out, const void* data, size_t length) {
    const auto* bytes = static_cast<const uint8_t*>(data);
    out.insert(out.end(), bytes, bytes + length);
}

}  // namespace

// -------------------------------------------------------------
// ChunkCorpus (RocksDB)
// -------------------------------------------------------------
ChunkCorpus::ChunkCorpus(const std::string& db_path) : db_path_(db_path) {}
ChunkCorpus::~ChunkCorpus() { Close(); }

AFF4Status ChunkCorpus::Initialize() {
    rocksdb::Options options;
    options.create_if_missing = true;
    options.create_missing_column_families = true;
    
    std::vector<rocksdb::ColumnFamilyDescriptor> column_families;
    column_families.push_back(rocksdb::ColumnFamilyDescriptor(std::string(rocksdb::kDefaultColumnFamilyName), rocksdb::ColumnFamilyOptions()));
    column_families.push_back(rocksdb::ColumnFamilyDescriptor(std::string("chunk_index_cf"), rocksdb::ColumnFamilyOptions()));
    column_families.push_back(rocksdb::ColumnFamilyDescriptor(std::string("metadata_cf"), rocksdb::ColumnFamilyOptions()));

    rocksdb::Status status = rocksdb::DB::Open(options, db_path_, column_families, &handles_, &db_);
    if (!status.ok()) return GENERIC_ERROR;

    if (handles_.size() >= 3) {
        chunk_index_cf_ = handles_[1];
        metadata_cf_ = handles_[2];
    }
    return STATUS_OK;
}

void ChunkCorpus::Close() {
    if (db_) {
        for (auto handle : handles_) {
            db_->DestroyColumnFamilyHandle(handle);
        }
        handles_.clear();
        delete db_;
        db_ = nullptr;
    }
}

AFF4Status ChunkCorpus::Put(const uint8_t hash[32], const ChunkIndexValue& value) {
    if (!db_ || !chunk_index_cf_) return GENERIC_ERROR;
    rocksdb::Slice key((const char*)hash, 32);
    rocksdb::Slice val((const char*)&value, sizeof(value));
    rocksdb::Status s = db_->Put(rocksdb::WriteOptions(), chunk_index_cf_, key, val);
    return s.ok() ? STATUS_OK : GENERIC_ERROR;
}

AFF4Status ChunkCorpus::Get(const uint8_t hash[32], ChunkIndexValue* value) {
    if (!db_ || !chunk_index_cf_) return GENERIC_ERROR;
    rocksdb::Slice key((const char*)hash, 32);
    std::string val;
    rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), chunk_index_cf_, key, &val);
    if (s.ok() && val.size() == sizeof(ChunkIndexValue)) {
        memcpy(value, val.data(), sizeof(ChunkIndexValue));
        return STATUS_OK;
    }
    return GENERIC_ERROR; // NOT_FOUND or sized mismatched
}

AFF4Status ChunkCorpus::PutMetadata(const std::string& key, const std::string& value) {
    if (!db_ || !metadata_cf_) return GENERIC_ERROR;

    rocksdb::Slice metadata_key(key);
    rocksdb::Slice metadata_value(value);
    rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), metadata_cf_, metadata_key, metadata_value);
    return status.ok() ? STATUS_OK : GENERIC_ERROR;
}

AFF4Status ChunkCorpus::GetMetadata(const std::string& key, std::string* value) {
    if (!db_ || !metadata_cf_ || !value) return GENERIC_ERROR;

    rocksdb::Slice metadata_key(key);
    rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), metadata_cf_, metadata_key, value);
    return status.ok() ? STATUS_OK : GENERIC_ERROR;
}

AFF4Status ChunkCorpus::ListMetadataPrefix(
    const std::string& prefix,
    std::vector<std::pair<std::string, std::string>>* entries) {
    if (!db_ || !metadata_cf_ || !entries) {
        return GENERIC_ERROR;
    }

    entries->clear();
    std::unique_ptr<rocksdb::Iterator> it(db_->NewIterator(rocksdb::ReadOptions(), metadata_cf_));
    for (it->Seek(prefix); it->Valid(); it->Next()) {
        std::string key = it->key().ToString();
        if (key.rfind(prefix, 0) != 0) {
            break;
        }
        entries->emplace_back(key, it->value().ToString());
    }

    if (!it->status().ok()) {
        return GENERIC_ERROR;
    }

    return STATUS_OK;
}

// -------------------------------------------------------------
// BevyWriter
// -------------------------------------------------------------
BevyWriter::BevyWriter(const std::string& dir_path, uint32_t initial_id) : dir_path_(dir_path), bevy_id_(initial_id) {}
BevyWriter::~BevyWriter() {
    if (file_) {
        FinalizeBevy();
    }
}

AFF4Status BevyWriter::Initialize() {
    std::string filepath = dir_path_ + "/bevy_" + std::to_string(bevy_id_) + ".bev";
    file_ = fopen(filepath.c_str(), "w+b");
    if (!file_) return GENERIC_ERROR;
    char magic[16] = {};
    memcpy(magic, "AFF4BEVY", 8);
    if (fwrite(magic, 1, 16, file_) != 16) {
        fclose(file_);
        file_ = nullptr;
        return GENERIC_ERROR;
    }
    chunk_offsets_.clear();
    current_size_ = 16;
    return STATUS_OK;
}

AFF4Status BevyWriter::AppendChunk(const ChunkRecordHeader& header, const uint8_t* data, uint32_t* out_bevy_id, uint64_t* out_offset) {
    if (!file_ || !out_bevy_id || !out_offset || !data) return GENERIC_ERROR;
    *out_bevy_id = bevy_id_;
    *out_offset = current_size_;
    
    chunk_offsets_.push_back(current_size_);

    if (fwrite(&header, 1, sizeof(header), file_) != sizeof(header)) {
        return GENERIC_ERROR;
    }
    current_size_ += sizeof(header);
    
    uint32_t payload_len = (header.compression_type == 0) ? header.uncompressed_size : header.compressed_size;
    if (fwrite(data, 1, payload_len, file_) != payload_len) {
        return GENERIC_ERROR;
    }
    current_size_ += payload_len;
    
    return STATUS_OK;
}

AFF4Status BevyWriter::FinalizeBevy() {
    if (!file_) return STATUS_OK;

    uint64_t index_offset = current_size_;

    for (uint64_t offset : chunk_offsets_) {
        if (fwrite(&offset, 1, sizeof(offset), file_) != sizeof(offset)) {
            return GENERIC_ERROR;
        }
        current_size_ += sizeof(offset);
    }

    BevyFooter footer{};
    footer.chunk_count = chunk_offsets_.size();
    footer.bevy_size = current_size_ + sizeof(footer);
    footer.index_offset = index_offset;

    if (!ComputeSha256ForFilePrefix(file_, current_size_, footer.sha256)) {
        return GENERIC_ERROR;
    }

    if (fseek(file_, current_size_, SEEK_SET) != 0) {
        return GENERIC_ERROR;
    }
    if (fwrite(&footer, 1, sizeof(footer), file_) != sizeof(footer)) {
        return GENERIC_ERROR;
    }
    current_size_ += sizeof(footer);

    fclose(file_);
    file_ = nullptr;
    chunk_offsets_.clear();
    return STATUS_OK;
}

// -------------------------------------------------------------
// ArchiveMapStream
// -------------------------------------------------------------
ArchiveMapStream::ArchiveMapStream(DataStore* resolver, URN urn) : AFF4Stream(resolver, urn) {}
ArchiveMapStream::~ArchiveMapStream() {}

AFF4Status ArchiveMapStream::AppendRef(const ChunkRef& ref) {
    current_segment_refs_.push_back(ref);
    logical_offset_ += ref.uncompressed_size;
    return STATUS_OK;
}

AFF4Status ArchiveMapStream::FinalizeMap(const std::string& path) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) return GENERIC_ERROR;

    std::vector<uint8_t> map_data;

    MapStreamHeader header{};
    memcpy(header.magic, "AFF4MAP1", sizeof(header.magic));
    header.logical_size = logical_offset_;
    header.segment_count = 1;
    header.reserved = 0;
    AppendBytes(map_data, &header, sizeof(header));

    SegmentHeader segment{};
    segment.logical_offset = 0;
    segment.chunk_count = current_segment_refs_.size();
    AppendBytes(map_data, &segment, sizeof(segment));

    for (const auto& ref : current_segment_refs_) {
        AppendBytes(map_data, &ref, sizeof(ref));
    }

    MapStreamFooter footer{};
    footer.chunk_count = current_segment_refs_.size();
    footer.logical_size = logical_offset_;
    footer.data_size = map_data.size() + sizeof(footer);
    ComputeSha256(map_data.data(), map_data.size(), footer.sha256);
    AppendBytes(map_data, &footer, sizeof(footer));

    int max_lz4_size = LZ4_compressBound(static_cast<int>(map_data.size()));
    if (max_lz4_size <= 0) {
        fclose(f);
        return GENERIC_ERROR;
    }
    std::vector<uint8_t> lz4_buf(max_lz4_size);
    int compressed_size = LZ4_compress_default(
        reinterpret_cast<const char*>(map_data.data()),
        reinterpret_cast<char*>(lz4_buf.data()),
        static_cast<int>(map_data.size()),
        max_lz4_size);
    if (compressed_size <= 0) {
        fclose(f);
        return GENERIC_ERROR;
    }

    uint64_t uncompressed_size = map_data.size();
    if (fwrite(&uncompressed_size, 1, sizeof(uint64_t), f) != sizeof(uint64_t) ||
        fwrite(lz4_buf.data(), 1, compressed_size, f) != static_cast<size_t>(compressed_size)) {
        fclose(f);
        return GENERIC_ERROR;
    }

    fclose(f);
    return STATUS_OK;
}

AFF4Status ArchiveMapStream::ReadBuffer(char* data, size_t* length) { *length = 0; return STATUS_OK; }
AFF4Status ArchiveMapStream::Write(const char* data, size_t length) { return STATUS_OK; }
AFF4Status ArchiveMapStream::Truncate() { return NOT_IMPLEMENTED; }
aff4_off_t ArchiveMapStream::Size() const { return logical_offset_; }


// -------------------------------------------------------------
// ArchiveChunkStore
// -------------------------------------------------------------
ArchiveChunkStore::ArchiveChunkStore(DataStore* resolver, const std::string& archive_dir) 
    : resolver_(resolver), archive_dir_(archive_dir) {}
    
ArchiveChunkStore::~ArchiveChunkStore() {}

AFF4Status ArchiveChunkStore::Initialize() {
    mkdir(archive_dir_.c_str(), 0777);
    
    std::string db_dir = archive_dir_ + "/chunk_corpus";
    corpus_ = std::make_unique<ChunkCorpus>(db_dir);
    if (corpus_->Initialize() != STATUS_OK) {
        std::cerr << "Failed to init RocksDB\n";
        return GENERIC_ERROR;
    }
    
    std::string bevies_dir = archive_dir_ + "/bevies";
    mkdir(bevies_dir.c_str(), 0777);
    
    std::string maps_dir = archive_dir_ + "/maps";
    mkdir(maps_dir.c_str(), 0777);

    if (RecoverChunkJournal() != STATUS_OK) {
        std::cerr << "Failed to recover archive journal state\n";
        return GENERIC_ERROR;
    }
    
    uint32_t next_bevy_id = 1;
    try {
        for (const auto& entry : std::filesystem::directory_iterator(bevies_dir)) {
            std::string filename = entry.path().filename().string();
            if (filename.find("bevy_") == 0 && filename.find(".bev") != std::string::npos) {
                uint32_t id = std::stoi(filename.substr(5));
                if (id >= next_bevy_id) {
                    next_bevy_id = id + 1;
                }
            }
        }
    } catch (...) {}
    
    bevy_writer_ = std::make_unique<BevyWriter>(bevies_dir, next_bevy_id);
    if (bevy_writer_->Initialize() != STATUS_OK) return GENERIC_ERROR;
    
    return STATUS_OK;
}

AFF4Status ArchiveChunkStore::RecoverChunkJournal() {
    if (!corpus_) {
        return GENERIC_ERROR;
    }

    std::vector<std::pair<std::string, std::string>> entries;
    if (corpus_->ListMetadataPrefix("journal/", &entries) != STATUS_OK) {
        return GENERIC_ERROR;
    }

    uint32_t recovered_entries = 0;
    for (const auto& [key, value] : entries) {
        JournalEntry journal_entry;
        if (!ParseJournalValue(value, &journal_entry)) {
            continue;
        }

        if (journal_entry.state == "ABORTED") {
            continue;
        }

        std::string bevy_path = archive_dir_ + "/bevies/bevy_" + std::to_string(journal_entry.bevy_id) + ".bev";
        FILE* bevy_file = fopen(bevy_path.c_str(), "rb");
        if (!bevy_file) {
            journal_entry.state = "ABORTED";
            corpus_->PutMetadata(key, BuildJournalValue(journal_entry));
            continue;
        }

        if (fseek(bevy_file, static_cast<long>(journal_entry.offset), SEEK_SET) != 0) {
            fclose(bevy_file);
            journal_entry.state = "ABORTED";
            corpus_->PutMetadata(key, BuildJournalValue(journal_entry));
            continue;
        }

        ChunkRecordHeader header{};
        if (fread(&header, 1, sizeof(header), bevy_file) != sizeof(header)) {
            fclose(bevy_file);
            journal_entry.state = "ABORTED";
            corpus_->PutMetadata(key, BuildJournalValue(journal_entry));
            continue;
        }

        if (header.compressed_size != journal_entry.compressed_size ||
            header.uncompressed_size != journal_entry.uncompressed_size ||
            header.compression_type != static_cast<uint8_t>(journal_entry.flags) ||
            memcmp(header.blake3_hash, journal_entry.hash, sizeof(journal_entry.hash)) != 0) {
            fclose(bevy_file);
            journal_entry.state = "ABORTED";
            corpus_->PutMetadata(key, BuildJournalValue(journal_entry));
            continue;
        }

        fclose(bevy_file);

        ChunkIndexValue index_val{};
        index_val.bevy_id = journal_entry.bevy_id;
        index_val.offset = journal_entry.offset;
        index_val.compressed_size = journal_entry.compressed_size;
        index_val.uncompressed_size = journal_entry.uncompressed_size;
        index_val.flags = journal_entry.flags;

        if (corpus_->Put(journal_entry.hash, index_val) != STATUS_OK) {
            return GENERIC_ERROR;
        }

        journal_entry.state = JournalStateFromBool(true);
        if (corpus_->PutMetadata(key, BuildJournalValue(journal_entry)) != STATUS_OK) {
            return GENERIC_ERROR;
        }

        ++recovered_entries;
    }

    if (recovered_entries > 0) {
        std::cout << "Recovered " << recovered_entries << " pending chunk journal entries.\n";
    }

    return STATUS_OK;
}

AFF4Status ArchiveChunkStore::IngestStream(
    AFF4Stream* input_stream, URN image_urn, const std::string& expected_input_sha256_hex) {
    if (!input_stream || !corpus_ || !bevy_writer_) return GENERIC_ERROR;
    
    ArchiveMapStream map_stream(resolver_, image_urn);

    std::vector<uint8_t> buffer(kCdcMaxSize);
    uint32_t current_chunk_size = 0;
    uint64_t hash = 0;

    auto process_chunk = [&](const uint8_t* chunk_data, uint32_t chunk_size) -> AFF4Status {
        if (chunk_size == 0) {
            return STATUS_OK;
        }

        blake3_hasher hasher;
        blake3_hasher_init(&hasher);
        blake3_hasher_update(&hasher, chunk_data, chunk_size);
        uint8_t raw_hash[BLAKE3_OUT_LEN];
        blake3_hasher_finalize(&hasher, raw_hash, BLAKE3_OUT_LEN);

        ChunkIndexValue index_val;
        if (corpus_->Get(raw_hash, &index_val) == STATUS_OK) {
            ChunkRef ref{index_val.bevy_id, index_val.offset, index_val.uncompressed_size};
            if (map_stream.AppendRef(ref) != STATUS_OK) {
                return GENERIC_ERROR;
            }
            return STATUS_OK;
        }

        int max_lz4_size = LZ4_compressBound(chunk_size);
        std::vector<uint8_t> compressed(max_lz4_size);
        int comp_size = LZ4_compress_default(
            reinterpret_cast<const char*>(chunk_data),
            reinterpret_cast<char*>(compressed.data()),
            chunk_size,
            max_lz4_size);

        ChunkRecordHeader record_header{};
        record_header.uncompressed_size = chunk_size;
        memcpy(record_header.blake3_hash, raw_hash, 32);
        memset(record_header.reserved, 0, 3);

        const uint8_t* payload_ptr = chunk_data;
        if (comp_size > 0 && comp_size < static_cast<int>(chunk_size)) {
            record_header.compressed_size = static_cast<uint32_t>(comp_size);
            record_header.compression_type = 1;
            payload_ptr = compressed.data();
        } else {
            record_header.compressed_size = chunk_size;
            record_header.compression_type = 0;
        }

        JournalEntry journal_entry{};
        journal_entry.state = "PREPARED";
        journal_entry.compressed_size = record_header.compressed_size;
        journal_entry.uncompressed_size = record_header.uncompressed_size;
        journal_entry.flags = record_header.compression_type;
        memcpy(journal_entry.hash, raw_hash, sizeof(journal_entry.hash));

        std::string journal_key;
        uint32_t stored_bevy_id = 0;
        uint64_t stored_offset = 0;

        {
            std::lock_guard<std::mutex> lock(write_mutex_);
            if (bevy_writer_->current_size() > 2ULL * 1024 * 1024 * 1024) {
                const uint32_t next_bevy_id = bevy_writer_->current_bevy_id() + 1;
                if (bevy_writer_->FinalizeBevy() != STATUS_OK) {
                    return GENERIC_ERROR;
                }
                bevy_writer_ = std::make_unique<BevyWriter>(archive_dir_ + "/bevies", next_bevy_id);
                if (bevy_writer_->Initialize() != STATUS_OK) {
                    return GENERIC_ERROR;
                }
            }

            journal_entry.bevy_id = bevy_writer_->current_bevy_id();
            journal_entry.offset = bevy_writer_->current_size();
            journal_key = BuildJournalKey(journal_entry.bevy_id, journal_entry.offset);

            if (corpus_->PutMetadata(journal_key, BuildJournalValue(journal_entry)) != STATUS_OK) {
                return GENERIC_ERROR;
            }

            if (bevy_writer_->AppendChunk(record_header, payload_ptr, &stored_bevy_id, &stored_offset) != STATUS_OK) {
                return GENERIC_ERROR;
            }
        }

        if (stored_bevy_id == 0 || stored_offset != journal_entry.offset) {
            return GENERIC_ERROR;
        }

        index_val.bevy_id = stored_bevy_id;
        index_val.offset = stored_offset;
        index_val.compressed_size = record_header.compressed_size;
        index_val.uncompressed_size = record_header.uncompressed_size;
        index_val.flags = record_header.compression_type;
        if (corpus_->Put(raw_hash, index_val) != STATUS_OK) {
            return GENERIC_ERROR;
        }

        journal_entry.state = JournalStateFromBool(true);
        if (corpus_->PutMetadata(journal_key, BuildJournalValue(journal_entry)) != STATUS_OK) {
            return GENERIC_ERROR;
        }

        ChunkRef ref{stored_bevy_id, stored_offset, chunk_size};
        if (map_stream.AppendRef(ref) != STATUS_OK) {
            return GENERIC_ERROR;
        }

        return STATUS_OK;
    };
    
    while (true) {
        char next_byte[1];
        size_t read_bytes = 1;
        AFF4Status r_status = input_stream->ReadBuffer(next_byte, &read_bytes);
        if (r_status != STATUS_OK || read_bytes == 0) {
            if (current_chunk_size > 0) {
                if (process_chunk(buffer.data(), current_chunk_size) != STATUS_OK) {
                    return GENERIC_ERROR;
                }
            }
            break;
        }
        
        buffer[current_chunk_size++] = next_byte[0];
        
        // CDC rolling hash
        hash = (hash << 1) + GEAR_MATRIX[static_cast<uint8_t>(next_byte[0])];
        bool is_cdc_boundary = (current_chunk_size >= kCdcMinSize) && IsCdcBoundary(hash, current_chunk_size);
        bool is_max_boundary = (current_chunk_size >= kCdcMaxSize);
        
        if (is_cdc_boundary || is_max_boundary) {
            if (process_chunk(buffer.data(), current_chunk_size) != STATUS_OK) {
                return GENERIC_ERROR;
            }
            
            // Reset for next chunk
            current_chunk_size = 0;
            hash = 0;
        }
    }

    // Save map
    std::string clean_urn = CleanUrnComponent(image_urn);
    std::string map_file = archive_dir_ + "/maps/map_" + clean_urn + ".map.lz4";
    if (map_stream.FinalizeMap(map_file) != STATUS_OK) {
        return GENERIC_ERROR;
    }

    if (!expected_input_sha256_hex.empty()) {
        const std::string metadata_key = "file_sha256:" + clean_urn;
        if (corpus_->PutMetadata(metadata_key, expected_input_sha256_hex) != STATUS_OK) {
            std::cerr << "Failed to persist SHA-256 metadata for " << clean_urn << "\n";
            return GENERIC_ERROR;
        }
    }

    return STATUS_OK;
}

// -------------------------------------------------------------
// ArchiveExtractor
// -------------------------------------------------------------
ArchiveExtractor::ArchiveExtractor(const std::string& archive_dir) : archive_dir_(archive_dir) {}

ArchiveExtractor::~ArchiveExtractor() {
    for (auto& pair : open_bevies_) {
        if (pair.second) fclose(pair.second);
    }
}

AFF4Status ArchiveExtractor::ValidateBevy(FILE* bevy_file, uint32_t bevy_id) {
    if (!bevy_file) {
        return GENERIC_ERROR;
    }

    if (fseek(bevy_file, 0, SEEK_END) != 0) {
        return GENERIC_ERROR;
    }
    long file_size_long = ftell(bevy_file);
    if (file_size_long < 0) {
        return GENERIC_ERROR;
    }

    uint64_t file_size = static_cast<uint64_t>(file_size_long);
    if (file_size < 16 + sizeof(BevyFooter)) {
        return GENERIC_ERROR;
    }

    if (fseek(bevy_file, 0, SEEK_SET) != 0) {
        return GENERIC_ERROR;
    }

    char magic[16] = {0};
    if (fread(magic, 1, sizeof(magic), bevy_file) != sizeof(magic)) {
        return GENERIC_ERROR;
    }
    if (memcmp(magic, "AFF4BEVY", 8) != 0) {
        return GENERIC_ERROR;
    }

    if (fseek(bevy_file, static_cast<long>(file_size - sizeof(BevyFooter)), SEEK_SET) != 0) {
        return GENERIC_ERROR;
    }

    BevyFooter footer{};
    if (fread(&footer, 1, sizeof(footer), bevy_file) != sizeof(footer)) {
        return GENERIC_ERROR;
    }

    if (footer.bevy_size != file_size || footer.index_offset < 16 || footer.index_offset >= file_size - sizeof(BevyFooter)) {
        return GENERIC_ERROR;
    }

    std::vector<uint8_t> computed_sha(32);
    if (!ComputeSha256ForFilePrefix(bevy_file, file_size - sizeof(BevyFooter), computed_sha.data())) {
        return GENERIC_ERROR;
    }

    if (memcmp(computed_sha.data(), footer.sha256, 32) != 0) {
        return GENERIC_ERROR;
    }

    if (fseek(bevy_file, 0, SEEK_SET) != 0) {
        return GENERIC_ERROR;
    }

    validated_bevies_.insert(bevy_id);
    return STATUS_OK;
}

FILE* ArchiveExtractor::GetBevyFile(uint32_t bevy_id) {
    auto cached = open_bevies_.find(bevy_id);
    if (cached != open_bevies_.end()) {
        return cached->second;
    }

    std::string path = archive_dir_ + "/bevies/bevy_" + std::to_string(bevy_id) + ".bev";
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) {
        return nullptr;
    }

    if (ValidateBevy(f, bevy_id) != STATUS_OK) {
        fclose(f);
        return nullptr;
    }

    open_bevies_[bevy_id] = f;
    return f;
}

AFF4Status ArchiveExtractor::ExtractMap(const std::string& map_name, const std::string& output_path) {
    std::string map_path = archive_dir_ + "/maps/" + map_name;
    FILE* f = fopen(map_path.c_str(), "rb");
    if (!f) { std::cerr<<"Fail 1: Cannot open " << map_path << "\n"; return GENERIC_ERROR; }

    uint64_t uncompressed_size = 0;
    if (fread(&uncompressed_size, 1, sizeof(uint64_t), f) != sizeof(uint64_t)) {
        fclose(f); std::cerr<<"Fail 2: Cannot read uncompressed size\n"; return GENERIC_ERROR;
    }

    fseek(f, 0, SEEK_END);
    size_t compressed_size = ftell(f) - sizeof(uint64_t);
    fseek(f, sizeof(uint64_t), SEEK_SET);

    std::vector<uint8_t> compressed(compressed_size);
    if (fread(compressed.data(), 1, compressed_size, f) != compressed_size) {
        fclose(f); std::cerr<<"Fail 3: Cannot read compressed data\n"; return GENERIC_ERROR;
    }
    fclose(f);

    std::vector<uint8_t> uncompressed(uncompressed_size);
    int res = LZ4_decompress_safe((const char*)compressed.data(), (char*)uncompressed.data(), compressed_size, uncompressed_size);
    if (res < 0 || uncompressed_size < sizeof(MapStreamHeader) + sizeof(SegmentHeader) + sizeof(MapStreamFooter)) { std::cerr<<"Fail 4: res="<<res<<" uncomp="<<uncompressed_size<<"\n"; return GENERIC_ERROR; }

    MapStreamHeader* map_header = reinterpret_cast<MapStreamHeader*>(uncompressed.data());
    if (memcmp(map_header->magic, "AFF4MAP1", sizeof(map_header->magic)) != 0) { std::cerr<<"Fail 5: Magic mismatch\n"; return GENERIC_ERROR; }

    MapStreamFooter* footer = reinterpret_cast<MapStreamFooter*>(uncompressed.data() + uncompressed_size - sizeof(MapStreamFooter));
    if (footer->data_size != uncompressed_size || footer->logical_size != map_header->logical_size) { std::cerr<<"Fail 5b: Map footer mismatch\n"; return GENERIC_ERROR; }

    std::vector<uint8_t> computed_sha(32);
    ComputeSha256(uncompressed.data(), uncompressed_size - sizeof(MapStreamFooter), computed_sha.data());
    if (memcmp(computed_sha.data(), footer->sha256, 32) != 0) { std::cerr<<"Fail 5c: Map checksum mismatch\n"; return GENERIC_ERROR; }

    SegmentHeader* seg = reinterpret_cast<SegmentHeader*>(uncompressed.data() + sizeof(MapStreamHeader));
    ChunkRef* refs = reinterpret_cast<ChunkRef*>(uncompressed.data() + sizeof(MapStreamHeader) + sizeof(SegmentHeader));

    if (seg->chunk_count != footer->chunk_count || map_header->segment_count != 1 || seg->logical_offset != 0 || map_header->logical_size != footer->logical_size) {
        std::cerr << "Fail 5c: Map segment metadata mismatch\n";
        return GENERIC_ERROR;
    }

    FILE* out_f = fopen(output_path.c_str(), "wb");
    if (!out_f) { std::cerr<<"Fail 6: Cannot open output\n"; return GENERIC_ERROR; }

    for (uint32_t i = 0; i < seg->chunk_count; i++) {
        const ChunkRef& ref = refs[i];
        FILE* bevy_f = GetBevyFile(ref.bevy_id);
        if (!bevy_f) {
            fclose(out_f); std::cerr<<"Fail 7: Cannot open bevy_id="<<ref.bevy_id<<"\n"; return GENERIC_ERROR;
        }

        fseek(bevy_f, ref.offset, SEEK_SET);
        ChunkRecordHeader header;
        if (fread(&header, 1, sizeof(header), bevy_f) != sizeof(header)) {
            fclose(out_f); std::cerr<<"Fail 8 at offset "<<ref.offset<<"\n"; return GENERIC_ERROR;
        }

        std::vector<uint8_t> chunk_data(header.compressed_size);
        if (fread(chunk_data.data(), 1, header.compressed_size, bevy_f) != header.compressed_size) {
            fclose(out_f); std::cerr<<"Fail 9: Cannot read chunk data\n"; return GENERIC_ERROR;
        }

        std::vector<uint8_t> raw_data;
        if (header.compression_type == 1) { // LZ4
            raw_data.resize(header.uncompressed_size);
            int dec_res = LZ4_decompress_safe((const char*)chunk_data.data(), (char*)raw_data.data(), header.compressed_size, header.uncompressed_size);
            if (dec_res < 0) {
                fclose(out_f); std::cerr<<"Fail 10: LZ4 payload format broken\n"; return GENERIC_ERROR;
            }
        } else { // Raw
            raw_data = chunk_data;
        }
        
        // --- BLAKE3 INTEGRITY VERIFICATION ---
        blake3_hasher verifier;
        blake3_hasher_init(&verifier);
        blake3_hasher_update(&verifier, raw_data.data(), raw_data.size());
        uint8_t test_hash[BLAKE3_OUT_LEN];
        blake3_hasher_finalize(&verifier, test_hash, BLAKE3_OUT_LEN);
        
        if (memcmp(test_hash, header.blake3_hash, BLAKE3_OUT_LEN) != 0) {
            std::cerr << "Fail 11: CRITICAL DATA CORRUPTION. Extracted chunk BLAKE3 hash does not match physical Bevy header!\n";
            fclose(out_f); return GENERIC_ERROR;
        }
        
        fwrite(raw_data.data(), 1, raw_data.size(), out_f);
    }

    fclose(out_f);

    ChunkCorpus corpus(archive_dir_ + "/chunk_corpus");
    if (corpus.Initialize() != STATUS_OK) {
        std::cerr << "Fail 12: Cannot initialize metadata store for integrity verification\n";
        return GENERIC_ERROR;
    }

    const std::string clean_urn = CleanUrnComponentFromMapName(map_name);
    const std::string metadata_key = "file_sha256:" + clean_urn;
    std::string expected_hash_hex;
    if (corpus.GetMetadata(metadata_key, &expected_hash_hex) != STATUS_OK) {
        std::cerr << "Fail 13: Missing stored SHA-256 metadata for map " << map_name << "\n";
        return GENERIC_ERROR;
    }

    uint8_t extracted_hash[32] = {};
    if (!ComputeSha256ForFilePath(output_path, extracted_hash)) {
        std::cerr << "Fail 14: Cannot compute SHA-256 for extracted output\n";
        return GENERIC_ERROR;
    }

    const std::string extracted_hash_hex = BytesToHex(extracted_hash, sizeof(extracted_hash));
    if (expected_hash_hex != extracted_hash_hex) {
        std::cerr << "Fail 15: SHA-256 integrity verification failed\n"
                  << "  Stored   : " << expected_hash_hex << "\n"
                  << "  Extracted: " << extracted_hash_hex << "\n";
        return GENERIC_ERROR;
    }

    std::cout << "SHA-256 integrity verification passed for extracted file.\n"
              << "  Stored   : " << expected_hash_hex << "\n"
              << "  Extracted: " << extracted_hash_hex << "\n";

    return STATUS_OK;
}

} // namespace aff4
