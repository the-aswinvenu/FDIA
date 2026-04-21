/*
Copyright 2015-2017 Google Inc. All rights reserved.
Copyright 2018-present Velocidex Innovations.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use
this file except in compliance with the License.  You may obtain a copy of the
License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed
under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
CONDITIONS OF ANY KIND, either express or implied.  See the License for the
specific language governing permissions and limitations under the License.
*/

#include "imgformatlib/aff4/aff4_hash.h"
#include "imgformatlib/aff4/lexicon.h"
#include "imgformatlib/aff4/data_store.h"

#include <algorithm>
#include <cctype>
#include <cstring>

namespace aff4 {

// ============================================================================
// Hash Type Utilities
// ============================================================================

std::string HashTypeToString(HashType type) {
    switch (type) {
        case HashType::HASH_MD5:    return "md5";
        case HashType::HASH_SHA1:   return "sha1";
        case HashType::HASH_SHA256: return "sha256";
        case HashType::HASH_SHA512: return "sha512";
        case HashType::HASH_BLAKE2B: return "blake2b";
        default:                    return "unknown";
    }
}

HashType HashTypeFromString(const std::string& name) {
    std::string lower = name;
    std::transform(lower.begin(), lower.end(), lower.begin(),
                   [](unsigned char c) { return std::tolower(c); });

    if (lower == "md5") return HashType::HASH_MD5;
    if (lower == "sha1") return HashType::HASH_SHA1;
    if (lower == "sha256") return HashType::HASH_SHA256;
    if (lower == "sha512") return HashType::HASH_SHA512;
    if (lower == "blake2b") return HashType::HASH_BLAKE2B;

    return HashType::HASH_NONE;
}

std::string HashTypeToURN(HashType type) {
    switch (type) {
        case HashType::HASH_MD5:    return AFF4_HASH_MD5;
        case HashType::HASH_SHA1:   return AFF4_HASH_SHA1;
        case HashType::HASH_SHA256: return AFF4_HASH_SHA256;
        case HashType::HASH_SHA512: return AFF4_HASH_SHA512;
        case HashType::HASH_BLAKE2B: return AFF4_HASH_BLAKE2B;
        default:                    return "";
    }
}

HashType HashTypeFromURN(const std::string& urn) {
    if (urn == AFF4_HASH_MD5) return HashType::HASH_MD5;
    if (urn == AFF4_HASH_SHA1) return HashType::HASH_SHA1;
    if (urn == AFF4_HASH_SHA256) return HashType::HASH_SHA256;
    if (urn == AFF4_HASH_SHA512) return HashType::HASH_SHA512;
    if (urn == AFF4_HASH_BLAKE2B) return HashType::HASH_BLAKE2B;

    return HashType::HASH_NONE;
}

size_t HashDigestLength(HashType type) {
    switch (type) {
        case HashType::HASH_MD5:    return 16;   // 128 bits
        case HashType::HASH_SHA1:   return 20;   // 160 bits
        case HashType::HASH_SHA256: return 32;   // 256 bits
        case HashType::HASH_SHA512: return 64;   // 512 bits
        case HashType::HASH_BLAKE2B: return 64;  // 512 bits (default)
        default:                    return 0;
    }
}


// ============================================================================
// AFF4Hash
// ============================================================================

static const char* const hex_lut = "0123456789abcdef";

std::string AFF4Hash::HexDigest() const {
    std::string result;
    result.reserve(digest.size() * 2);

    for (unsigned char c : digest) {
        result.push_back(hex_lut[c >> 4]);
        result.push_back(hex_lut[c & 0x0F]);
    }

    return result;
}

AFF4Status AFF4Hash::FromHexDigest(HashType type, const std::string& hex, AFF4Hash& result) {
    // Length must be even
    if (hex.length() % 2 != 0) {
        return INVALID_INPUT;
    }

    // Verify expected length
    size_t expected_len = HashDigestLength(type);
    if (hex.length() != expected_len * 2) {
        return INVALID_INPUT;
    }

    std::string binary;
    binary.reserve(hex.length() / 2);

    for (size_t i = 0; i < hex.length(); i += 2) {
        char high = hex[i];
        char low = hex[i + 1];

        int high_val, low_val;

        if (high >= '0' && high <= '9') high_val = high - '0';
        else if (high >= 'a' && high <= 'f') high_val = high - 'a' + 10;
        else if (high >= 'A' && high <= 'F') high_val = high - 'A' + 10;
        else return INVALID_INPUT;

        if (low >= '0' && low <= '9') low_val = low - '0';
        else if (low >= 'a' && low <= 'f') low_val = low - 'a' + 10;
        else if (low >= 'A' && low <= 'F') low_val = low - 'A' + 10;
        else return INVALID_INPUT;

        binary.push_back(static_cast<char>((high_val << 4) | low_val));
    }

    result.type = type;
    result.digest = std::move(binary);

    return STATUS_OK;
}


// ============================================================================
// AFF4Hasher Base
// ============================================================================

AFF4Status AFF4Hasher::HashBuffer(const char* data, size_t length, AFF4Hash& result) {
    AFF4Status status = Init();
    if (status != STATUS_OK) return status;

    status = Update(data, length);
    if (status != STATUS_OK) return status;

    return Finalize(result);
}

AFF4Status AFF4Hasher::HashString(const std::string& data, AFF4Hash& result) {
    return HashBuffer(data.data(), data.size(), result);
}


// ============================================================================
// OpenSSL Hasher Implementation
// ============================================================================

#ifdef HAVE_OPENSSL

OpenSSLHasher::OpenSSLHasher(HashType type)
    : type_(type), ctx_(nullptr), md_(nullptr), initialized_(false) {

    switch (type) {
        case HashType::HASH_MD5:
            md_ = EVP_md5();
            break;
        case HashType::HASH_SHA1:
            md_ = EVP_sha1();
            break;
        case HashType::HASH_SHA256:
            md_ = EVP_sha256();
            break;
        case HashType::HASH_SHA512:
            md_ = EVP_sha512();
            break;
        case HashType::HASH_BLAKE2B:
            md_ = EVP_blake2b512();
            break;
        default:
            md_ = nullptr;
    }
}

OpenSSLHasher::~OpenSSLHasher() {
    if (ctx_) {
        EVP_MD_CTX_free(ctx_);
        ctx_ = nullptr;
    }
}

OpenSSLHasher::OpenSSLHasher(OpenSSLHasher&& other) noexcept
    : type_(other.type_), ctx_(other.ctx_), md_(other.md_), initialized_(other.initialized_) {
    other.ctx_ = nullptr;
    other.initialized_ = false;
}

OpenSSLHasher& OpenSSLHasher::operator=(OpenSSLHasher&& other) noexcept {
    if (this != &other) {
        if (ctx_) {
            EVP_MD_CTX_free(ctx_);
        }
        type_ = other.type_;
        ctx_ = other.ctx_;
        md_ = other.md_;
        initialized_ = other.initialized_;
        other.ctx_ = nullptr;
        other.initialized_ = false;
    }
    return *this;
}

AFF4Status OpenSSLHasher::Init() {
    if (!md_) {
        return NOT_IMPLEMENTED;
    }

    if (ctx_) {
        EVP_MD_CTX_free(ctx_);
    }

    ctx_ = EVP_MD_CTX_new();
    if (!ctx_) {
        return MEMORY_ERROR;
    }

    if (EVP_DigestInit_ex(ctx_, md_, nullptr) != 1) {
        EVP_MD_CTX_free(ctx_);
        ctx_ = nullptr;
        return GENERIC_ERROR;
    }

    initialized_ = true;
    return STATUS_OK;
}

AFF4Status OpenSSLHasher::Update(const char* data, size_t length) {
    if (!initialized_ || !ctx_) {
        return GENERIC_ERROR;
    }

    if (length == 0) {
        return STATUS_OK;
    }

    if (EVP_DigestUpdate(ctx_, data, length) != 1) {
        return GENERIC_ERROR;
    }

    return STATUS_OK;
}

AFF4Status OpenSSLHasher::Finalize(AFF4Hash& result) {
    if (!initialized_ || !ctx_) {
        return GENERIC_ERROR;
    }

    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int digest_len = 0;

    if (EVP_DigestFinal_ex(ctx_, digest, &digest_len) != 1) {
        return GENERIC_ERROR;
    }

    result.type = type_;
    result.digest = std::string(reinterpret_cast<char*>(digest), digest_len);

    initialized_ = false;
    return STATUS_OK;
}

#endif  // HAVE_OPENSSL


// ============================================================================
// Hasher Factory
// ============================================================================

std::unique_ptr<AFF4Hasher> HasherFactory::Create(HashType type) {
    if (!IsSupported(type)) {
        return nullptr;
    }

#ifdef HAVE_OPENSSL
    return std::make_unique<OpenSSLHasher>(type);
#else
    return nullptr;
#endif
}

bool HasherFactory::IsSupported(HashType type) {
#ifdef HAVE_OPENSSL
    switch (type) {
        case HashType::HASH_MD5:
        case HashType::HASH_SHA1:
        case HashType::HASH_SHA256:
        case HashType::HASH_SHA512:
        case HashType::HASH_BLAKE2B:
            return true;
        default:
            return false;
    }
#else
    return false;
#endif
}

std::vector<HashType> HasherFactory::SupportedTypes() {
    std::vector<HashType> types;

#ifdef HAVE_OPENSSL
    types.push_back(HashType::HASH_MD5);
    types.push_back(HashType::HASH_SHA1);
    types.push_back(HashType::HASH_SHA256);
    types.push_back(HashType::HASH_SHA512);
    types.push_back(HashType::HASH_BLAKE2B);
#endif

    return types;
}


// ============================================================================
// MultiHasher
// ============================================================================

AFF4Status MultiHasher::AddHashType(HashType type) {
    auto hasher = HasherFactory::Create(type);
    if (!hasher) {
        return NOT_IMPLEMENTED;
    }
    hashers_.push_back(std::move(hasher));
    return STATUS_OK;
}

AFF4Status MultiHasher::AddHashTypes(const std::vector<HashType>& types) {
    for (HashType type : types) {
        AFF4Status status = AddHashType(type);
        if (status != STATUS_OK) {
            return status;
        }
    }
    return STATUS_OK;
}

AFF4Status MultiHasher::Init() {
    for (auto& hasher : hashers_) {
        AFF4Status status = hasher->Init();
        if (status != STATUS_OK) {
            return status;
        }
    }
    return STATUS_OK;
}

AFF4Status MultiHasher::Update(const char* data, size_t length) {
    for (auto& hasher : hashers_) {
        AFF4Status status = hasher->Update(data, length);
        if (status != STATUS_OK) {
            return status;
        }
    }
    return STATUS_OK;
}

AFF4Status MultiHasher::Finalize(std::vector<AFF4Hash>& results) {
    results.clear();
    results.reserve(hashers_.size());

    for (auto& hasher : hashers_) {
        AFF4Hash hash;
        AFF4Status status = hasher->Finalize(hash);
        if (status != STATUS_OK) {
            return status;
        }
        results.push_back(std::move(hash));
    }
    return STATUS_OK;
}

AFF4Status MultiHasher::HashBuffer(const char* data, size_t length, std::vector<AFF4Hash>& results) {
    AFF4Status status = Init();
    if (status != STATUS_OK) return status;

    status = Update(data, length);
    if (status != STATUS_OK) return status;

    return Finalize(results);
}


// ============================================================================
// Hash Verification
// ============================================================================

bool VerifyHash(const AFF4Hash& expected, const AFF4Hash& computed) {
    if (expected.type != computed.type) {
        return false;
    }

    if (expected.digest.size() != computed.digest.size()) {
        return false;
    }

    // Constant-time comparison to prevent timing attacks
    const unsigned char* a = reinterpret_cast<const unsigned char*>(expected.digest.data());
    const unsigned char* b = reinterpret_cast<const unsigned char*>(computed.digest.data());
    unsigned char result = 0;

    for (size_t i = 0; i < expected.digest.size(); ++i) {
        result |= a[i] ^ b[i];
    }

    return result == 0;
}


// ============================================================================
// DataStore Integration
// ============================================================================

std::string GetHashAttributeURI(HashType type) {
    return HashTypeToURN(type);
}

std::string GetImageHashAttributeURI(HashType type) {
    switch (type) {
        case HashType::HASH_MD5:    return AFF4_IMAGE_HASH_MD5;
        case HashType::HASH_SHA1:   return AFF4_IMAGE_HASH_SHA1;
        case HashType::HASH_SHA256: return AFF4_IMAGE_HASH_SHA256;
        case HashType::HASH_SHA512: return AFF4_IMAGE_HASH_SHA512;
        case HashType::HASH_BLAKE2B: return AFF4_IMAGE_HASH_BLAKE2B;
        default:                    return "";
    }
}

std::string GetBlockHashAttributeURI(HashType type) {
    switch (type) {
        case HashType::HASH_MD5:    return AFF4_BLOCK_HASH_MD5;
        case HashType::HASH_SHA1:   return AFF4_BLOCK_HASH_SHA1;
        case HashType::HASH_SHA256: return AFF4_BLOCK_HASH_SHA256;
        case HashType::HASH_SHA512: return AFF4_BLOCK_HASH_SHA512;
        case HashType::HASH_BLAKE2B: return AFF4_BLOCK_HASH_BLAKE2B;
        default:                    return "";
    }
}

namespace {

// Helper to create the correct RDF hash type object for storing
std::shared_ptr<RDFValue> CreateRDFHash(HashType type, const std::string& hex_digest) {
    switch (type) {
        case HashType::HASH_MD5:
            return std::make_shared<MD5Hash>(hex_digest);
        case HashType::HASH_SHA1:
            return std::make_shared<SHA1Hash>(hex_digest);
        case HashType::HASH_SHA256:
            return std::make_shared<SHA256Hash>(hex_digest);
        case HashType::HASH_SHA512:
            return std::make_shared<SHA512Hash>(hex_digest);
        case HashType::HASH_BLAKE2B:
            return std::make_shared<Blake2BHash>(hex_digest);
        default:
            return nullptr;
    }
}

}  // anonymous namespace


AFF4Status StoreHash(DataStore* resolver, const URN& subject, const AFF4Hash& hash) {
    if (!resolver || !hash.IsValid()) {
        return INVALID_INPUT;
    }

    std::string attr_uri = GetHashAttributeURI(hash.type);
    if (attr_uri.empty()) {
        return NOT_IMPLEMENTED;
    }

    auto rdf_hash = CreateRDFHash(hash.type, hash.HexDigest());
    if (!rdf_hash) {
        return GENERIC_ERROR;
    }

    resolver->Set(subject, URN(attr_uri), rdf_hash, true);
    return STATUS_OK;
}

AFF4Status StoreImageHash(DataStore* resolver, const URN& subject, const AFF4Hash& hash) {
    if (!resolver || !hash.IsValid()) {
        return INVALID_INPUT;
    }

    std::string attr_uri = GetImageHashAttributeURI(hash.type);
    if (attr_uri.empty()) {
        return NOT_IMPLEMENTED;
    }

    auto rdf_hash = CreateRDFHash(hash.type, hash.HexDigest());
    if (!rdf_hash) {
        return GENERIC_ERROR;
    }

    resolver->Set(subject, URN(attr_uri), rdf_hash, true);
    return STATUS_OK;
}

AFF4Status StoreBlockHash(DataStore* resolver, const URN& subject, const AFF4Hash& hash) {
    if (!resolver || !hash.IsValid()) {
        return INVALID_INPUT;
    }

    std::string attr_uri = GetBlockHashAttributeURI(hash.type);
    if (attr_uri.empty()) {
        return NOT_IMPLEMENTED;
    }

    auto rdf_hash = CreateRDFHash(hash.type, hash.HexDigest());
    if (!rdf_hash) {
        return GENERIC_ERROR;
    }

    resolver->Set(subject, URN(attr_uri), rdf_hash, true);
    return STATUS_OK;
}

AFF4Status GetHash(DataStore* resolver, const URN& subject, HashType type, AFF4Hash& hash) {
    if (!resolver) {
        return INVALID_INPUT;
    }

    std::string attr_uri = GetHashAttributeURI(type);
    if (attr_uri.empty()) {
        return NOT_IMPLEMENTED;
    }

    // We need to use the matching RDF type for retrieval since DataStore
    // compares typeid. Create a temporary object of the correct type.
    AFF4Status status;
    std::string hex_digest;

    switch (type) {
        case HashType::HASH_MD5: {
            MD5Hash value(resolver);
            status = resolver->Get(subject, URN(attr_uri), value);
            if (status == STATUS_OK) hex_digest = value.SerializeToString();
            break;
        }
        case HashType::HASH_SHA1: {
            SHA1Hash value(resolver);
            status = resolver->Get(subject, URN(attr_uri), value);
            if (status == STATUS_OK) hex_digest = value.SerializeToString();
            break;
        }
        case HashType::HASH_SHA256: {
            SHA256Hash value(resolver);
            status = resolver->Get(subject, URN(attr_uri), value);
            if (status == STATUS_OK) hex_digest = value.SerializeToString();
            break;
        }
        case HashType::HASH_SHA512: {
            SHA512Hash value(resolver);
            status = resolver->Get(subject, URN(attr_uri), value);
            if (status == STATUS_OK) hex_digest = value.SerializeToString();
            break;
        }
        case HashType::HASH_BLAKE2B: {
            Blake2BHash value(resolver);
            status = resolver->Get(subject, URN(attr_uri), value);
            if (status == STATUS_OK) hex_digest = value.SerializeToString();
            break;
        }
        default:
            return NOT_IMPLEMENTED;
    }

    if (status != STATUS_OK) {
        return status;
    }

    return AFF4Hash::FromHexDigest(type, hex_digest, hash);
}

AFF4Status GetAllHashes(DataStore* resolver, const URN& subject, std::vector<AFF4Hash>& hashes) {
    if (!resolver) {
        return INVALID_INPUT;
    }

    hashes.clear();

    // Try to retrieve each supported hash type
    std::vector<HashType> types = HasherFactory::SupportedTypes();

    for (HashType type : types) {
        AFF4Hash hash;
        AFF4Status status = GetHash(resolver, subject, type, hash);
        if (status == STATUS_OK) {
            hashes.push_back(std::move(hash));
        }
        // Ignore NOT_FOUND errors, just skip that hash type
    }

    return STATUS_OK;
}

AFF4Status GetImageHash(DataStore* resolver, const URN& subject, HashType type, AFF4Hash& hash) {
    if (!resolver) {
        return INVALID_INPUT;
    }

    std::string attr_uri = GetImageHashAttributeURI(type);
    if (attr_uri.empty()) {
        return NOT_IMPLEMENTED;
    }

    // We need to use the matching RDF type for retrieval since DataStore
    // compares typeid. Create a temporary object of the correct type.
    AFF4Status status;
    std::string hex_digest;

    switch (type) {
        case HashType::HASH_MD5: {
            MD5Hash value(resolver);
            status = resolver->Get(subject, URN(attr_uri), value);
            if (status == STATUS_OK) hex_digest = value.SerializeToString();
            break;
        }
        case HashType::HASH_SHA1: {
            SHA1Hash value(resolver);
            status = resolver->Get(subject, URN(attr_uri), value);
            if (status == STATUS_OK) hex_digest = value.SerializeToString();
            break;
        }
        case HashType::HASH_SHA256: {
            SHA256Hash value(resolver);
            status = resolver->Get(subject, URN(attr_uri), value);
            if (status == STATUS_OK) hex_digest = value.SerializeToString();
            break;
        }
        case HashType::HASH_SHA512: {
            SHA512Hash value(resolver);
            status = resolver->Get(subject, URN(attr_uri), value);
            if (status == STATUS_OK) hex_digest = value.SerializeToString();
            break;
        }
        case HashType::HASH_BLAKE2B: {
            Blake2BHash value(resolver);
            status = resolver->Get(subject, URN(attr_uri), value);
            if (status == STATUS_OK) hex_digest = value.SerializeToString();
            break;
        }
        default:
            return NOT_IMPLEMENTED;
    }

    if (status != STATUS_OK) {
        return status;
    }

    return AFF4Hash::FromHexDigest(type, hex_digest, hash);
}

AFF4Status GetAllImageHashes(DataStore* resolver, const URN& subject, std::vector<AFF4Hash>& hashes) {
    if (!resolver) {
        return INVALID_INPUT;
    }

    hashes.clear();

    // Try to retrieve each supported hash type using image-level attributes
    std::vector<HashType> types = HasherFactory::SupportedTypes();

    for (HashType type : types) {
        AFF4Hash hash;
        AFF4Status status = GetImageHash(resolver, subject, type, hash);
        if (status == STATUS_OK) {
            hashes.push_back(std::move(hash));
        }
        // Ignore NOT_FOUND errors, just skip that hash type
    }

    return STATUS_OK;
}

std::string ImageVerifyResult::Summary() const {
    if (error_message.empty() == false) {
        return "Verification error: " + error_message;
    }

    if (hash_results.empty()) {
        return "No hashes found for verification";
    }

    std::string result;
    int passed = 0;
    int failed = 0;

    for (const auto& hr : hash_results) {
        if (hr.matches) {
            passed++;
        } else {
            failed++;
        }
    }

    if (all_passed) {
        result = "Verification PASSED: " + std::to_string(passed) + " hash(es) verified";
    } else {
        result = "Verification FAILED: " + std::to_string(failed) + " of " +
                 std::to_string(passed + failed) + " hash(es) did not match";
    }

    return result;
}


}  // namespace aff4
