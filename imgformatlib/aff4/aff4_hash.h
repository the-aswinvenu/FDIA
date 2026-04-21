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

/**
 * @file aff4_hash.h
 * @brief Cryptographic hashing support for AFF4 images.
 *
 * This module provides hash computation and verification for AFF4 images
 * as specified in Section 6 of the AFF4 standard.
 */

#ifndef SRC_AFF4_HASH_H_
#define SRC_AFF4_HASH_H_

#include "imgformatlib/aff4/config.h"
#include "aff4_errors.h"
#include "rdf.h"

#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

#ifdef HAVE_OPENSSL
#include <openssl/evp.h>
#include <openssl/md5.h>
#include <openssl/sha.h>
#endif

namespace aff4 {

/**
 * Supported hash algorithms.
 */
enum class HashType {
    HASH_NONE = 0,
    HASH_MD5,
    HASH_SHA1,
    HASH_SHA256,
    HASH_SHA512,
    HASH_BLAKE2B
};

/**
 * Get the string name for a hash type.
 */
std::string HashTypeToString(HashType type);

/**
 * Parse a hash type from string.
 */
HashType HashTypeFromString(const std::string& name);

/**
 * Get the AFF4 lexicon URI for a hash type.
 */
std::string HashTypeToURN(HashType type);

/**
 * Parse a hash type from AFF4 lexicon URI.
 */
HashType HashTypeFromURN(const std::string& urn);

/**
 * Get the digest length in bytes for a hash type.
 */
size_t HashDigestLength(HashType type);


/**
 * @brief Represents a computed or stored hash digest.
 */
class AFF4Hash {
public:
    HashType type;
    std::string digest;  // Binary digest bytes

    AFF4Hash() : type(HashType::HASH_NONE) {}
    AFF4Hash(HashType t, const std::string& d) : type(t), digest(d) {}

    /**
     * Returns the digest as a hex string.
     */
    std::string HexDigest() const;

    /**
     * Parse a hex digest string into binary.
     */
    static AFF4Status FromHexDigest(HashType type, const std::string& hex, AFF4Hash& result);

    /**
     * Compare two hashes for equality.
     */
    bool operator==(const AFF4Hash& other) const {
        return type == other.type && digest == other.digest;
    }

    bool operator!=(const AFF4Hash& other) const {
        return !(*this == other);
    }

    /**
     * Check if this hash is valid (has data).
     */
    bool IsValid() const {
        return type != HashType::HASH_NONE && !digest.empty();
    }
};


/**
 * @brief Abstract interface for hash computation.
 *
 * This class provides a streaming interface for computing cryptographic
 * hashes over data.
 */
class AFF4Hasher {
public:
    virtual ~AFF4Hasher() = default;

    /**
     * Initialize or reset the hasher.
     */
    virtual AFF4Status Init() = 0;

    /**
     * Update the hash with more data.
     */
    virtual AFF4Status Update(const char* data, size_t length) = 0;

    /**
     * Finalize and get the computed hash.
     */
    virtual AFF4Status Finalize(AFF4Hash& result) = 0;

    /**
     * Get the hash type this hasher computes.
     */
    virtual HashType GetType() const = 0;

    /**
     * Convenience: compute hash of a complete buffer.
     */
    AFF4Status HashBuffer(const char* data, size_t length, AFF4Hash& result);

    /**
     * Convenience: compute hash of a string.
     */
    AFF4Status HashString(const std::string& data, AFF4Hash& result);
};


#ifdef HAVE_OPENSSL

/**
 * @brief OpenSSL-based hasher using EVP interface.
 *
 * Supports MD5, SHA1, SHA256, SHA512.
 */
class OpenSSLHasher : public AFF4Hasher {
public:
    explicit OpenSSLHasher(HashType type);
    ~OpenSSLHasher() override;

    // Non-copyable
    OpenSSLHasher(const OpenSSLHasher&) = delete;
    OpenSSLHasher& operator=(const OpenSSLHasher&) = delete;

    // Movable
    OpenSSLHasher(OpenSSLHasher&& other) noexcept;
    OpenSSLHasher& operator=(OpenSSLHasher&& other) noexcept;

    AFF4Status Init() override;
    AFF4Status Update(const char* data, size_t length) override;
    AFF4Status Finalize(AFF4Hash& result) override;
    HashType GetType() const override { return type_; }

private:
    HashType type_;
    EVP_MD_CTX* ctx_;
    const EVP_MD* md_;
    bool initialized_;
};

#endif  // HAVE_OPENSSL


/**
 * @brief Factory for creating hashers.
 */
class HasherFactory {
public:
    /**
     * Create a hasher for the given type.
     * Returns nullptr if the type is not supported.
     */
    static std::unique_ptr<AFF4Hasher> Create(HashType type);

    /**
     * Check if a hash type is supported.
     */
    static bool IsSupported(HashType type);

    /**
     * Get list of all supported hash types.
     */
    static std::vector<HashType> SupportedTypes();
};


/**
 * @brief Multi-hash computation.
 *
 * Computes multiple hashes simultaneously over the same data stream.
 */
class MultiHasher {
public:
    MultiHasher() = default;

    /**
     * Add a hash type to compute.
     */
    AFF4Status AddHashType(HashType type);

    /**
     * Add multiple hash types.
     */
    AFF4Status AddHashTypes(const std::vector<HashType>& types);

    /**
     * Initialize all hashers.
     */
    AFF4Status Init();

    /**
     * Update all hashers with data.
     */
    AFF4Status Update(const char* data, size_t length);

    /**
     * Finalize all hashers and get results.
     */
    AFF4Status Finalize(std::vector<AFF4Hash>& results);

    /**
     * Convenience: compute all hashes of a buffer.
     */
    AFF4Status HashBuffer(const char* data, size_t length, std::vector<AFF4Hash>& results);

    /**
     * Get the number of hash types being computed.
     */
    size_t Count() const { return hashers_.size(); }

private:
    std::vector<std::unique_ptr<AFF4Hasher>> hashers_;
};


/**
 * @brief Hash verification result.
 */
struct HashVerifyResult {
    HashType type;
    AFF4Hash expected;
    AFF4Hash computed;
    bool matches;

    HashVerifyResult() : type(HashType::HASH_NONE), matches(false) {}
};


/**
 * Verify a computed hash against an expected hash.
 */
bool VerifyHash(const AFF4Hash& expected, const AFF4Hash& computed);


// Forward declarations for DataStore integration
class DataStore;

/**
 * @brief Get the appropriate RDF hash attribute URI for a given hash type.
 *
 * This maps HashType to the correct AFF4 lexicon entry for storing
 * the hash as an RDF triple (e.g., AFF4_HASH_SHA256).
 */
std::string GetHashAttributeURI(HashType type);

/**
 * @brief Get the image-level (linear) hash attribute URI for a given hash type.
 *
 * These are used to store computed hashes of complete images
 * (e.g., AFF4_IMAGE_HASH_SHA256).
 */
std::string GetImageHashAttributeURI(HashType type);

/**
 * @brief Get the block-level hash attribute URI for a given hash type.
 *
 * These are used to store hashes of individual blocks/chunks
 * (e.g., AFF4_BLOCK_HASH_SHA256).
 */
std::string GetBlockHashAttributeURI(HashType type);

/**
 * @brief Store a hash in the DataStore for a given subject URN.
 *
 * This stores the hash as an RDF triple using the appropriate
 * hash type attribute (e.g., aff4:SHA256).
 *
 * @param resolver The DataStore to store the hash in.
 * @param subject The URN of the object being hashed.
 * @param hash The computed hash to store.
 * @return STATUS_OK on success.
 */
AFF4Status StoreHash(DataStore* resolver, const URN& subject, const AFF4Hash& hash);

/**
 * @brief Store an image-level hash in the DataStore.
 *
 * Uses the linearHash* attributes for image-level hashes.
 */
AFF4Status StoreImageHash(DataStore* resolver, const URN& subject, const AFF4Hash& hash);

/**
 * @brief Store a block-level hash in the DataStore.
 *
 * Uses the blockHash* attributes for block-level hashes.
 */
AFF4Status StoreBlockHash(DataStore* resolver, const URN& subject, const AFF4Hash& hash);

/**
 * @brief Retrieve a hash of a specific type from the DataStore.
 *
 * @param resolver The DataStore to query.
 * @param subject The URN of the object.
 * @param type The hash type to retrieve.
 * @param hash Output: the retrieved hash.
 * @return STATUS_OK if found, NOT_FOUND if no hash of that type exists.
 */
AFF4Status GetHash(DataStore* resolver, const URN& subject, HashType type, AFF4Hash& hash);

/**
 * @brief Retrieve all hashes for a given subject from the DataStore.
 *
 * @param resolver The DataStore to query.
 * @param subject The URN of the object.
 * @param hashes Output: vector of all hashes found.
 * @return STATUS_OK on success.
 */
AFF4Status GetAllHashes(DataStore* resolver, const URN& subject, std::vector<AFF4Hash>& hashes);

/**
 * @brief Retrieve an image-level hash of a specific type from the DataStore.
 *
 * @param resolver The DataStore to query.
 * @param subject The URN of the image.
 * @param type The hash type to retrieve.
 * @param hash Output: the retrieved hash.
 * @return STATUS_OK if found, NOT_FOUND if no hash of that type exists.
 */
AFF4Status GetImageHash(DataStore* resolver, const URN& subject, HashType type, AFF4Hash& hash);

/**
 * @brief Retrieve all image-level hashes for a given subject from the DataStore.
 *
 * @param resolver The DataStore to query.
 * @param subject The URN of the image.
 * @param hashes Output: vector of all image-level hashes found.
 * @return STATUS_OK on success.
 */
AFF4Status GetAllImageHashes(DataStore* resolver, const URN& subject, std::vector<AFF4Hash>& hashes);


/**
 * @brief Image verification result containing all hash comparisons.
 */
struct ImageVerifyResult {
    URN image_urn;
    std::vector<HashVerifyResult> hash_results;
    bool all_passed = true;
    std::string error_message;

    /**
     * Check if verification passed for all hashes.
     */
    bool Passed() const { return all_passed && error_message.empty(); }

    /**
     * Get a human-readable summary of the verification.
     */
    std::string Summary() const;
};


}  // namespace aff4

#endif  // SRC_AFF4_HASH_H_
