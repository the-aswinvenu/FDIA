#include "aff4/aff4_archive.h"
#include <iostream>
#include <vector>
#include <lz4.h>
#include <cstring>

using namespace aff4;

int main(int argc, char** argv) {
    if (argc < 2) return 1;
    FILE* f = fopen(argv[1], "rb");
    if (!f) return 1;

    uint64_t uncompressed_size = 0;
    fread(&uncompressed_size, 1, sizeof(uint64_t), f);

    fseek(f, 0, SEEK_END);
    size_t compressed_size = ftell(f) - sizeof(uint64_t);
    fseek(f, sizeof(uint64_t), SEEK_SET);

    std::vector<uint8_t> compressed(compressed_size);
    fread(compressed.data(), 1, compressed_size, f);
    fclose(f);

    std::vector<uint8_t> uncompressed(uncompressed_size);
    LZ4_decompress_safe((const char*)compressed.data(), (char*)uncompressed.data(), compressed_size, uncompressed_size);

    SegmentHeaderV1* seg = (SegmentHeaderV1*)(uncompressed.data() + 8);
    ChunkRefV1* refs = (ChunkRefV1*)(uncompressed.data() + 8 + sizeof(SegmentHeaderV1));

    for (uint32_t i = 0; i < 5; i++) { // Print first 5
        printf("Chunk %d: Bevy %d, Offset %llu, Size %u\n", i, refs[i].bevy_id, (unsigned long long)refs[i].offset, refs[i].uncompressed_size);
    }
    return 0;
}
