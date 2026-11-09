#include "ConsistentHashRing.h"

/*
 * ConsistentHashRing.cpp
 * ======================
 * See ConsistentHashRing.h for the idea.
 */

ConsistentHashRing::ConsistentHashRing(int virtualNodes)
    : virtualNodes_(virtualNodes < 1 ? 1 : virtualNodes)
{
}

uint32_t ConsistentHashRing::hash(const std::string& text)
{
    // FNV-1a: start from the offset basis, then for every byte
    // XOR it in and multiply by the FNV prime.
    uint32_t h = 2166136261u;
    for (unsigned char c : text) {
        h ^= c;
        h *= 16777619u;
    }
    // Final mix (MurmurHash3 fmix32): FNV-1a alone keeps strings that
    // differ only in their last character close together on the ring.
    h ^= h >> 16;
    h *= 0x85ebca6bu;
    h ^= h >> 13;
    h *= 0xc2b2ae35u;
    h ^= h >> 16;
    return h;
}

void ConsistentHashRing::addServer(int serverId)
{
    if (!servers_.insert(serverId).second) {
        return;   // already on the ring
    }
    for (int v = 0; v < virtualNodes_; v++) {
        std::string point = "S" + std::to_string(serverId) + "#" + std::to_string(v);
        ring_.emplace(hash(point), serverId);   // keeps the first owner on a collision
    }
}

void ConsistentHashRing::removeServer(int serverId)
{
    if (servers_.erase(serverId) == 0) {
        return;
    }
    for (auto it = ring_.begin(); it != ring_.end();) {
        if (it->second == serverId) {
            it = ring_.erase(it);
        } else {
            ++it;
        }
    }
}

int ConsistentHashRing::lookup(const std::string& key) const
{
    return lookup(key, [](int) { return true; });
}

int ConsistentHashRing::lookup(const std::string& key,
                               const std::function<bool(int)>& isUp) const
{
    if (ring_.empty()) {
        return -1;
    }
    // First position at or after the key's position; wrap to the start.
    auto it = ring_.lower_bound(hash(key));
    for (size_t step = 0; step < ring_.size(); step++) {
        if (it == ring_.end()) {
            it = ring_.begin();
        }
        if (isUp(it->second)) {
            return it->second;
        }
        ++it;
    }
    return -1;   // every server is down
}

int ConsistentHashRing::getServerCount() const
{
    return static_cast<int>(servers_.size());
}

int ConsistentHashRing::pointsFor(int serverId) const
{
    int count = 0;
    for (const auto& point : ring_) {
        if (point.second == serverId) count++;
    }
    return count;
}
