#ifndef CONSISTENT_HASH_RING_H
#define CONSISTENT_HASH_RING_H

#include <cstdint>
#include <functional>
#include <map>
#include <set>
#include <string>

/*
 * ConsistentHashRing.h
 * ====================
 * Consistent hashing, used by the LoadBalancer to pick the server that
 * receives a client's request.
 *
 * Picture a circle of 2^32 positions.  Every server is placed on it many
 * times ("virtual nodes", e.g. S1#0 ... S1#49) so the servers are spread
 * evenly.  A key (the customer ID) is hashed onto the same circle and is
 * handled by the first server found going clockwise from that position.
 *
 * Why this is useful:
 *   - the same customer always reaches the same server;
 *   - when a server is down, only that server's customers move (to the
 *     next server on the circle); everybody else keeps their server.
 *
 * The circle is a std::map (sorted by position), so "first server
 * clockwise" is one lower_bound search: O(log n).
 */

class ConsistentHashRing {
public:
    explicit ConsistentHashRing(int virtualNodes = 50);

    // Put a server on the ring (virtualNodes points) / take it off.
    void addServer(int serverId);
    void removeServer(int serverId);

    // Server for a key, or -1 if the ring is empty.
    int lookup(const std::string& key) const;

    // Same, but skips servers for which isUp(id) is false (keeps going
    // clockwise).  Returns -1 if no server on the ring is up.
    int lookup(const std::string& key,
               const std::function<bool(int)>& isUp) const;

    int getServerCount() const;
    int pointsFor(int serverId) const;   // positions this server owns

    // Position of a string on the ring: 32-bit FNV-1a followed by a
    // standard mixing step so that similar strings land far apart.
    static uint32_t hash(const std::string& text);

private:
    std::map<uint32_t, int> ring_;   // position -> server ID
    std::set<int>           servers_;
    int virtualNodes_;
};

#endif
