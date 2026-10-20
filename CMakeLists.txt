cmake_minimum_required(VERSION 3.16)
project(NodeSync LANGUAGES C CXX)

# ── Language standards ────────────────────────────────────────────────
set(CMAKE_C_STANDARD   11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# ── Include directories ──────────────────────────────────────────────
include_directories(
    ${CMAKE_SOURCE_DIR}/dsa_c/include
    ${CMAKE_SOURCE_DIR}/cpp/include
)

# ── C DSA library (compiled as C) ────────────────────────────────────
add_library(dsa_c STATIC
    dsa_c/src/linked_list.c
    dsa_c/src/queue.c
    dsa_c/src/log_structure.c
)

# ── C++ core library (Node, Message, Network, Storage, Raft) ─────────
add_library(nodesync_core STATIC
    cpp/src/Node.cpp
    cpp/src/Message.cpp
    cpp/src/Network.cpp
    cpp/src/Storage.cpp
    cpp/src/Server.cpp
    cpp/src/LoadBalancer.cpp
    cpp/src/RaftEngine.cpp
    cpp/src/RaftCluster.cpp
)
target_link_libraries(nodesync_core PUBLIC dsa_c)

# ── Main executable ──────────────────────────────────────────────────
add_executable(nodesync
    cpp/src/main.cpp
)
target_link_libraries(nodesync PRIVATE nodesync_core)

# ── Tests ────────────────────────────────────────────────────────────
enable_testing()

# Phase 1 tests
add_executable(test_phase1
    tests/test_phase1.cpp
    cpp/src/Storage.cpp
)
target_link_libraries(test_phase1 PRIVATE dsa_c)
add_test(NAME Phase1Tests COMMAND test_phase1)

# Individual Basic Block CLI tests
add_executable(test_queue
    tests/test_queue.cpp
)
target_link_libraries(test_queue PRIVATE dsa_c)

add_executable(test_linked_list
    tests/test_linked_list.cpp
)
target_link_libraries(test_linked_list PRIVATE dsa_c)

add_executable(test_log_structure
    tests/test_log_structure.cpp
)
target_link_libraries(test_log_structure PRIVATE dsa_c)

add_executable(test_storage
    tests/test_storage.cpp
    cpp/src/Storage.cpp
)
target_link_libraries(test_storage PRIVATE dsa_c)

# Node tests (Sidharth)
add_executable(test_node
    cpp/tests/test_node.cpp
)
target_link_libraries(test_node PRIVATE nodesync_core)
add_test(NAME NodeTests COMMAND test_node)

# Message tests (Aakshi)
add_executable(test_message
    cpp/tests/test_message.cpp
)
target_link_libraries(test_message PRIVATE nodesync_core)
add_test(NAME MessageTests COMMAND test_message)

# Network tests (Astik)
add_executable(test_network
    cpp/tests/test_network.cpp
)
target_link_libraries(test_network PRIVATE nodesync_core)
add_test(NAME NetworkTests COMMAND test_network)

# Communication integration tests (Sanjam)
add_executable(test_communication
    cpp/tests/test_communication.cpp
)
target_link_libraries(test_communication PRIVATE nodesync_core)
add_test(NAME CommunicationTests COMMAND test_communication)

# Load Balancer tests
add_executable(test_loadbalancer
    cpp/tests/test_loadbalancer.cpp
)
target_link_libraries(test_loadbalancer PRIVATE nodesync_core)
add_test(NAME LoadBalancerTests COMMAND test_loadbalancer)

# ── Feature 3: Raft Leader Election Tests ────────────────────────────

# Raft Engine unit tests
add_executable(test_raft_engine
    cpp/tests/test_raft_engine.cpp
)
target_link_libraries(test_raft_engine PRIVATE nodesync_core)
add_test(NAME RaftEngineTests COMMAND test_raft_engine)

# Election integration tests
add_executable(test_election
    cpp/tests/test_election.cpp
)
target_link_libraries(test_election PRIVATE nodesync_core)
add_test(NAME ElectionTests COMMAND test_election)

# Storage metadata tests
add_executable(test_storage_metadata
    cpp/tests/test_storage_metadata.cpp
)
target_link_libraries(test_storage_metadata PRIVATE nodesync_core)
add_test(NAME StorageMetadataTests COMMAND test_storage_metadata)

# Simulation clock tests
add_executable(test_simulation
    cpp/tests/test_simulation.cpp
)
target_link_libraries(test_simulation PRIVATE nodesync_core)
add_test(NAME SimulationTests COMMAND test_simulation)

# ── Feature 4: Log Replication and Commitment Tests ──────────────────

# Storage operations and conflict resolution
add_executable(test_log_storage
    cpp/tests/test_log_storage.cpp
)
target_link_libraries(test_log_storage PRIVATE nodesync_core)
add_test(NAME LogStorageTests COMMAND test_log_storage)

# AppendEntries protocol, progress tracking and commitment
add_executable(test_append_entries
    cpp/tests/test_append_entries.cpp
)
target_link_libraries(test_append_entries PRIVATE nodesync_core)
add_test(NAME AppendEntriesTests COMMAND test_append_entries)

# Stale, duplicate and queued replication messages
add_executable(test_replication_messages
    cpp/tests/test_replication_messages.cpp
)
target_link_libraries(test_replication_messages PRIVATE nodesync_core)
add_test(NAME ReplicationMessageTests COMMAND test_replication_messages)

# Cluster-level replication scenarios
add_executable(test_log_replication
    cpp/tests/test_log_replication.cpp
)
target_link_libraries(test_log_replication PRIVATE nodesync_core)
add_test(NAME LogReplicationTests COMMAND test_log_replication)
