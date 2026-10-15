#include "Node.h"

Node::Node(int id)
    : nodeId(id), active(false)
{
}

// Start the node
void Node::start()
{
    active = true;
}

// Stop the node
void Node::stop()
{
    active = false;
}

// Check if active
bool Node::isActive() const
{
    return active;
}

// Get node ID
int Node::getId() const
{
    return nodeId;
}