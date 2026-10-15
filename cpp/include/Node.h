#ifndef NODE_H
#define NODE_H

class Node {
private:
  int nodeId;
  bool active;

public:
  Node(int id);

  void start();
  void stop();

  bool isActive() const;
  int getId() const;
};

#endif