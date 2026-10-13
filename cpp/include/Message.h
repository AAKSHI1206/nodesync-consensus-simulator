#ifndef MESSAGE_H
#define MESSAGE_H

#include <string>

enum class MessageType {
  REQUEST_VOTE,
  VOTE_RESPONSE,
  APPEND_ENTRIES,
  APPEND_RESPONSE,
  CLIENT_COMMAND
};

std::string messageTypeToString(MessageType type);

class Message {
private:
  int senderId;
  int receiverId;
  int term;
  MessageType type;
  std::string payload;

public:
  Message(int sender, int receiver, int term, MessageType type,
          const std::string &payload);

  int getSenderId() const;
  int getReceiverId() const;
  int getTerm() const;
  MessageType getType() const;
  std::string getPayload() const;
};

#endif
