import { useEffect, useRef, useState } from "react";
import { socketUrl } from "./api";
import { ChatConnection, type ChatMessage, type ConnectionInfo, type OutboxItem } from "./chat/ChatConnection";

export function useChat(token: string, conversationId: string | null, onState?: (frame: Record<string, any>) => void) {
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [outbox, setOutbox] = useState<OutboxItem[]>([]);
  const [connection, setConnection] = useState<ConnectionInfo>({ status: "connecting" });
  const [typing, setTyping] = useState<string | null>(null);
  const client = useRef<ChatConnection | null>(null);
  const stateHandler = useRef(onState);
  stateHandler.current = onState;

  useEffect(() => {
    setMessages([]);
    setOutbox([]);
    setTyping(null);
    if (!conversationId) return;
    const chat = new ChatConnection({
      url: socketUrl(),
      token,
      conversationId,
      listener: {
        onMessages: setMessages,
        onOutbox: setOutbox,
        onConnection: setConnection,
        onState: (frame) => stateHandler.current?.(frame),
        onTyping: (frame) => setTyping(frame.is_typing ? frame.role : null),
      },
    });
    client.current = chat;
    chat.start();
    return () => {
      chat.stop();
      client.current = null;
    };
  }, [token, conversationId]);

  return { messages, outbox, connection, typing, client };
}
