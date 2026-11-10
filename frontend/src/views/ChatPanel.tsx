import { type FormEvent, useEffect, useRef, useState } from "react";
import type { ChatConnection, ChatMessage, ConnectionInfo, OutboxItem } from "../chat/ChatConnection";

const ERROR_TEXT: Record<string, string> = {
  conversation_resolved: "This conversation is resolved, so the message wasn't sent.",
  not_assigned: "You aren't assigned to this conversation, so the message wasn't sent.",
  read_only: "Admins can view conversations but can't send messages.",
  invalid_frame: "The message couldn't be sent. It may be too long.",
};

export function ConnectionBanner({ info }: { info: ConnectionInfo }) {
  if (info.status === "closed") {
    return info.closeReason ? <div className="banner banner-bad" role="alert">{info.closeReason}</div> : null;
  }
  if (info.status === "reconnecting") {
    return (
      <div className="banner banner-warn" role="status">
        Connection lost. Reconnecting (attempt {info.reconnectAttempt}). Your messages are saved and will be sent.
      </div>
    );
  }
  if (info.status === "online" && info.migrated) {
    return (
      <div className="banner banner-good" role="status">
        Conversation moved to server {info.pod?.slice(0, 12)}. {info.lastReplayCount ?? 0} missed message(s) restored
        {info.lastReplaySource ? ` from ${info.lastReplaySource}` : ""}.
      </div>
    );
  }
  return null;
}

function Bubble({ message, mine }: { message: ChatMessage; mine: boolean }) {
  const kind = message.event_type === "system_message" ? "system" : mine ? "mine" : "theirs";
  const time = new Date(message.timestamp).toLocaleTimeString([], { hour: "numeric", minute: "2-digit" });
  if (kind === "system") {
    return <li className="bubble-system">{message.text}</li>;
  }
  return (
    <li className={`bubble-row ${kind}`}>
      <div className={`bubble bubble-${kind}`}>
        {!mine && <div className="bubble-sender">{message.sender.name ?? "Support"}</div>}
        <div className="bubble-text">{message.text}</div>
        <div className="bubble-meta" title={`Sequence ${message.sequence_number}, message ${message.message_id}`}>
          {time} · #{message.sequence_number}
        </div>
      </div>
    </li>
  );
}

interface Props {
  messages: ChatMessage[];
  outbox: OutboxItem[];
  connection: ConnectionInfo;
  typing: string | null;
  client: { current: ChatConnection | null };
  selfSenderType: string;
  disabled?: boolean;
  disabledText?: string;
  placeholder?: string;
}

export function ChatPanel({ messages, outbox, connection, typing, client, selfSenderType, disabled, disabledText, placeholder }: Props) {
  const [draft, setDraft] = useState("");
  const end = useRef<HTMLLIElement>(null);
  const typingTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  useEffect(() => {
    end.current?.scrollIntoView({ block: "end" });
  }, [messages.length, outbox.length]);

  function submit(event: FormEvent) {
    event.preventDefault();
    const text = draft.trim();
    if (!text) return;
    client.current?.send(text);
    client.current?.setTyping(false);
    setDraft("");
  }

  function onDraft(value: string) {
    setDraft(value);
    client.current?.setTyping(true);
    if (typingTimer.current) clearTimeout(typingTimer.current);
    typingTimer.current = setTimeout(() => client.current?.setTyping(false), 2000);
  }

  return (
    <div className="chat">
      <ConnectionBanner info={connection} />
      <ul className="chat-log" aria-live="polite">
        {messages.map((message) => (
          <Bubble key={message.message_id} message={message} mine={message.sender.type === selfSenderType} />
        ))}
        {outbox.map((item) => (
          <li key={item.clientMessageId} className="bubble-row mine">
            <div className={`bubble bubble-mine pending ${item.failed ? "failed" : ""}`}>
              <div className="bubble-text">{item.text}</div>
              {item.failed ? (
                <div className="bubble-meta">
                  {ERROR_TEXT[item.failed] ?? "The message wasn't sent."}{" "}
                  <button className="link" onClick={() => client.current?.dismiss(item.clientMessageId)}>Dismiss</button>
                </div>
              ) : (
                <div className="bubble-meta">Sending…</div>
              )}
            </div>
          </li>
        ))}
        {typing && <li className="typing">{typing === "customer" ? "Customer" : "Agent"} is typing…</li>}
        <li ref={end} aria-hidden="true" />
      </ul>
      {disabled ? (
        <div className="chat-closed">{disabledText}</div>
      ) : (
        <form className="chat-input" onSubmit={submit}>
          <input
            aria-label="Message"
            value={draft}
            maxLength={4000}
            placeholder={placeholder ?? "Type a message"}
            onChange={(event) => onDraft(event.target.value)}
          />
          <button className="primary" type="submit" disabled={!draft.trim()}>Send</button>
        </form>
      )}
    </div>
  );
}
