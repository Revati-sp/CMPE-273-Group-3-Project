// k6 version of the chat load test: k6 run -e GATEWAY=http://localhost:18080 scripts/load/k6-chat.js
// Each virtual user registers, opens a conversation and chats on /ws (support chat).
import http from "k6/http";
import ws from "k6/ws";
import { check } from "k6";
import { Counter, Trend, Rate } from "k6/metrics";

const GATEWAY = __ENV.GATEWAY || "http://localhost:18080";
const WS_URL = GATEWAY.replace(/^http/, "ws") + "/ws";
const QUESTIONS = [
  "Can you recommend a tent under $200?",
  "Where is my order ORD-1001?",
  "What is your warranty policy?",
  "Has ORD-1002 shipped yet?",
];

const ackLatency = new Trend("engagehub_ack_latency_ms", true);
const replyLatency = new Trend("engagehub_reply_latency_ms", true);
const errors = new Rate("engagehub_errors");
const replies = new Counter("engagehub_agent_replies");

export const options = {
  scenarios: {
    customers: { executor: "ramping-vus", startVUs: 0, stages: [
      { duration: "30s", target: 50 }, { duration: "2m", target: 50 }, { duration: "30s", target: 0 },
    ] },
  },
  thresholds: { engagehub_errors: ["rate<0.05"], engagehub_reply_latency_ms: ["p(95)<5000"] },
};

export default function () {
  const email = `k6-${__VU}-${__ITER}-${Date.now()}@acme.example`;
  const reg = http.post(`${GATEWAY}/api/auth/register`, JSON.stringify(
    { tenant_id: "acme", email, name: `k6 ${__VU}`, password: "load-test-password" }),
    { headers: { "Content-Type": "application/json" } });
  if (!check(reg, { registered: (r) => r.status === 201 })) { errors.add(1); return; }
  const token = reg.json("access_token");
  const conv = http.post(`${GATEWAY}/api/chat/conversations`, "{}",
    { headers: { "Content-Type": "application/json", Authorization: `Bearer ${token}` } });
  if (!check(conv, { conversation: (r) => r.status === 201 })) { errors.add(1); return; }
  const conversationId = conv.json("conversation_id");

  ws.connect(WS_URL, {}, (socket) => {
    let sentAt = 0, acked = false, turns = 0, clientId = "";
    const ask = () => {
      clientId = `${__VU}-${__ITER}-${turns}-${Math.random()}`.slice(0, 60);
      acked = false;
      sentAt = Date.now();
      socket.send(JSON.stringify({ type: "message", client_message_id: clientId.padEnd(8, "x"),
        text: QUESTIONS[(__VU + turns) % QUESTIONS.length] }));
    };
    socket.on("open", () => socket.send(JSON.stringify(
      { type: "hello", token, conversation_id: conversationId, last_seen_seq: 0 })));
    socket.on("message", (raw) => {
      const frame = JSON.parse(raw);
      if (frame.type === "replay_complete" && turns === 0) ask();
      else if (frame.type === "message_ack") { ackLatency.add(Date.now() - sentAt); acked = true; }
      else if (frame.type === "message" && acked && frame.sender.type !== "customer") {
        replyLatency.add(Date.now() - sentAt);
        replies.add(1);
        errors.add(0);
        turns += 1;
        if (turns >= 4) socket.close(); else socket.setTimeout(ask, 1000);
      } else if (frame.type === "error") errors.add(1);
    });
    socket.setTimeout(() => { errors.add(1); socket.close(); }, 60000);
  });
}
