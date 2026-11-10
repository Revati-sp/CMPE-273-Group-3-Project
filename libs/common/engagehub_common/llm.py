"""The LLM adapter boundary.

``RuleBasedProvider`` is deterministic, so the system and its tests run without
an API key. To add a real model, implement :class:`LLMProvider` and register it
in :func:`get_llm_provider`. Nothing else in the system changes.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Protocol

INTENT_TO_AGENT = {
    "product": "product_advisor",
    "order": "order_tracking",
    "returns": "returns_refunds",
    "billing": "billing",
    "technical": "technical_support",
    "faq": "faq",
    "complaint": "complaint_management",
    "unknown": "supervisor",
}


@dataclass(frozen=True)
class Classification:
    intent: str
    confidence: float
    sentiment: float  # -1.0 (very negative) to 1.0 (very positive)
    language: str = "en"
    wants_human: bool = False
    sensitive_topic: str | None = None
    signals: tuple[str, ...] = field(default=())


class LLMProvider(Protocol):
    async def classify(self, text: str) -> Classification: ...

    async def summarize(self, transcript: list[dict]) -> str:
        """Summarise ``[{"role": ..., "text": ...}]`` for a human agent."""


_KEYWORDS: dict[str, dict[str, int]] = {
    "returns": {
        "return": 3, "refund": 4, "exchange": 2, "send it back": 3, "money back": 4, "rma": 3,
    },
    "order": {
        "order": 2, "tracking": 4, "track": 3, "shipment": 3, "delivery": 3, "delivered": 2,
        "arrive": 2, "where is my": 3, "shipped": 3, "ord-": 3, "pedido": 3,
    },
    "billing": {
        "invoice": 4, "bill": 3, "billing": 4, "charged": 3, "charge": 2, "payment": 3,
        "receipt": 3, "credit card": 3, "overcharged": 4, "factura": 3,
    },
    "technical": {
        "not working": 4, "broken": 3, "error": 3, "bug": 3, "crash": 3, "won't turn on": 4,
        "reset": 2, "password": 2, "login": 2, "firmware": 3, "setup": 2, "install": 2,
        "troubleshoot": 4, "wifi": 2, "pair": 2,
    },
    "product": {
        "recommend": 4, "looking for": 3, "suggest": 3, "buy": 2, "product": 2, "price": 2,
        "compare": 3, "best": 2, "in stock": 3, "tent": 3, "boots": 3, "backpack": 3, "jacket": 3,
        "headphones": 3, "laptop": 3, "camera": 3, "speaker": 3, "phone": 2, "sleeping bag": 3,
    },
    "faq": {
        "hours": 3, "policy": 3, "shipping cost": 4, "how long": 2, "warranty": 3, "contact": 2,
        "do you": 1, "location": 2, "open": 1, "international": 2,
    },
    "complaint": {
        "complaint": 5, "complain": 5, "unacceptable": 4, "terrible": 3, "awful": 3,
        "worst": 3, "disappointed": 3, "rude": 4, "poor service": 4,
    },
}  # fmt: skip

_NEGATIVE = {
    "angry": 3, "furious": 4, "frustrated": 3, "frustrating": 3, "terrible": 2, "awful": 2, "horrible": 3,
    "worst": 3, "useless": 3, "ridiculous": 3, "unacceptable": 3, "hate": 3, "disappointed": 2,
    "annoyed": 2, "waste": 2, "scam": 4, "!!!": 2, "rude": 2,
}  # fmt: skip
_POSITIVE = {"thanks": 1, "thank you": 2, "great": 2, "awesome": 2, "perfect": 2, "love": 2, "helpful": 2}

_HUMAN_REQUEST = re.compile(
    r"\b(human|real person|live agent|real agent|representative|speak (to|with) (someone|a person|an agent)"
    r"|talk (to|with) (someone|a person|an agent)|agente humano|persona real)\b",
    re.I,
)
_SENSITIVE = {
    "legal": re.compile(r"\b(lawyer|attorney|lawsuit|sue|legal action)\b", re.I),
    "privacy": re.compile(r"\b(gdpr|ccpa|delete my (data|account)|privacy|data breach)\b", re.I),
    "fraud": re.compile(r"\b(fraud|fraudulent|chargeback|stolen card|unauthori[sz]ed)\b", re.I),
    "safety": re.compile(r"\b(injur(y|ed)|fire hazard|caught fire|unsafe|burned)\b", re.I),
}
_SPANISH = re.compile(r"\b(hola|gracias|pedido|dónde|donde|ayuda|quiero|necesito|por favor|factura)\b", re.I)


def _score(text: str, table: dict[str, int]) -> int:
    return sum(weight for phrase, weight in table.items() if phrase in text)


class RuleBasedProvider:
    async def classify(self, text: str) -> Classification:
        lowered = text.lower()
        scores = {intent: _score(lowered, table) for intent, table in _KEYWORDS.items()}
        ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
        best_intent, best = ranked[0]
        runner_up = ranked[1][1]
        confidence = 0.0 if best == 0 else min(1.0, 0.35 + best * 0.1 + (best - runner_up) * 0.05)

        negative, positive = _score(lowered, _NEGATIVE), _score(lowered, _POSITIVE)
        total = negative + positive
        sentiment = 0.0 if total == 0 else max(-1.0, min(1.0, (positive - negative) / max(total, 3)))

        sensitive = next((name for name, rx in _SENSITIVE.items() if rx.search(text)), None)
        wants_human = bool(_HUMAN_REQUEST.search(text))
        signals = tuple(
            name
            for name, on in (
                ("human_request", wants_human),
                ("negative_sentiment", sentiment <= -0.5),
                ("sensitive_topic", sensitive is not None),
                ("low_confidence", confidence < 0.5),
            )
            if on
        )
        return Classification(
            intent=best_intent if best > 0 else "unknown",
            confidence=round(confidence, 2),
            sentiment=round(sentiment, 2),
            language="es" if _SPANISH.search(text) else "en",
            wants_human=wants_human,
            sensitive_topic=sensitive,
            signals=signals,
        )

    async def summarize(self, transcript: list[dict]) -> str:
        customer = [m["text"] for m in transcript if m.get("role") == "customer"]
        agents = [m["text"] for m in transcript if m.get("role") in ("virtual_agent", "human_agent")]
        if not customer:
            return "No customer messages yet."
        parts = [f'Customer opened with: "{_clip(customer[0])}".']
        if len(customer) > 1:
            parts.append(f'Most recent message: "{_clip(customer[-1])}".')
        classifications = [await self.classify(t) for t in customer]
        intents = list(dict.fromkeys(c.intent for c in classifications if c.intent != "unknown"))
        if intents:
            parts.append(f"Topics: {', '.join(intents)}.")
        average = sum(c.sentiment for c in classifications) / len(classifications)
        mood = "negative" if average <= -0.25 else "positive" if average >= 0.25 else "neutral"
        parts.append(f"Overall sentiment: {mood}.")
        orders = sorted({o for t in customer for o in re.findall(r"ORD-\d+", t, re.I)})
        if orders:
            parts.append(f"Orders mentioned: {', '.join(o.upper() for o in orders)}.")
        if agents:
            parts.append(f'Last virtual-agent reply: "{_clip(agents[-1])}".')
        parts.append(f"{len(customer)} customer message(s), {len(agents)} agent reply(ies).")
        return " ".join(parts)


def _clip(text: str, limit: int = 140) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


def get_llm_provider(name: str = "rules") -> LLMProvider:
    if name == "rules":
        return RuleBasedProvider()
    raise ValueError(f"unknown LLM provider: {name}")
