"""The eight specialised virtual agents.

Each is deterministic and backed by mock data so that the system runs without an
LLM key. A generative implementation can replace ``respond`` behind this interface.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from app.clients import DomainClient

REFUND_APPROVAL_LIMIT = 500.0
_ORDER_RE = re.compile(r"\bORD-\d+\b", re.I)
_PRICE_RE = re.compile(r"(?:under|below|less than|max|up to)\s*\$?\s*(\d+(?:\.\d+)?)", re.I)
_STOPWORDS = {
    "a", "an", "the", "i", "me", "my", "for", "to", "of", "and", "or", "is", "are", "do", "you", "have",
    "can", "could", "please", "looking", "recommend", "suggest", "want", "need", "buy", "some", "any",
    "under", "below", "less", "than", "with", "in", "on", "good", "best", "what", "which", "would", "like",
    "product", "products", "price", "stock", "max", "up", "something",
}  # fmt: skip


@dataclass(frozen=True)
class AgentTask:
    tenant_id: str
    customer_id: str
    text: str
    source_message_id: str
    language: str = "en"


@dataclass
class AgentReply:
    text: str
    escalate_reason: str | None = None
    notification: dict | None = None
    metadata: dict = field(default_factory=dict)


class VirtualAgent:
    agent_type = ""
    display_name = ""

    def __init__(self, domain: DomainClient) -> None:
        self.domain = domain

    async def respond(self, task: AgentTask) -> AgentReply:
        raise NotImplementedError


def _money(value: float) -> str:
    return f"${value:,.2f}"


def _order_id(text: str) -> str | None:
    match = _ORDER_RE.search(text)
    return match.group(0).upper() if match else None


class ProductAdvisorAgent(VirtualAgent):
    agent_type = "product_advisor"
    display_name = "Product Advisor"

    async def respond(self, task: AgentTask) -> AgentReply:
        price = _PRICE_RE.search(task.text)
        max_price = float(price.group(1)) if price else None
        words = [w for w in re.findall(r"[a-z]+", task.text.lower()) if w not in _STOPWORDS]
        products: list[dict] = []
        for word in words:
            products = await self.domain.search_products(
                task.tenant_id, task.customer_id, query=word, max_price=max_price
            )
            if products:
                break
        heading = "Here's what I found"
        if not products:
            products = await self.domain.recommendations(task.tenant_id, task.customer_id, max_price=max_price)
            heading = "I couldn't find an exact match, but these are popular"
        if not products:
            return AgentReply("I couldn't find a product matching that. Could you tell me more about what you need?")
        lines = [
            f"• {p['name']} ({_money(p['price'])}): {p['description']}" + ("" if p["stock"] > 0 else " [out of stock]")
            for p in products[:3]
        ]
        return AgentReply(
            f"{heading}:\n" + "\n".join(lines),
            metadata={"products": [p["id"] for p in products[:3]]},
        )


class OrderTrackingAgent(VirtualAgent):
    agent_type = "order_tracking"
    display_name = "Order Tracking"

    async def respond(self, task: AgentTask) -> AgentReply:
        order_id = _order_id(task.text)
        if order_id:
            order = await self.domain.get_order(task.tenant_id, task.customer_id, order_id)
            if order is None:
                return AgentReply(f"I couldn't find order {order_id} on your account. Please check the number.")
            return AgentReply(self._describe(order), metadata={"order_id": order_id})
        orders = await self.domain.list_orders(task.tenant_id, task.customer_id)
        if not orders:
            return AgentReply("I don't see any orders on your account. Do you have an order number?")
        summary = "\n".join(f"• {o['id']}: {o['status']}" for o in orders[:5])
        return AgentReply(f"Here are your recent orders:\n{summary}\nWhich one would you like details on?")

    @staticmethod
    def _describe(order: dict) -> str:
        parts = [f"Order {order['id']} is {order['status'].replace('_', ' ')}."]
        if order.get("tracking_number"):
            parts.append(f"Tracking number: {order['tracking_number']}.")
        if order.get("estimated_delivery"):
            parts.append(f"Estimated delivery: {order['estimated_delivery']}.")
        return " ".join(parts)


class ReturnsRefundsAgent(VirtualAgent):
    agent_type = "returns_refunds"
    display_name = "Returns and Refunds"

    async def respond(self, task: AgentTask) -> AgentReply:
        order_id = _order_id(task.text)
        if not order_id:
            return AgentReply("I can help with that. What's the order number (for example, ORD-1001)?")
        order = await self.domain.get_order(task.tenant_id, task.customer_id, order_id)
        if order is None:
            return AgentReply(f"I couldn't find order {order_id} on your account. Please check the number.")
        if order["status"] != "delivered":
            return AgentReply(
                f"Order {order_id} is {order['status'].replace('_', ' ')}, so it can't be returned until delivered."
            )
        if order["total"] > REFUND_APPROVAL_LIMIT:
            return AgentReply(
                f"Refunds above {_money(REFUND_APPROVAL_LIMIT)} need human approval. "
                "I'm passing this to a support agent.",
                escalate_reason="refund_approval_required",
                metadata={"order_id": order_id},
            )
        # The source message ID makes a redelivered task reuse the same return request.
        created = await self.domain.create_return(
            task.tenant_id, task.customer_id, order_id, task.text[:500], task.source_message_id
        )
        if created is None:
            return AgentReply(f"I couldn't open a return for {order_id}. Let me get someone to help.",
                              escalate_reason="return_failed")  # fmt: skip
        return AgentReply(
            f"Return {created['id']} is open for order {order_id}. "
            f"A refund of {_money(order['total'])} will be issued once we receive the item.",
            notification={
                "kind": "return_created",
                "title": "Return request created",
                "body": f"Return {created['id']} for order {order_id} is open.",
            },
            metadata={"order_id": order_id, "return_id": created["id"]},
        )


class BillingAgent(VirtualAgent):
    agent_type = "billing"
    display_name = "Billing"

    async def respond(self, task: AgentTask) -> AgentReply:
        lowered = task.text.lower()
        if any(p in lowered for p in ("charged twice", "double charge", "overcharged", "wrong amount")):
            return AgentReply(
                "I'm sorry about that. Disputed charges need a billing specialist, so I'm connecting you with one.",
                escalate_reason="billing_dispute",
            )
        order_id = _order_id(task.text)
        if order_id:
            order = await self.domain.get_order(task.tenant_id, task.customer_id, order_id)
            if order is None:
                return AgentReply(f"I couldn't find order {order_id} on your account.")
            return AgentReply(
                f"Invoice for {order_id}: {_money(order['total'])}, payment status {order['payment_status']}.",
                metadata={"order_id": order_id},
            )
        return AgentReply("Happy to help with billing. Which order number is this about?")


class TechnicalSupportAgent(VirtualAgent):
    agent_type = "technical_support"
    display_name = "Technical Support"

    _GUIDES = (
        (("won't turn on", "not turning on", "power"), "Hold the power button for 10 seconds, then charge for 30 minutes."),
        (("wifi", "wi-fi", "network", "pair", "bluetooth"), "Forget the device in your settings, restart it, then pair again."),
        (("password", "login", "log in", "sign in"), "Use 'Forgot password' on the sign-in page, and check your spam folder."),
        (("firmware", "update"), "Install the latest firmware from Settings > About > Update, on a full charge."),
        (("error", "crash", "bug", "freez"), "Restart the device and install any updates. Note the error code if it returns."),
    )  # fmt: skip

    async def respond(self, task: AgentTask) -> AgentReply:
        lowered = task.text.lower()
        for keywords, advice in self._GUIDES:
            if any(k in lowered for k in keywords):
                return AgentReply(f"{advice} Did that fix it? If not, tell me and I'll bring in a specialist.")
        if any(p in lowered for p in ("didn't work", "did not work", "still broken", "still not")):
            return AgentReply("Sorry that didn't fix it. I'm bringing in a specialist.",
                              escalate_reason="troubleshooting_exhausted")  # fmt: skip
        return AgentReply("Could you describe what's happening, including any error message?")


class FAQAgent(VirtualAgent):
    agent_type = "faq"
    display_name = "FAQ"

    _ANSWERS = (
        (("shipping", "deliver"), "Standard shipping takes 3 to 5 business days and is free above $50."),
        (("warranty",), "All products have a 1-year limited warranty covering manufacturing defects."),
        (("return policy", "policy"), "Items can be returned within 30 days of delivery in original condition."),
        (("hours", "open"), "Our support team is available 8am to 8pm Pacific, every day. Virtual agents run 24/7."),
        (("contact", "phone", "email"), "You can reach us right here in chat, any time."),
        (("international",), "We ship to over 40 countries. Rates are shown at checkout."),
    )  # fmt: skip

    async def respond(self, task: AgentTask) -> AgentReply:
        lowered = task.text.lower()
        for keywords, answer in self._ANSWERS:
            if any(k in lowered for k in keywords):
                return AgentReply(answer)
        return AgentReply("I don't have an answer for that yet. Could you rephrase it, or ask for a human agent?")


class ComplaintManagementAgent(VirtualAgent):
    agent_type = "complaint_management"
    display_name = "Complaint Management"

    async def respond(self, task: AgentTask) -> AgentReply:
        return AgentReply(
            "I'm really sorry about this experience. I've recorded your complaint so we can make it right. "
            "Can you share what happened? If you'd rather speak with a person, just say so.",
            notification={
                "kind": "complaint_logged",
                "title": "We received your complaint",
                "body": "Our team will review it and follow up.",
            },
        )


class SupervisorAgent(VirtualAgent):
    """Asks for clarification when routing is unsure. Handoffs are in worker.py."""

    agent_type = "supervisor"
    display_name = "Supervisor"

    async def respond(self, task: AgentTask) -> AgentReply:
        return AgentReply(
            "I want to make sure you reach the right specialist. "
            "Is this about a product, an order, a return, billing, or a technical problem?"
        )


AGENT_CLASSES: dict[str, type[VirtualAgent]] = {
    cls.agent_type: cls
    for cls in (
        ProductAdvisorAgent,
        OrderTrackingAgent,
        ReturnsRefundsAgent,
        BillingAgent,
        TechnicalSupportAgent,
        FAQAgent,
        ComplaintManagementAgent,
        SupervisorAgent,
    )
}
