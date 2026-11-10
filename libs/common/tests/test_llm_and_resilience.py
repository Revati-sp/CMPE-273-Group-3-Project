import pytest
from engagehub_common.auth import TokenError, issue_token, verify_token
from engagehub_common.config import ServiceSettings
from engagehub_common.llm import RuleBasedProvider, get_llm_provider
from engagehub_common.resilience import CircuitBreaker, CircuitOpenError, backoff_delay

SECRET = "x" * 40


@pytest.mark.parametrize(
    "text,intent",
    [
        ("Can you recommend a tent under $200?", "product"),
        ("Where is my order ORD-1001?", "order"),
        ("I want to return this and get a refund", "returns"),
        ("Why was I charged on this invoice?", "billing"),
        ("My device is not working and shows an error", "technical"),
        ("What is your warranty policy?", "faq"),
        ("I want to file a complaint about poor service", "complaint"),
        ("zzzz qqqq", "unknown"),
    ],
)
async def test_intent_classification(text, intent):
    assert (await RuleBasedProvider().classify(text)).intent == intent


async def test_escalation_signals():
    provider = RuleBasedProvider()
    assert (await provider.classify("let me speak to a person")).wants_human
    assert (await provider.classify("I will contact my lawyer")).sensitive_topic == "legal"
    assert (await provider.classify("This is a chargeback")).sensitive_topic == "fraud"
    assert (await provider.classify("I am furious, this is useless!!!")).sentiment <= -0.5
    assert (await provider.classify("Hola, necesito ayuda con mi pedido")).language == "es"


async def test_summary_mentions_key_facts():
    summary = await RuleBasedProvider().summarize(
        [
            {"role": "customer", "text": "Where is ORD-1001?"},
            {"role": "virtual_agent", "text": "It shipped."},
            {"role": "customer", "text": "This is terrible, I am furious!"},
        ]
    )
    assert "ORD-1001" in summary and "negative" in summary and "2 customer" in summary


def test_unknown_provider_fails_loudly():
    with pytest.raises(ValueError):
        get_llm_provider("nope")


def test_backoff_grows_and_is_capped():
    assert backoff_delay(0, 1.0, 100) <= 1.0
    assert 4.0 <= backoff_delay(3, 1.0, 100) <= 8.0
    assert backoff_delay(20, 1.0, 10) <= 10.0


async def test_circuit_breaker_opens_then_recovers():
    now = [0.0]
    breaker = CircuitBreaker("dep", failure_threshold=2, reset_seconds=10, clock=lambda: now[0])

    async def fail():
        raise ConnectionError

    async def ok():
        return "ok"

    for _ in range(2):
        with pytest.raises(ConnectionError):
            await breaker.call(fail)
    with pytest.raises(CircuitOpenError):
        await breaker.call(ok)
    now[0] = 11
    assert await breaker.call(ok) == "ok"
    assert breaker.state == "closed"


def test_tokens_verify_and_reject_tampering_and_wrong_secret():
    token = issue_token(SECRET, subject="u1", tenant_id="t1", role="customer", customer_id="u1")
    principal = verify_token(SECRET, token)
    assert principal.tenant_id == "t1" and principal.customer_id == "u1"
    with pytest.raises(TokenError):
        verify_token("y" * 40, token)
    with pytest.raises(TokenError):
        verify_token(SECRET, token[:-3] + "abc")
    expired = issue_token(SECRET, subject="u1", tenant_id="t1", role="customer", ttl_seconds=-5)
    with pytest.raises(TokenError):
        verify_token(SECRET, expired)


def test_weak_jwt_secret_is_refused():
    with pytest.raises(ValueError):
        ServiceSettings(jwt_secret="short")
