from engagehub_common.topics import SESSION_STATE, TOPIC_SPECS


def test_session_state_topic_is_compacted_and_history_topics_are_not():
    specs = {spec.name: spec for spec in TOPIC_SPECS}
    compacted = specs[SESSION_STATE].configs(2)
    assert compacted["cleanup.policy"] == "compact" and "retention.ms" not in compacted
    for name in ("chat.messages", "chat.replies", "agent.responses"):
        assert specs[name].configs(2)["cleanup.policy"] == "delete"
        assert int(specs[name].configs(2)["retention.ms"]) > 0


def test_every_topic_requires_two_in_sync_replicas():
    assert all(spec.configs(2)["min.insync.replicas"] == "2" for spec in TOPIC_SPECS)
