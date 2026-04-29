from rpcstream.adapters.evm.dag import resolve_internal_entities, resolve_sink_entities


def test_resolve_internal_entities_adds_dependencies_for_transactions_and_logs():
    assert resolve_internal_entities(["transaction", "log"]) == [
        "block",
        "transaction",
        "receipt",
        "log",
    ]


def test_resolve_sink_entities_excludes_internal_receipt_dependency():
    assert resolve_sink_entities(["receipt", "transaction", "trace"]) == [
        "transaction",
        "trace",
    ]
