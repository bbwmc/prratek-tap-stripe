"""Unit tests for custom stream logic."""

import pytest

import tap_stripe.streams as stream_module
from tap_stripe.streams import BalanceTransactionsStream, PayoutsStream, StripeStream
from tap_stripe.tap import TapStripe


class DummyListObject:
    """Mimic stripe.ListObject behavior for tests."""

    def __init__(self, items):
        self._items = items

    def auto_paging_iter(self):
        return iter(self._items)


class DummyStripeObject(dict):
    """Simple dict-based stand-in for stripe resources."""

    def to_dict(self):
        return dict(self)


@pytest.fixture
def tap():
    return TapStripe(
        config={
            "api_key": "sk_test_dummy",
            "start_date": "2021-01-01T00:00:00Z",
        }
    )


def test_payouts_stream_expands_destination(monkeypatch, tap):
    """Verify payout iterator requests expanded destinations."""

    stream = PayoutsStream(tap=tap)
    captured_kwargs = {}

    def fake_list(**kwargs):
        captured_kwargs.update(kwargs)
        return DummyListObject([])

    monkeypatch.setattr(
        stream_module.stripe.Payout,
        "list",
        staticmethod(fake_list),
    )

    stream._get_iterator(0, 10)
    assert captured_kwargs["expand"] == ["data.destination"]


def test_balance_transactions_include_payout_records(monkeypatch, tap):
    """Ensure payout-scoped balance transactions are fetched once."""

    stream = BalanceTransactionsStream(tap=tap)

    def fake_super_get_records(self, context):
        yield {"id": "txn_base", "payout": "po_existing"}

    monkeypatch.setattr(
        StripeStream,
        "get_records",
        fake_super_get_records,
    )

    stream._make_time_chunks = lambda context: [(0, 10)]

    payouts = [
        DummyStripeObject({"id": "po_existing"}),
        DummyStripeObject({"id": "po_new"}),
    ]
    payout_transactions = {
        "po_existing": [
            DummyStripeObject({"id": "txn_base", "payout": "po_existing"}),
            DummyStripeObject({"id": "txn_extra_existing"}),
        ],
        "po_new": [
            DummyStripeObject({"id": "txn_new_without_payout"}),
        ],
    }

    def fake_payout_list(**kwargs):
        return DummyListObject(payouts)

    def fake_balance_list(**kwargs):
        payout_id = kwargs["payout"]
        return DummyListObject(payout_transactions[payout_id])

    monkeypatch.setattr(
        stream_module.stripe.Payout,
        "list",
        staticmethod(fake_payout_list),
    )
    monkeypatch.setattr(
        stream_module.stripe.BalanceTransaction,
        "list",
        staticmethod(fake_balance_list),
    )

    records = list(stream.get_records(context=None))

    assert any(r["id"] == "txn_base" for r in records)
    assert any(r["id"] == "txn_extra_existing" for r in records)
    payout_link = next(r for r in records if r["id"] == "txn_new_without_payout")
    assert payout_link["payout"] == "po_new"

