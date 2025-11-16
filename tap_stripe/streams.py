"""Stream class for tap-stripe."""

from pathlib import Path
from typing import Iterable, List, Optional, Set, Tuple

import pendulum
import stripe
from singer_sdk.streams import Stream
from singer_sdk.streams.core import REPLICATION_FULL_TABLE, REPLICATION_INCREMENTAL
# from stripe.api_resources.abstract import (
#     ListableAPIResource as StripeListableAPIResource,
# )
# from stripe.api_resources.list_object import ListObject as StripeListObject

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")
DEFAULT_STRIPE_PAGE_LIMIT = 100

SDK_OBJECTS = {
    "balance_transactions": stripe.BalanceTransaction,
    "charges": stripe.Charge,
    "checkout_sessions": stripe.checkout.Session,
    "coupons": stripe.Coupon,
    "customers": stripe.Customer,
    "disputes": stripe.Dispute,
    "events": stripe.Event,
    "invoices": stripe.Invoice,
    "payment_intents": stripe.PaymentIntent,
    "payouts": stripe.Payout,
    "plans": stripe.Plan,
    "promotion_codes": stripe.PromotionCode,
    "refunds": stripe.Refund,
    "subscriptions": stripe.Subscription,
    "subscription_schedules": stripe.SubscriptionSchedule,
}

EVENT_TYPE_FILTERS = {
    "charges": {
        "types": [
            "charge.captured",
            "charge.expired",
            "charge.failed",
            "charge.pending",
            "charge.refunded",
            "charge.succeeded",
            "charge.updated",
        ]
    },
    "coupons": {"type": "coupon.*"},
    "customers": {
        "types": ["customer.created", "customer.deleted", "customer.updated"]
    },
    "discounts": {"type": "customer.discount.*"},
    "disputes": {"type": "charge.dispute.*"},
    "invoices": {"type": "invoice.*"},
    "payment_intents": {"type": "payment_intent.*"},
    "payouts": {"type": "payout.*"},
    "plans": {"type": "plan.*"},
    "promotion_codes": {"type": "promotion_code.*"},
    "checkout_sessions": {"type": "checkout.session.*"},
    "subscription_schedules": {"type": "subscription_schedule.*"},
    "subscriptions": {"type": "customer.subscription.*"},
}


class StripeStream(Stream):
    """Stream class for Stripe streams."""

    is_immutable = False
    time_chunk_seconds = 60 * 24 * 24  # number of seconds in a day

    def get_starting_created_value(self, context: Optional[dict]) -> Optional[int]:
        val = self.get_starting_replication_key_value(context)
        if isinstance(val, str):
            val = pendulum.parse(val).int_timestamp
        assert isinstance(val, int)

        if self.name == "disputes":
            # For the disputes stream, increase the lookback window to 90 days (3 months).
            # This is for several reasons:
            # 1. Dispute volume is relatively small.
            # 2. Dispute entities, not events, are being ingested. That means we are not capturing updates to dispute objects.
            # This increased lookback window allows us to capture changes to any objects that occurred in the last 90 days.
            # An ideal end state is to pass the lookback window as a config parameter to the Meltano stream,
            # so it can be fine-tuned per stream.
            val = val - (self.time_chunk_seconds * 90)
        return val

    @property
    def sdk_object(self):
        if self.is_immutable:
            return SDK_OBJECTS[self.name]
        return (
            stripe.Event
            if self.replication_method == REPLICATION_INCREMENTAL
            else SDK_OBJECTS[self.name]
        )

    def _make_created_filter(self, context: Optional[dict]) -> dict:
        return {"gte": self.get_starting_created_value(context)}

    def _make_params(self, start_epoch: int, end_epoch: int, limit: int = 100) -> dict:
        if self.replication_method == REPLICATION_INCREMENTAL:
            type_filter = {} if self.is_immutable else EVENT_TYPE_FILTERS[self.name]
            other_filters = {
                "created": {"gte": start_epoch, "lte": end_epoch},
                "limit": limit,
            }
            return {**type_filter, **other_filters}

        elif self.replication_method == REPLICATION_FULL_TABLE:
            if self.name == "subscriptions":
                return {
                    "created": {"gte": start_epoch, "lte": end_epoch},
                    "limit": limit,
                    "status": "all",
                }
            else:
                return {
                    "created": {"gte": start_epoch, "lte": end_epoch},
                    "limit": limit,
                }

        else:
            raise ValueError

    def _make_time_chunks(self, context) -> Iterable[Tuple[int, int]]:
        step = self.time_chunk_seconds
        if self.name == "disputes":
            # Since dispute volume is so (relatively) low,
            # we can process dispute in a single request that spans 90 days,
            # rather than processing 90 one-day ingestion requests.
            step = step * 90

        return (
            (i, i + step)
            for i in range(
                self.get_starting_created_value(context),
                pendulum.now().int_timestamp,
                step,
            )
        )

    def _get_iterator(
        self, start_epoch: int, end_epoch: int, limit: int = 100
    ):
        params = self._make_params(start_epoch, end_epoch, limit=limit)
        return self.sdk_object.list(**params)

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        The optional `partition` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `partitions` argument.
        """

        stripe.api_key = self._config["api_key"]

        for start, end in self._make_time_chunks(context):
            self.logger.info(f"Fetching {self.name} from {pendulum.from_timestamp(start)} "
                             f"to {pendulum.from_timestamp(end)}")
            iterator = self._get_iterator(start, end)

            for row in iterator.auto_paging_iter():
                yield row.to_dict()

            self.finalize_state_progress_markers(
                {"bookmarks": {self.name: {"replication_key_value": end}}}
            )


class BalanceTransactionsStream(StripeStream):
    """Stripe Plans stream"""

    name = "balance_transactions"
    is_immutable = True
    primary_keys = ["id"]
    replication_key = "created"
    schema_filepath = SCHEMAS_DIR / "balance-transactions.schema.json"

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return balance transactions plus payout-linked transactions."""

        seen_ids: Set[str] = set()
        for record in super().get_records(context):
            record_id = record.get("id")
            if record_id:
                seen_ids.add(record_id)
            yield record

        yield from self._yield_payout_balance_transactions(context, seen_ids)

    def _yield_payout_balance_transactions(
        self, context: Optional[dict], seen_ids: Set[str]
    ) -> Iterable[dict]:
        """Fetch balance transactions scoped to individual payouts."""

        self.logger.info("Fetching payout-linked balance transactions.")

        for payout in self._iter_payouts_for_linking(context):
            payout_id = payout.get("id")
            if not payout_id:
                continue

            for transaction in self._iter_balance_transactions_for_payout(payout_id):
                transaction_id = transaction.get("id")
                if transaction_id and transaction_id in seen_ids:
                    continue

                if transaction_id:
                    seen_ids.add(transaction_id)

                if not transaction.get("payout"):
                    transaction["payout"] = payout_id

                yield transaction

    def _iter_payouts_for_linking(
        self, context: Optional[dict], limit: int = DEFAULT_STRIPE_PAGE_LIMIT
    ) -> Iterable[dict]:
        """Return payout objects to backfill payout-linked transactions."""

        for start, end in self._make_time_chunks(context):
            params = {
                "created": {"gte": start, "lte": end},
                "limit": limit,
                "expand": ["data.destination"],
            }
            iterator = stripe.Payout.list(**params)
            for payout in iterator.auto_paging_iter():
                yield payout.to_dict()

    def _iter_balance_transactions_for_payout(
        self, payout_id: str, limit: int = DEFAULT_STRIPE_PAGE_LIMIT
    ) -> Iterable[dict]:
        """Return balance transactions filtered by payout."""

        iterator = stripe.BalanceTransaction.list(limit=limit, payout=payout_id)
        for transaction in iterator.auto_paging_iter():
            yield transaction.to_dict()


class ChargesStream(StripeStream):
    """Stripe Plans stream"""

    name = "charges"
    primary_keys = ["id"]
    replication_key = "created"
    schema_filepath = SCHEMAS_DIR / "charges.schema.json"


class CheckoutSessionsStream(StripeStream):
    """Stripe Checkout Sessions stream."""

    name = "checkout_sessions"
    primary_keys = ["id"]
    replication_key = "created"
    schema_filepath = SCHEMAS_DIR / "checkout-sessions.schema.json"


class CouponsStream(StripeStream):
    """Stripe Plans stream"""

    name = "coupons"
    primary_keys = ["id"]
    replication_key = "created"
    schema_filepath = SCHEMAS_DIR / "coupons.schema.json"


class CustomersStream(StripeStream):
    """Stripe Plans stream"""

    name = "customers"
    primary_keys = ["id"]
    replication_key = "created"
    schema_filepath = SCHEMAS_DIR / "customers.schema.json"


class DiscountsStream(StripeStream):
    """Stripe Plans stream"""

    name = "discounts"
    primary_keys = ["id"]
    replication_key = "created"
    schema_filepath = SCHEMAS_DIR / "discounts.schema.json"


class DisputesStream(StripeStream):
    """Stripe Plans stream"""

    name = "disputes"
    primary_keys = ["id"]
    replication_key = "created"
    schema_filepath = SCHEMAS_DIR / "disputes.schema.json"


class InvoicesStream(StripeStream):
    """Stripe Plans stream"""

    name = "invoices"
    primary_keys = ["id"]
    replication_key = "created"
    schema_filepath = SCHEMAS_DIR / "invoices.schema.json"


class PaymentIntentsStream(StripeStream):
    """Stripe Payment Intents stream"""

    name = "payment_intents"
    is_immutable = True
    primary_keys = ["id"]
    replication_key = "created"
    schema_filepath = SCHEMAS_DIR / "payment-intents.schema.json"


class PayoutsStream(StripeStream):
    """Stripe Plans stream"""

    name = "payouts"
    is_immutable = True
    primary_keys = ["id"]
    replication_key = "created"
    schema_filepath = SCHEMAS_DIR / "payouts.schema.json"

    def _get_iterator(
        self, start_epoch: int, end_epoch: int, limit: int = DEFAULT_STRIPE_PAGE_LIMIT
    ):
        params = self._make_params(start_epoch, end_epoch, limit=limit)
        params["expand"] = ["data.destination"]
        return self.sdk_object.list(**params)


class PlansStream(StripeStream):
    """Stripe Plans stream"""

    name = "plans"
    primary_keys = ["id"]
    replication_key = "created"
    schema_filepath = SCHEMAS_DIR / "shared/plans.schema.json"


class PromotionCodesStream(StripeStream):
    """Stripe Plans stream"""

    name = "promotion_codes"
    primary_keys = ["id"]
    replication_key = "created"
    schema_filepath = SCHEMAS_DIR / "promotion-codes.schema.json"


class RefundsStream(StripeStream):
    """Stripe Plans stream"""

    name = "refunds"
    # It appears you can only update metadata on a Refund object - https://stripe.com/docs/api/refunds/update
    # Further, only the charge.refund.updated event returns a refund, with no event firing on creation -
    # https://stripe.com/docs/api/events/types#event_types-charge.refund.updated
    is_immutable = True
    primary_keys = ["id"]
    replication_key = "created"
    schema_filepath = SCHEMAS_DIR / "refunds.schema.json"


class SubscriptionSchedulesStream(StripeStream):
    """Stripe Subscriptions stream."""

    name = "subscription_schedules"
    primary_keys = ["id"]
    replication_key = "created"
    schema_filepath = SCHEMAS_DIR / "subscription-schedules.schema.json"


class SubscriptionsStream(StripeStream):
    """Stripe Subscriptions stream."""

    name = "subscriptions"
    primary_keys = ["id"]
    replication_key = "created"
    schema_filepath = SCHEMAS_DIR / "subscriptions.schema.json"
