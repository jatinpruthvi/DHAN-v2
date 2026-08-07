from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Any

from .dhan import DhanRestClient


@dataclass(frozen=True)
class OrderIntent:
    dhan_client_id: str
    transaction_type: str
    exchange_segment: str
    product_type: str
    order_type: str
    validity: str
    security_id: str
    quantity: int
    price: float
    correlation_id: str = ""

    def to_dhan_payload(self) -> dict[str, Any]:
        return {
            "dhanClientId": self.dhan_client_id,
            "correlationId": self.correlation_id,
            "transactionType": self.transaction_type,
            "exchangeSegment": self.exchange_segment,
            "productType": self.product_type,
            "orderType": self.order_type,
            "validity": self.validity,
            "securityId": self.security_id,
            "quantity": self.quantity,
            "disclosedQuantity": 0,
            "price": self.price,
            "triggerPrice": 0,
            "afterMarketOrder": False,
            "amoTime": "",
            "boProfitValue": 0,
            "boStopLossValue": 0,
        }


class ExecutionRouter:
    """Guarded execution router.

    demo_trade=True returns simulated order responses. demo_trade=False sends to Dhan via DhanRestClient.
    This module is not used by the paper-mode engine unless explicitly called.
    """

    def __init__(self, dhan_client: DhanRestClient):
        self.dhan_client = dhan_client

    def place(self, intent: OrderIntent) -> Mapping[str, Any]:
        return self.dhan_client.place_order(intent.to_dhan_payload())
