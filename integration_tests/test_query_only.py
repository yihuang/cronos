from pathlib import Path

import pytest

from .network import setup_custom_cronos
from .utils import (
    ADDRS,
)

@pytest.fixture(scope="module")
def primary(tmp_path_factory):
    path = tmp_path_factory.mktemp("cronos-primary")
    yield from setup_custom_cronos(
        path, 26750, Path(__file__).parent / "configs/primary.jsonnet"
    )


@pytest.fixture(scope="module")
def replica(tmp_path_factory):
    path = tmp_path_factory.mktemp("cronos-replica")
    yield from setup_custom_cronos(
        path, 26770, Path(__file__).parent / "configs/replica.jsonnet"
    )


def test_basic(primary, replica):
    pw3 = primary.w3
    rw3 = replica.w3
    validator = ADDRS["validator"]
    community = ADDRS["community"]
    def print_balance():
        print(pw3.eth.get_balance(community))
        # print(rw3.eth.get_balance(community))

    print_balance()
    txhash = pw3.eth.send_transaction(
        {
            "from": validator,
            "to": community,
            "value": 1000,
        }
    )
    receipt = pw3.eth.wait_for_transaction_receipt(txhash)
    assert receipt.status == 1
    print_balance()
