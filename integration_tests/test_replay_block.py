from pathlib import Path

import pytest
import web3

from .network import setup_custom_cronos
from .utils import ADDRS, CONTRACTS, KEYS, deploy_contract, sign_transaction


@pytest.fixture(scope="module")
def custom_cronos(tmp_path_factory):
    path = tmp_path_factory.mktemp("cronos")
    yield from setup_custom_cronos(
        path, 26000, Path(__file__).parent / "configs/low_block_gas_limit.yaml"
    )


def test_replay_block(custom_cronos):
    w3: web3.Web3 = custom_cronos.w3
    contract = deploy_contract(
        w3,
        CONTRACTS["TestMessageCall"],
        key=KEYS["community"],
    )
    iterations = 400
    for i in range(10):
        nonce = w3.eth.get_transaction_count(ADDRS["validator"])
        txs = [
            contract.functions.test(iterations).buildTransaction(
                {
                    "nonce": nonce,
                }
            ),
            contract.functions.test(iterations).buildTransaction(
                {
                    "nonce": nonce + 1,
                }
            ),
        ]
        txhashes = [
            w3.eth.send_raw_transaction(sign_transaction(w3, tx).rawTransaction)
            for tx in txs
        ]
        receipt1 = w3.eth.wait_for_transaction_receipt(txhashes[0])
        try:
            receipt2 = w3.eth.wait_for_transaction_receipt(txhashes[1], timeout=60)
        except web3.exceptions.TimeExhausted:
            # expected exception, tx2 is included but failed.
            receipt2 = None
            break
        if receipt1.blockNumber == receipt2.blockNumber:
            break
        print(
            "tx1 and tx2 are included in two different blocks, retry now.",
            receipt1.blockNumber,
            receipt2.blockNumber,
        )
    else:
        assert False, "timeout"
    assert not receipt2
    # check sender's nonce is increased twice, which means both txs are included.
    assert nonce + 2 == w3.eth.get_transaction_count(ADDRS["validator"])
    rsp = w3.provider.make_request("cronos_replayBlock", [hex(receipt1.blockNumber)])
    print("rsp", rsp)
    assert "error" not in rsp, rsp["error"]
    assert 2 == len(rsp["result"])
