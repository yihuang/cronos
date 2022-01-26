from .utils import ADDRS, sign_transaction


def test_mempool(cronos):
    w3 = cronos.w3

    signed = sign_transaction(
        w3,
        {
            "to": ADDRS["community"],
            "value": 1000,
        },
    )
    txhash = w3.eth.send_raw_transaction(signed.rawTransaction)
    tx = w3.eth.get_transaction(txhash)
    # check the tx is indeed in mempool
    assert tx.blockHash is None
    assert tx.blockNumber is None
