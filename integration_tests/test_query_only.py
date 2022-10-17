from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from threading import Thread
from typing import NamedTuple

import pytest

from .network import Cronos, setup_custom_cronos
from .utils import ADDRS, wait_for_port

class Network(NamedTuple):
    primary: Cronos
    replica: Cronos


@pytest.fixture(scope="module")
def network(request, tmp_path_factory):
    base = Path(__file__).parent / "configs"
    # primary
    path = tmp_path_factory.mktemp("cronos-primary")
    gen = setup_custom_cronos(
        path, 26750, base / "primary.jsonnet"
    )
    primary = next(gen)
    print("primary path:", path)
    # http server
    dir = path / "cronos_777-1/node0/data/file_streamer"
    print("dir: ", dir)
    port = 8080
    handler = partial(SimpleHTTPRequestHandler, directory=dir)
    httpd = HTTPServer(("localhost", port), handler)
    thread = Thread(target=httpd.serve_forever)
    thread.setDaemon(True)
    thread.start()
    wait_for_port(port)
    # replica
    path = tmp_path_factory.mktemp("cronos-replica")
    gen = setup_custom_cronos(
        path, 26770, base / "replica.jsonnet"
    )
    replica = next(gen)
    yield Network(primary, replica)


def test_basic(network):
    pw3 = network.primary.w3
    rw3 = network.replica.w3
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
