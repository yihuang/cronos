import os
import signal
import subprocess
from functools import partial
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path
from threading import Thread
from typing import NamedTuple

import pytest
import requests
from pystarport import ports

from .network import Cronos
from .utils import ADDRS, wait_for_fn, wait_for_port


class Network(NamedTuple):
    primary: Cronos
    replica: Cronos


class QuietServer(SimpleHTTPRequestHandler):
    def __init__(self, dir: str, *args, **kwargs):
        super().__init__(directory=dir, *args, **kwargs)

    def log_message(self, format, *args):
        pass


def exec(config, path, base_port):
    cmd = [
        "pystarport",
        "init",
        "--config",
        config,
        "--data",
        path,
        "--base_port",
        str(base_port),
        "--no_remove",
    ]
    print(*cmd)
    subprocess.run(cmd, check=True)
    return subprocess.Popen(
        ["pystarport", "start", "--data", path, "--quiet"],
        preexec_fn=os.setsid,
    )


@pytest.fixture(scope="module")
def network(tmp_path_factory):
    chain_id = "cronos_777-1"
    base = Path(__file__).parent / "configs"
    # primary
    path0 = tmp_path_factory.mktemp("cronos-primary")
    base_port0 = 26750
    procs = [exec(base / "primary.jsonnet", path0, base_port0)]

    # http server
    dir = path0 / f"{chain_id}/node0/data/file_streamer"
    print("dir: ", dir)
    port = 8080
    httpd = HTTPServer(("localhost", port), partial(QuietServer, dir))
    thread = Thread(target=httpd.serve_forever)
    thread.setDaemon(True)
    thread.start()
    wait_for_port(port)

    # replica
    path1 = tmp_path_factory.mktemp("cronos-replica")
    base_port1 = 26770
    procs.append(exec(base / "replica.jsonnet", path1, base_port1))
    try:
        wait_for_port(ports.evmrpc_port(base_port0))
        wait_for_port(ports.grpc_port(base_port1))
        yield Network(Cronos(path0 / chain_id), Cronos(path1 / chain_id))
    finally:
        httpd.shutdown()
        for proc in procs:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            proc.wait()
            print("killed:", proc.pid)


def grpc_call(p, address):
    url = f"http://127.0.0.1:{p}/cosmos/bank/v1beta1/balances/{address}"
    response = requests.get(url)
    if not response.ok:
        raise Exception(
            f"response code: {response.status_code}, "
            f"{response.reason}, {response.json()}"
        )
    result = response.json()
    if result.get("code"):
        raise Exception(result["raw_log"])
    return result["balances"]


def test_basic(network):
    pw3 = network.primary.w3
    pcli = network.primary.cosmos_cli()
    validator = pcli.address("validator")
    community = pcli.address("community")
    print("address: ", validator, community)
    replica_grpc_port = ports.api_port(network.replica.base_port(0))

    def check_balances():
        pbalances = [pcli.balances(community), pcli.balances(validator)]
        rbalances = [
            grpc_call(replica_grpc_port, community),
            grpc_call(replica_grpc_port, validator),
        ]
        print("primary", pbalances)
        print("replica", rbalances)
        return pbalances == rbalances

    txhash = pw3.eth.send_transaction(
        {
            "from": ADDRS["validator"],
            "to": ADDRS["community"],
            "value": 1000,
        }
    )
    receipt = pw3.eth.wait_for_transaction_receipt(txhash)
    assert receipt.status == 1
    wait_for_fn("cross-check-balances", check_balances, timeout=50)
