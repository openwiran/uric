"""Run one or more simulated E2 nodes: ``python -m uric.sim --nodes 3 --ues 5``."""

from __future__ import annotations

import argparse
import asyncio
import logging

from .e2node import SimE2Node, World


def main() -> None:
    p = argparse.ArgumentParser(description="uric E2 node simulator (E2AP + E2SM-KPM v3 + E2SM-RC)")
    p.add_argument("--ric", default="127.0.0.1", help="RIC E2 address (default 127.0.0.1)")
    p.add_argument("--port", type=int, default=36421, help="RIC E2 port (default 36421)")
    p.add_argument("--transport", choices=("auto", "sctp", "tcp"), default="auto")
    p.add_argument("--nodes", type=int, default=2, help="number of gNBs")
    p.add_argument("--ues", type=int, default=4, help="UEs per gNB")
    p.add_argument("--cells", type=int, default=2, help="cells per gNB")
    p.add_argument("--plmn", default="00101")
    p.add_argument("--first-gnb-id", type=lambda s: int(s, 0), default=0xE00)
    p.add_argument("--churn", type=float, default=0.05, help="per second attach/detach probability")
    p.add_argument("--log-level", default="INFO")
    a = p.parse_args()
    logging.basicConfig(level=a.log_level.upper(), format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")

    async def run():
        world = World()
        nodes = [SimE2Node(a.first_gnb_id + i, a.plmn, a.cells, a.ues, world, a.churn) for i in range(a.nodes)]
        await asyncio.gather(*(n.run(a.ric, a.port, a.transport) for n in nodes))

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
