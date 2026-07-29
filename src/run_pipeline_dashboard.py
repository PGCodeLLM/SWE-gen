"""Launch the separate k3s/PGMQ dashboard on port 8766."""

from __future__ import annotations

import argparse

from swegen.dashboard.server import serve


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8766)
    args = parser.parse_args()
    serve(args.host, args.port)


if __name__ == "__main__":
    main()
