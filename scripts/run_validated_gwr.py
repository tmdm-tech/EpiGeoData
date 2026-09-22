#!/usr/bin/env python3
"""Validated GWR CLI entrypoint; never delegates to legacy generator before approval."""
from __future__ import annotations

import argparse
from stage3_pipeline_gate import require_authorized_gwr


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate scientific prerequisites before GWR")
    parser.add_argument("--table", required=True)
    parser.add_argument("--municipalities", required=True)
    parser.add_argument("--dependent", required=True)
    parser.add_argument("--independent", nargs="+", required=True)
    args = parser.parse_args()
    require_authorized_gwr(args.table, args.municipalities, args.dependent, args.independent)
    raise RuntimeError("GWR executor is not yet homologated")


if __name__ == "__main__":
    main()
