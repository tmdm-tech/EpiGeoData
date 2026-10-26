#!/usr/bin/env python3
"""Kaggle batch adapter for EpiGeoData. No credentials or synthetic data."""
from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from datetime import datetime, timezone

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = Path(os.environ.get("EPIGEODATA_BATCH_OUTPUT", "/kaggle/working/epigeodata_output"))
ENTRY = ROOT / "scripts" / "precompute_gwr_products.py"

def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--disease", default=None)
    p.add_argument("--predictors", nargs="+", default=["temperatura", "precipitacao"])
    p.add_argument("--execute", action="store_true", help="Run validated pipeline; otherwise preflight only")
    a = p.parse_args()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    required = [ENTRY, ROOT / "app.py", ROOT / "stage3_validation.py",
                ROOT / "stage3_pipeline_gate.py",
                ROOT / "scripts" / "generate_epidemiological_gwr_maps.py"]
    missing = [str(x.relative_to(ROOT)) for x in required if not x.is_file()]
    manifest = {
        "schema_version": 1, "year": a.year, "disease": a.disease,
        "predictors": a.predictors,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "status": "missing_dependencies" if missing else "preflight_ok",
        "missing": missing, "executed": False, "artifacts": [],
    }
    if not missing and a.execute:
        cmd = [sys.executable, str(ENTRY)]
        if a.disease:
            cmd += ["--disease", a.disease, "--year", str(a.year),
                    "--predictors", *a.predictors]
        else:
            cmd += ["--year", str(a.year)]
        env = dict(os.environ, PYTHONPATH=str(ROOT) + os.pathsep + os.environ.get("PYTHONPATH", ""))
        try:
            run = subprocess.run(cmd, cwd=ROOT, env=env, text=True,
                                 capture_output=True, timeout=1800)
            (OUTPUT / "stdout.log").write_text(run.stdout[-50000:], encoding="utf-8")
            (OUTPUT / "stderr.log").write_text(run.stderr[-50000:], encoding="utf-8")
            manifest["executed"] = True
            manifest["exit_code"] = run.returncode
            manifest["status"] = "completed" if run.returncode == 0 else "failed"
            if run.returncode == 0:
                # Copy only outputs produced for the requested year, never all datasets.
                candidates = [ROOT / "data" / "climaticas" / f"painel_climatico_pe_{a.year}.csv",
                              ROOT / "data" / "climaticas" / f"manifest_climatico_pe_{a.year}.json"]
                for src in candidates:
                    if src.is_file():
                        dst = OUTPUT / src.name
                        shutil.copy2(src, dst)
                        manifest["artifacts"].append(dst.name)
                # Other GWR artifacts remain in the project's runtime directory;
                # publishing them requires a separate, verified storage adapter.
        except subprocess.TimeoutExpired:
            manifest["executed"] = True
            manifest["status"] = "timeout"
    (OUTPUT / "batch_manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(manifest, indent=2, ensure_ascii=False))
    return 0 if manifest["status"] in ("preflight_ok", "completed") else 1

if __name__ == "__main__":
    raise SystemExit(main())
