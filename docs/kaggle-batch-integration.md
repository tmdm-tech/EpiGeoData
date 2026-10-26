# EpiGeoData — Kaggle batch integration (draft)

This branch adds an **opt-in, fail-closed** batch adapter; it does **not** deploy a production Kaggle integration or change the frontend.

## Execute inside a Kaggle notebook
1. Attach the complete EpiGeoData repository as a Kaggle dataset and copy it to `/kaggle/working/EpiGeoData`.
2. Ensure the branch's `scripts/kaggle_batch_adapter.py` is included in that copy.
3. Preflight (does not process data):

```bash
%cd /kaggle/working/EpiGeoData
!python scripts/kaggle_batch_adapter.py --year 2022
```

4. Climate processing for a year (requires the actual official data and working dependencies):

```bash
!python scripts/kaggle_batch_adapter.py --year 2022 --execute
```

5. A **separately authorized** disease-specific GWR attempt:

```bash
!python scripts/kaggle_batch_adapter.py --year 2022 --disease esquistossomose --execute
```

The adapter writes `/kaggle/working/epigeodata_output/batch_manifest.json`, logs, and available climate products. Errors are reported rather than replaced with synthetic data. The pre-existing scientific validation gate remains mandatory.

## Still required before Render integration
- Confirm that the official INMET/DATASUS/IBGE inputs can be accessed and their provenance checked in Kaggle.
- Test a complete run with a validated dataset and confirm memory, duration and outputs.
- Decide on a **persistent** artifact store and secure upload mechanism (the Kaggle working directory is ephemeral).
- Implement job dispatch and status polling (Kaggle notebook sessions are not an always-on API).
- Wire Render to published, verified artifacts and failure statuses.
- Add integration tests and review the changes before merging into `main`.

No API keys should be committed to this repository. Avoid putting individual-level health data in public Kaggle datasets.
