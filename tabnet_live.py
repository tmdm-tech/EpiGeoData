"""On-demand DATASUS/TABNET adapter.

TABNET is a web tabulator, not a streaming API. This client discovers the
official HTML form at request time and submits a municipality x year query.
Failures are explicit; callers may use a separately-labelled local fallback.
"""
from __future__ import annotations

from dataclasses import dataclass
from html.parser import HTMLParser
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen
import io
import pandas as pd


class _FormParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.action = None
        self.hidden = {}
        self.selects = {}
        self._select = None
        self._option = None
        self._text = []

    def handle_starttag(self, tag, attrs):
        d = dict(attrs)
        if tag == "form" and self.action is None:
            self.action = d.get("action")
        elif tag == "input" and d.get("type", "").lower() == "hidden" and d.get("name"):
            self.hidden[d["name"]] = d.get("value", "")
        elif tag == "select" and d.get("name"):
            self._select = d["name"]
            self.selects.setdefault(self._select, [])
        elif tag == "option" and self._select:
            self._option = d.get("value", "")
            self._text = []

    def handle_data(self, data):
        if self._option is not None:
            self._text.append(data)

    def handle_endtag(self, tag):
        if tag == "option" and self._select and self._option is not None:
            self.selects[self._select].append((self._option, " ".join(self._text).strip()))
            self._option = None
            self._text = []
        elif tag == "select":
            self._select = None


def _pick(options, needles):
    for value, label in options:
        normalized = label.casefold()
        if any(n in normalized for n in needles):
            return value
    return options[0][0] if options else None


@dataclass(frozen=True)
class TabnetResult:
    source_url: str
    queried_at: str
    rows: list[dict]


def query_tabnet(definition_url: str, timeout: int = 20) -> TabnetResult:
    """Query the latest table currently published by an official TABNET form."""
    from datetime import datetime, timezone

    req = Request(definition_url, headers={"User-Agent": "EpiGeoData/1.0"})
    with urlopen(req, timeout=timeout) as response:
        html = response.read().decode("latin-1", errors="strict")
        final_url = response.url

    parser = _FormParser()
    parser.feed(html)
    if not parser.action:
        raise RuntimeError("TABNET form action not found")

    params = dict(parser.hidden)
    for name, options in parser.selects.items():
        key = name.casefold()
        if key == "linha":
            params[name] = _pick(options, ("munic",))
        elif key == "coluna":
            params[name] = _pick(options, ("ano",))
        elif key == "incremento":
            params[name] = _pick(options, ("freq", "caso", "positiv"))
        elif key == "arquivos":
            # Query every currently published file so the platform receives the
            # complete series exposed by TABNET, including the newest period.
            params[name] = [value for value, _ in options if value]
        else:
            selected = _pick(options, ("todos", "todas"))
            if selected is not None:
                params.setdefault(name, selected)

    endpoint = urljoin(final_url, parser.action)
    data = urlencode({k: v for k, v in params.items() if v is not None}, doseq=True).encode("latin-1", errors="ignore")
    post = Request(endpoint, data=data, headers={
        "User-Agent": "EpiGeoData/1.0",
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": final_url,
    })
    with urlopen(post, timeout=timeout) as response:
        result_html = response.read()
        result_url = response.url

    tables = pd.read_html(io.BytesIO(result_html), decimal=",", thousands=".")
    if not tables:
        raise RuntimeError("TABNET returned no tabular result")
    table = max(tables, key=lambda frame: frame.shape[0] * max(frame.shape[1], 1))
    table.columns = [str(c).strip() for c in table.columns]
    rows = table.where(pd.notna(table), None).to_dict(orient="records")
    return TabnetResult(
        source_url=result_url,
        queried_at=datetime.now(timezone.utc).isoformat(),
        rows=rows,
    )
