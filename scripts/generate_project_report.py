from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from textwrap import wrap

from reportlab.lib.pagesizes import A4
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas


ROOT = Path(__file__).resolve().parent.parent
OUTPUT = ROOT / "docs" / "relatorio_execucao_epigeodata.pdf"


SECTIONS = [
    {
        "title": "Resumo do Projeto",
        "type": "text",
        "content": [
            "Este documento consolida o estado atual do repositório EpiGeoData em 2026-03-25.",
            "O projeto possui um backend Flask em app.py, interface HTML em templates/index.html, dados epidemiológicos CSV locais e camadas climáticas GeoJSON.",
            "Também existe uma estrutura Flutter no repositório, mas o fluxo operacional documentado no README e no Render está centrado no backend Python.",
            "Este PDF foi gerado automaticamente a partir dos arquivos versionados no repositório.",
        ],
    },
    {
        "title": "Arquivos Considerados",
        "type": "text",
        "content": [
            "README.md",
            "requirements.txt",
            "render.yaml",
            "app.py",
            "templates/index.html",
            "lib/main.dart",
            "data/climaticas/README.md",
        ],
    },
    {
        "title": "README.md",
        "type": "file",
        "path": ROOT / "README.md",
        "max_lines": 120,
    },
    {
        "title": "requirements.txt",
        "type": "file",
        "path": ROOT / "requirements.txt",
        "max_lines": 80,
    },
    {
        "title": "render.yaml",
        "type": "file",
        "path": ROOT / "render.yaml",
        "max_lines": 120,
    },
    {
        "title": "app.py",
        "type": "file",
        "path": ROOT / "app.py",
        "max_lines": 260,
    },
    {
        "title": "templates/index.html",
        "type": "file",
        "path": ROOT / "templates" / "index.html",
        "max_lines": 220,
    },
    {
        "title": "lib/main.dart",
        "type": "file",
        "path": ROOT / "lib" / "main.dart",
        "max_lines": 120,
    },
    {
        "title": "data/climaticas/README.md",
        "type": "file",
        "path": ROOT / "data" / "climaticas" / "README.md",
        "max_lines": 120,
    },
]


class PdfWriter:
    def __init__(self, output_path: Path):
        self.output_path = output_path
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.pdf = canvas.Canvas(str(output_path), pagesize=A4)
        self.width, self.height = A4
        self.left = 40
        self.right = self.width - 40
        self.top = self.height - 40
        self.bottom = 40
        self.y = self.top
        self.page_number = 1
        self._draw_header()
        self._draw_footer()

    def _draw_header(self) -> None:
        self.pdf.setFont("Helvetica-Bold", 14)
        self.pdf.drawString(self.left, self.height - 28, "EpiGeoData - Relatorio de Execucao do Projeto")

    def _draw_footer(self) -> None:
        self.pdf.setFont("Helvetica", 9)
        self.pdf.drawRightString(self.right, 20, f"Pagina {self.page_number}")

    def _new_page(self) -> None:
        self.pdf.showPage()
        self.page_number += 1
        self.y = self.top
        self._draw_header()
        self._draw_footer()

    def _ensure_space(self, lines: int = 1, line_height: int = 12) -> None:
        if self.y - (lines * line_height) < self.bottom:
            self._new_page()

    def add_title(self, text: str) -> None:
        self._ensure_space(2, 20)
        self.pdf.setFont("Helvetica-Bold", 16)
        self.pdf.drawString(self.left, self.y, text)
        self.y -= 22

    def add_paragraph(self, text: str, font_name: str = "Helvetica", font_size: int = 10, line_height: int = 13) -> None:
        max_width = self.right - self.left
        avg_char_width = max(stringWidth("M", font_name, font_size), 1)
        max_chars = max(int(max_width / avg_char_width), 20)
        wrapped = wrap(text, width=max_chars, break_long_words=False, break_on_hyphens=False) or [""]
        self._ensure_space(len(wrapped), line_height)
        self.pdf.setFont(font_name, font_size)
        for line in wrapped:
            self.pdf.drawString(self.left, self.y, line)
            self.y -= line_height

    def add_code_block(self, lines: list[str]) -> None:
        self.pdf.setFont("Courier", 8)
        max_width = self.right - self.left
        max_chars = max(int(max_width / stringWidth("M", "Courier", 8)), 20)
        for raw_line in lines:
            chunks = wrap(raw_line.replace("\t", "    "), width=max_chars, drop_whitespace=False) or [""]
            self._ensure_space(len(chunks), 10)
            for chunk in chunks:
                self.pdf.drawString(self.left, self.y, chunk)
                self.y -= 10

    def save(self) -> None:
        self.pdf.save()


def read_file_excerpt(path: Path, max_lines: int) -> list[str]:
    try:
        content = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        content = path.read_text(encoding="latin-1")
    lines = content.splitlines()
    excerpt = lines[:max_lines]
    if len(lines) > max_lines:
        excerpt.append("...")
        excerpt.append(f"[arquivo truncado: exibidas {max_lines} de {len(lines)} linhas]")
    return excerpt


def build_report() -> None:
    pdf = PdfWriter(OUTPUT)
    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    pdf.add_paragraph(f"Gerado em: {generated_at}", font_name="Helvetica-Bold", font_size=11)
    pdf.add_paragraph("Objetivo: registrar o script, a configuracao e os principais artefatos do projeto disponiveis no repositorio ate o momento.")

    for section in SECTIONS:
        pdf.y -= 8
        pdf.add_title(section["title"])
        if section["type"] == "text":
            for item in section["content"]:
                pdf.add_paragraph(item)
        else:
            path = section["path"]
            pdf.add_paragraph(f"Arquivo: {path.relative_to(ROOT).as_posix()}", font_name="Helvetica-Bold")
            pdf.add_code_block(read_file_excerpt(path, section["max_lines"]))

    pdf.save()


if __name__ == "__main__":
    build_report()