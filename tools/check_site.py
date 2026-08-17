"""Validate the dependency-free static documentation site."""

from __future__ import annotations

import sys
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import unquote, urlsplit


class PageParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.ids: set[str] = set()
        self.references: list[tuple[str, str]] = []
        self.has_description = False
        self.has_h1 = False
        self.has_skip_link = False
        self.language: str | None = None
        self.title_parts: list[str] = []
        self._in_title = False

    def handle_starttag(self, tag, attrs):
        values = dict(attrs)
        if tag == "html":
            self.language = values.get("lang")
        identifier = values.get("id")
        if identifier:
            self.ids.add(identifier)
        if tag == "meta" and values.get("name") == "description":
            self.has_description = bool(values.get("content"))
        if tag == "h1":
            self.has_h1 = True
        if tag == "a":
            href = values.get("href")
            if href:
                self.references.append(("href", href))
            if "skip-link" in values.get("class", "").split():
                self.has_skip_link = True
        if tag in {"link", "script", "img"}:
            attribute = "href" if tag == "link" else "src"
            reference = values.get(attribute)
            if reference:
                self.references.append((attribute, reference))
        if tag == "title":
            self._in_title = True

    def handle_endtag(self, tag):
        if tag == "title":
            self._in_title = False

    def handle_data(self, data):
        if self._in_title:
            self.title_parts.append(data)

    @property
    def title(self):
        return "".join(self.title_parts).strip()


def _target(page: Path, site: Path, reference: str):
    parsed = urlsplit(reference)
    if parsed.scheme or parsed.netloc or reference.startswith(("mailto:", "tel:")):
        return None, parsed.fragment
    path = unquote(parsed.path)
    if not path:
        return page, parsed.fragment
    if path.startswith("/"):
        candidate = site / path.removeprefix("anvil-events/").lstrip("/")
    else:
        candidate = page.parent / path
    if path.endswith("/"):
        candidate /= "index.html"
    return candidate.resolve(), parsed.fragment


def check_site(site: Path) -> list[str]:
    site = site.resolve()
    errors: list[str] = []
    parsed_pages: dict[Path, PageParser] = {}
    pages = sorted(site.rglob("*.html"))
    required = {
        site / "index.html",
        site / "architecture" / "index.html",
        site / "get-started" / "index.html",
        site / "deep-dive" / "index.html",
    }
    missing = sorted(required - set(pages))
    errors.extend(f"missing required page: {path.relative_to(site)}" for path in missing)
    if not (site / ".nojekyll").exists():
        errors.append("missing site/.nojekyll")

    for page in pages:
        parser = PageParser()
        parser.feed(page.read_text(encoding="utf-8"))
        parsed_pages[page.resolve()] = parser
        label = page.relative_to(site)
        if parser.language != "en":
            errors.append(f"{label}: html language must be en")
        if not parser.title:
            errors.append(f"{label}: missing title")
        if not parser.has_description:
            errors.append(f"{label}: missing meta description")
        if not parser.has_h1:
            errors.append(f"{label}: missing h1")
        if not parser.has_skip_link:
            errors.append(f"{label}: missing skip link")

    for page, parser in parsed_pages.items():
        label = page.relative_to(site)
        for attribute, reference in parser.references:
            target, fragment = _target(page, site, reference)
            if target is None:
                continue
            if not target.exists():
                errors.append(f"{label}: broken {attribute} {reference!r}")
                continue
            if fragment and target.suffix == ".html":
                target_parser = parsed_pages.get(target)
                if target_parser is None:
                    target_parser = PageParser()
                    target_parser.feed(target.read_text(encoding="utf-8"))
                    parsed_pages[target] = target_parser
                if fragment not in target_parser.ids:
                    errors.append(f"{label}: missing fragment {reference!r}")

    public_text = "\n".join(
        path.read_text(encoding="utf-8", errors="replace")
        for path in sorted(site.rglob("*")) if path.is_file()
    ).lower()
    for forbidden in (
        "fakoli dark", "fakoli mini", "ai-mbp25", "mid mod",
        "deepseek", "bearer token", "private key-----",
    ):
        if forbidden in public_text:
            errors.append(f"public site contains private/operator term: {forbidden!r}")

    return errors


def main():
    repository = Path(__file__).resolve().parents[1]
    errors = check_site(repository / "site")
    if errors:
        print("Static site validation failed:")
        for error in errors:
            print(f"- {error}")
        return 1
    pages = len(list((repository / "site").rglob("*.html")))
    print(f"Static site validation passed: {pages} HTML pages")
    return 0


if __name__ == "__main__":
    sys.exit(main())
