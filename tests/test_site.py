import tempfile
import unittest
from pathlib import Path

from tools.check_site import check_site

ROOT = Path(__file__).resolve().parents[1]


class TestDocumentationSite(unittest.TestCase):
    def test_site_contract_and_internal_links(self):
        self.assertEqual(check_site(ROOT / "site"), [])

    def test_checker_reports_broken_local_link(self):
        with tempfile.TemporaryDirectory() as directory:
            site = Path(directory)
            for relative in (
                "index.html", "architecture/index.html",
                "get-started/index.html", "deep-dive/index.html",
            ):
                page = site / relative
                page.parent.mkdir(parents=True, exist_ok=True)
                href = "missing/" if relative == "index.html" else "../"
                page.write_text(
                    '<!doctype html><html lang="en"><head>'
                    '<title>Test</title><meta name="description" content="x">'
                    '</head><body><a class="skip-link" href="#main">Skip</a>'
                    f'<h1 id="main">Test</h1><a href="{href}">Link</a>'
                    '</body></html>',
                    encoding="utf-8",
                )
            (site / ".nojekyll").touch()
            errors = check_site(site)
        self.assertTrue(any("broken href 'missing/'" in error for error in errors))

    def test_pypi_is_not_presented_as_current_install_source(self):
        guide = (ROOT / "site" / "get-started" / "index.html").read_text(
            encoding="utf-8",
        )
        self.assertIn("there is currently no PyPI distribution", guide)
        self.assertIn("releases/download/v0.2.2", guide)
        self.assertIn("demo-events/events.db", guide)
        self.assertIn('"schema_version":1', guide)

    def test_pages_workflow_publishes_site_artifact(self):
        workflow = (ROOT / ".github" / "workflows" / "pages.yml").read_text(
            encoding="utf-8",
        )
        self.assertIn("actions/upload-pages-artifact@v4", workflow)
        self.assertIn("actions/deploy-pages@v5", workflow)
        self.assertIn("path: site", workflow)
