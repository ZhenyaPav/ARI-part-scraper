from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import logging
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from playwright.async_api import BrowserContext, Frame, Page


START_URL = "https://www.genuinefactoryparts.com/en_US/ari-partstream.html"
BASE_PATH = [
    "MTD Merged Data Staging",
    "Troy-Bilt",
    "11-Push Walk-Behind Mowers",
]
CSV_FIELDS = [
    "unique_key",
    "full_scheme_path",
    "year",
    "model",
    "assembly",
    "scheme",
    "oem",
    "description",
    "scraped_at",
]
MODEL_RE = re.compile(r"\b(?:[0-9]{2}[A-Z0-9-]{3,}|[A-Z]{2}[0-9A-Z-]{3,})\b")
OEM_RE = re.compile(r"\b[0-9A-Z][0-9A-Z.-]{3,}[0-9A-Z]\b")


@dataclass(frozen=True)
class PartRecord:
    full_scheme_path: str
    year: str
    model: str
    assembly: str
    scheme: str
    oem: str
    description: str
    scraped_at: str

    @property
    def unique_key(self) -> str:
        raw = f"{self.full_scheme_path}|{self.oem}|{self.description}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def as_row(self) -> dict[str, str]:
        return {
            "unique_key": self.unique_key,
            "full_scheme_path": self.full_scheme_path,
            "year": self.year,
            "model": self.model,
            "assembly": self.assembly,
            "scheme": self.scheme,
            "oem": self.oem,
            "description": self.description,
            "scraped_at": self.scraped_at,
        }


@dataclass
class UpsertStats:
    collected: int = 0
    new: int = 0
    updated: int = 0


@dataclass
class ScrapeStats:
    collected: int = 0
    errors: int = 0


@dataclass(frozen=True)
class SchemeJob:
    year: str
    model: str
    scheme: str
    context_path: list[str]
    scheme_path: list[str]


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip()


def normalize_scheme_label(value: str) -> str:
    return normalize_text(value).lstrip(". ")


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def parse_bool(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "y", "headless"}:
        return True
    if normalized in {"0", "false", "no", "n", "headed"}:
        return False
    raise argparse.ArgumentTypeError("Expected true/false, yes/no, or 1/0")


def setup_logging(log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers.clear()

    formatter = logging.Formatter(
        fmt="%(asctime)s %(levelname)s %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    root.addHandler(console)
    root.addHandler(file_handler)


def read_existing_rows(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        return {}
    with path.open("r", newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        return {
            row["unique_key"]: {field: row.get(field, "") for field in CSV_FIELDS}
            for row in reader
            if row.get("unique_key")
        }


def business_row(row: dict[str, str]) -> dict[str, str]:
    return {field: row.get(field, "") for field in CSV_FIELDS if field != "scraped_at"}


def upsert_csv(path: Path, records: Iterable[PartRecord]) -> UpsertStats:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = read_existing_rows(path)
    stats = UpsertStats()

    for record in records:
        stats.collected += 1
        row = record.as_row()
        current = rows.get(row["unique_key"])
        if current is None:
            rows[row["unique_key"]] = row
            stats.new += 1
            continue
        if business_row(current) != business_row(row):
            rows[row["unique_key"]] = row
            stats.updated += 1

    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(sorted(rows.values(), key=lambda item: item["unique_key"]))

    return stats


class PartStreamScraper:
    def __init__(
        self,
        years: Sequence[str],
        headless: bool,
        artifacts_dir: Path,
        slow_mo_ms: int = 0,
        concurrency: int = 1,
    ) -> None:
        self.years = [str(year) for year in years]
        self.headless = headless
        self.artifacts_dir = artifacts_dir
        self.slow_mo_ms = slow_mo_ms
        self.concurrency = max(1, concurrency)
        self.scraped_at = utc_now()
        self.stats = ScrapeStats()

    async def scrape(self) -> list[PartRecord]:
        from playwright.async_api import async_playwright

        async with async_playwright() as playwright:
            browser = await playwright.chromium.launch(
                headless=self.headless,
                slow_mo=self.slow_mo_ms,
            )
            try:
                context = await browser.new_context(
                    viewport={"width": 1440, "height": 1100},
                    locale="en-US",
                )
                page = await context.new_page()
                try:
                    jobs = await self._discover_scheme_jobs(page)
                finally:
                    await page.close()

                logging.info(
                    "Discovered %s scheme jobs; scraping with concurrency=%s",
                    len(jobs),
                    min(self.concurrency, len(jobs)) if jobs else 0,
                )
                records = await self._scrape_jobs(context, jobs)
                self.stats.collected = len(records)
                return records
            finally:
                await browser.close()

    async def _open_start_page(self, page: Page) -> None:
        logging.info("Opening %s", START_URL)
        await retry(lambda: page.goto(START_URL, wait_until="domcontentloaded", timeout=60000))
        await page.wait_for_load_state("networkidle", timeout=30000)
        await self._dismiss_overlays(page)

    async def _discover_scheme_jobs(self, page: Page) -> list[SchemeJob]:
        jobs: list[SchemeJob] = []
        for year in self.years:
            year_label = f"{year} Models"
            context_path = [*BASE_PATH, year_label]
            try:
                await self._open_start_page(page)
                await self._navigate_path(page, context_path)
                models = await self._visible_model_labels(page, year)
                logging.info("Found %s candidate models for %s", len(models), year_label)
            except Exception as exc:
                await self._record_error(page, "navigation", context_path, exc)
                continue

            for model in models:
                model_path = [*context_path, model, f"Assemblies for {model}"]
                try:
                    await self._open_start_page(page)
                    await self._navigate_path(page, [*context_path, model])
                    await self._open_assemblies(page)
                    schemes = await self._visible_scheme_labels(page, excluded={model})
                    logging.info("Found %s schemes for %s", len(schemes), model)
                except Exception as exc:
                    await self._record_error(page, "model", model_path, exc)
                    continue

                for scheme in schemes:
                    scheme_path = [*model_path, scheme]
                    jobs.append(
                        SchemeJob(
                            year=year,
                            model=model,
                            scheme=scheme,
                            context_path=context_path,
                            scheme_path=scheme_path,
                        )
                    )

        return jobs

    async def _scrape_jobs(
        self,
        context: BrowserContext,
        jobs: Sequence[SchemeJob],
    ) -> list[PartRecord]:
        if not jobs:
            return []

        queue: asyncio.Queue[SchemeJob] = asyncio.Queue()
        for job in jobs:
            queue.put_nowait(job)

        records: list[PartRecord] = []
        worker_count = min(self.concurrency, len(jobs))

        async def worker(worker_id: int) -> None:
            page = await context.new_page()
            try:
                while True:
                    try:
                        job = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return

                    try:
                        job_records = await self._scrape_scheme_job(page, job)
                        records.extend(job_records)
                    except Exception as exc:
                        await self._record_error(
                            page,
                            f"scheme_worker_{worker_id}",
                            job.scheme_path,
                            exc,
                        )
                    finally:
                        queue.task_done()
            finally:
                await page.close()

        await asyncio.gather(*(worker(worker_id) for worker_id in range(1, worker_count + 1)))
        return records

    async def _scrape_scheme_job(self, page: Page, job: SchemeJob) -> list[PartRecord]:
        await self._open_start_page(page)
        await self._navigate_path(page, [*job.context_path, job.model])
        await self._open_assemblies(page)
        await self._click_best_text(page, job.scheme)
        records = await self._extract_parts(
            page,
            job.year,
            job.model,
            "Assemblies",
            job.scheme,
            job.scheme_path,
        )
        logging.info(
            "Collected %s parts from %s",
            len(records),
            " - ".join(job.scheme_path),
        )
        return records

    async def _navigate_path(self, page: Page, labels: Sequence[str]) -> None:
        await self._dismiss_overlays(page)
        for label in labels:
            await self._click_best_text(page, label)

    async def _open_assemblies(self, page: Page) -> None:
        if await self._assemblies_visible(page):
            return
        await self._click_best_text(page, "Assemblies", partial=True)
        if await self._assemblies_visible(page):
            return
        raise RuntimeError("Assemblies list did not become visible")

    async def _assemblies_visible(self, page: Page) -> bool:
        for frame in await self._catalog_frames(page):
            try:
                await frame.locator("#ari_assemblies *").first.wait_for(
                    state="visible",
                    timeout=3000,
                )
                return True
            except Exception:
                continue
        return False

    async def _click_best_text(self, page: Page, text: str, partial: bool = False) -> None:
        async def click_once() -> None:
            await self._dismiss_overlays(page)
            last_error: Exception | None = None
            for frame in await self._catalog_frames(page):
                scope = frame.locator("#ariPartStream").first
                if await scope.count() == 0:
                    scope = frame.locator("body").first
                candidates = [
                    scope.locator(f".brandLogoBox:has-text('{css_text(text)}')").first,
                    scope.locator(f"[title='{css_text(text)}']").first,
                    scope.locator(f"[value='{css_text(text)}']").first,
                    scope.get_by_text(text, exact=not partial).first,
                    scope.locator(f"[role='treeitem']:has-text('{css_text(text)}')").first,
                    scope.locator(f"li:has-text('{css_text(text)}')").first,
                    scope.locator(f"p:has-text('{css_text(text)}')").first,
                    scope.locator(f"a:has-text('{css_text(text)}')").first,
                    scope.locator(f"button:has-text('{css_text(text)}')").first,
                ]
                for locator in candidates:
                    try:
                        if await locator.count() == 0:
                            continue
                        if not await locator.is_visible(timeout=1000):
                            continue
                        await locator.scroll_into_view_if_needed(timeout=5000)
                        await locator.click(timeout=8000)
                        try:
                            await frame.page.wait_for_load_state("networkidle", timeout=15000)
                        except Exception:
                            pass
                        await frame.page.wait_for_timeout(750)
                        if not page.url.startswith(START_URL):
                            raise RuntimeError(
                                f"Unexpected navigation away from PartStream: {page.url}"
                            )
                        return
                    except Exception as exc:
                        if "onetrust" in str(exc).lower():
                            await self._dismiss_overlays(page)
                        last_error = exc
            raise RuntimeError(f"Could not click visible text {text!r}") from last_error

        await retry(click_once)

    async def _visible_model_labels(self, page: Page, year: str) -> list[str]:
        labels = await self._visible_labels(page)
        models: list[str] = []
        for label in labels:
            if year in label and MODEL_RE.search(label) and "Assemblies" not in label:
                models.append(label)
        if not models:
            for label in labels:
                if MODEL_RE.search(label) and "Models" not in label and "Assemblies" not in label:
                    models.append(label)
        return dedupe(models)

    async def _visible_scheme_labels(self, page: Page, excluded: set[str]) -> list[str]:
        labels: list[str] = []
        for frame in await self._catalog_frames(page):
            locator = frame.locator("#ari_assemblies *")
            try:
                count = min(await locator.count(), 1000)
                for index in range(count):
                    item = locator.nth(index)
                    if not await item.is_visible(timeout=500):
                        continue
                    label = normalize_scheme_label(await item.inner_text(timeout=1000))
                    if label:
                        labels.append(label)
            except Exception:
                continue

        blocked = {
            "Home",
            "Back",
            "Close",
            "Search",
            "Login",
            "Cart",
            "Assemblies",
            "New Search",
            "Change Brand",
            "Quick Search:",
            "Browse Catalog:",
            *excluded,
            *BASE_PATH,
        }
        schemes = [
            label
            for label in labels
            if label not in blocked
            and len(label) <= 100
            and "\n" not in label
            and not label.endswith("Models")
            and not label.startswith("Assemblies for ")
            and not MODEL_RE.fullmatch(label)
            and "Parts Diagram" not in label
            and "Shop by" not in label
        ]
        return dedupe(schemes)

    async def _visible_labels(self, page: Page) -> list[str]:
        selectors = [
            "[role='treeitem']",
            "[role='option']",
            "a",
            "button",
            "li",
            "td",
            ".partstream *",
            "[class*='part']",
            "[class*='diagram']",
            "[class*='assembly']",
        ]
        labels: list[str] = []
        for frame in await self._catalog_frames(page):
            scope = frame.locator("#ariPartStream").first
            if await scope.count() == 0:
                scope = frame.locator("body").first
            for selector in selectors:
                locator = scope.locator(selector)
                try:
                    count = min(await locator.count(), 300)
                    for index in range(count):
                        item = locator.nth(index)
                        if not await item.is_visible(timeout=500):
                            continue
                        if await self._is_ignored_catalog_element(item):
                            continue
                        text = normalize_text(await item.inner_text(timeout=1000))
                        if 2 <= len(text) <= 180:
                            labels.append(text)
                except Exception:
                    continue
        return dedupe(labels)

    async def _is_ignored_catalog_element(self, locator) -> bool:
        try:
            return await locator.evaluate(
                """element => Boolean(element.closest(
                    '#ari-breadCrumb, .ari-breadCrumbItem, #navHeader, #ari-searchBox'
                ))"""
            )
        except Exception:
            return False

    async def _catalog_frames(self, page: Page) -> list[Frame]:
        frames: list[Frame] = []
        for frame in self._frames(page):
            try:
                if (
                    await frame.locator("#ariPartStream").count()
                    or await frame.locator("#ari-container").count()
                    or "arinet.com" in frame.url
                ):
                    frames.append(frame)
            except Exception:
                continue
        return frames or self._frames(page)

    async def _extract_parts(
        self,
        page: Page,
        year: str,
        model: str,
        assembly: str,
        scheme: str,
        scheme_path: Sequence[str],
    ) -> list[PartRecord]:
        full_scheme_path = " - ".join(scheme_path)
        rows = await self._extract_ari_parts(page)
        if not rows:
            rows = await self._extract_table_parts(page)
        if not rows:
            rows = await self._extract_text_parts(page)

        return [
            PartRecord(
                full_scheme_path=full_scheme_path,
                year=year,
                model=model,
                assembly=assembly,
                scheme=scheme,
                oem=oem,
                description=description,
                scraped_at=self.scraped_at,
            )
            for oem, description in rows
        ]

    async def _extract_ari_parts(self, page: Page) -> list[tuple[str, str]]:
        parts: list[tuple[str, str]] = []
        for frame in await self._catalog_frames(page):
            row_locator = frame.locator("#ariPartList .ariPartInfo")
            try:
                await row_locator.first.wait_for(state="visible", timeout=5000)
            except Exception:
                continue

            count = min(await row_locator.count(), 1000)
            for index in range(count):
                row = row_locator.nth(index)
                try:
                    oem = normalize_text(
                        await row.locator(".ariPartNumber").first.inner_text(timeout=1000)
                    )
                    description = normalize_text(
                        await row.locator(".ariPLDesc").first.inner_text(timeout=1000)
                    )
                except Exception:
                    continue
                if oem and description:
                    parts.append((oem, description))
        return dedupe_pairs(parts)

    async def _extract_table_parts(self, page: Page) -> list[tuple[str, str]]:
        parts: list[tuple[str, str]] = []
        for frame in await self._catalog_frames(page):
            row_locator = frame.locator("table tr, [role='row']")
            try:
                count = min(await row_locator.count(), 1000)
                for index in range(count):
                    text = normalize_text(await row_locator.nth(index).inner_text(timeout=1000))
                    parsed = parse_part_line(text)
                    if parsed:
                        parts.append(parsed)
            except Exception:
                continue
        return dedupe_pairs(parts)

    async def _extract_text_parts(self, page: Page) -> list[tuple[str, str]]:
        parts: list[tuple[str, str]] = []
        for frame in await self._catalog_frames(page):
            try:
                scope = frame.locator("#ariPartStream").first
                if await scope.count() == 0:
                    scope = frame.locator("body").first
                body_text = await scope.inner_text(timeout=3000)
            except Exception:
                continue
            for line in body_text.splitlines():
                parsed = parse_part_line(normalize_text(line))
                if parsed:
                    parts.append(parsed)
        return dedupe_pairs(parts)

    async def _dismiss_overlays(self, page: Page) -> None:
        selectors = [
            "#onetrust-reject-all-handler",
            "#onetrust-accept-btn-handler",
            "#accept-recommended-btn-handler",
            ".onetrust-close-btn-handler",
            "#close-pc-btn-handler",
        ]
        texts = [
            "Reject All But Necessary",
            "Accept All Cookies",
            "Allow All",
            "Confirm My Choices",
            "Accept",
            "Accept All",
            "No",
            "Close",
        ]

        for selector in selectors:
            for frame in self._frames(page):
                locator = frame.locator(selector).first
                try:
                    if await locator.count() and await locator.is_visible(timeout=1500):
                        await locator.click(timeout=3000, force=True)
                        await page.wait_for_timeout(500)
                        return
                except Exception:
                    pass

        for text in texts:
            for frame in self._frames(page):
                locator = frame.get_by_text(text, exact=True).first
                try:
                    if await locator.count() and await locator.is_visible(timeout=1000):
                        await locator.click(timeout=2000, force=True)
                        await page.wait_for_timeout(500)
                        return
                except Exception:
                    pass

    async def _record_error(
        self,
        page: Page,
        stage: str,
        path: Sequence[str],
        exc: Exception,
    ) -> None:
        self.stats.errors += 1
        safe_name = re.sub(r"[^A-Za-z0-9_.-]+", "_", "_".join(path))[:160]
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        screenshot = self.artifacts_dir / f"{stage}_{safe_name}.png"
        html = self.artifacts_dir / f"{stage}_{safe_name}.html"
        logging.exception("Failed at %s: %s", " - ".join(path), exc)
        try:
            await page.screenshot(path=str(screenshot), full_page=True)
            html.write_text(await page.content(), encoding="utf-8")
            logging.info("Saved debug artifacts: %s and %s", screenshot, html)
        except Exception as artifact_exc:
            logging.warning("Could not save debug artifacts: %s", artifact_exc)

    def _frames(self, page: Page) -> list[Frame]:
        return [page.main_frame, *[frame for frame in page.frames if frame is not page.main_frame]]


def css_text(text: str) -> str:
    return text.replace("\\", "\\\\").replace("'", "\\'")


def parse_part_line(line: str) -> tuple[str, str] | None:
    if not line or len(line) < 6:
        return None
    if any(word in line.lower() for word in ["description", "part number", "price", "subtotal"]):
        return None
    match = OEM_RE.search(line)
    if not match:
        return None
    oem = match.group(0)
    description = normalize_text(line[match.end() :].strip(" -:\t|"))
    if not description:
        description = normalize_text(line[: match.start()].strip(" -:\t|"))
    if len(description) < 2 or description == oem:
        return None
    return oem, description


def dedupe(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        normalized = normalize_text(value)
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


def dedupe_pairs(values: Iterable[tuple[str, str]]) -> list[tuple[str, str]]:
    seen: set[tuple[str, str]] = set()
    result: list[tuple[str, str]] = []
    for oem, description in values:
        pair = (normalize_text(oem), normalize_text(description))
        if pair not in seen:
            seen.add(pair)
            result.append(pair)
    return result


async def retry(action, attempts: int = 3, delay_seconds: float = 2.0):
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return await action()
        except Exception as exc:
            last_error = exc
            if attempt == attempts:
                break
            logging.warning("Attempt %s/%s failed: %s", attempt, attempts, exc)
            await asyncio.sleep(delay_seconds * attempt)
    raise RuntimeError("All retry attempts failed") from last_error


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape GenuineFactoryParts PartStream diagrams into CSV."
    )
    parser.add_argument("--output", type=Path, default=Path("data/parts.csv"))
    parser.add_argument("--years", nargs="+", default=["2024", "2025", "2026"])
    parser.add_argument("--headless", type=parse_bool, default=True)
    parser.add_argument("--log-file", type=Path, default=Path("logs/run.log"))
    parser.add_argument("--artifacts-dir", type=Path, default=Path("artifacts/errors"))
    parser.add_argument(
        "--slow-mo-ms",
        type=int,
        default=0,
        help="Slow Playwright actions for headed debugging.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=1,
        help="Number of independent browser pages to use for scheme scraping.",
    )
    return parser.parse_args()


async def main_async() -> int:
    args = parse_args()
    setup_logging(args.log_file)
    started_at = utc_now()
    logging.info("Run started at %s", started_at)
    logging.info("Selected years: %s", ", ".join(args.years))
    logging.info("Scrape concurrency: %s", args.concurrency)

    scraper = PartStreamScraper(
        years=args.years,
        headless=args.headless,
        artifacts_dir=args.artifacts_dir,
        slow_mo_ms=args.slow_mo_ms,
        concurrency=args.concurrency,
    )
    records = await scraper.scrape()
    upsert_stats = upsert_csv(args.output, records)

    finished_at = utc_now()
    logging.info("Run finished at %s", finished_at)
    logging.info(
        "Collected=%s New=%s Updated=%s Errors=%s Output=%s",
        upsert_stats.collected,
        upsert_stats.new,
        upsert_stats.updated,
        scraper.stats.errors,
        args.output,
    )
    return 0 if scraper.stats.errors == 0 else 2


def main() -> int:
    return asyncio.run(main_async())


if __name__ == "__main__":
    raise SystemExit(main())
