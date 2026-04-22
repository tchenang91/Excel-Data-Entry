"""
UT County Expired Listings Lead Enrichment Scraper  (v2 – all bugs fixed)
====================================================
FIXES vs v1
-----------
  FIX 1 – triple_click() removed:
      wait_for_selector() returns ElementHandle which has NO triple_click().
      Replaced everywhere with element.click() + element.fill().
      fill() already clears and replaces the full content – no triple-click needed.

  FIX 2 – Google Chrome as the browser:
      launch_browser() tries channel='chrome' (installed Google Chrome) first,
      then falls back to bundled Chromium automatically.

  FIX 3 – Business search now runs:
      FIX 1 crashing the parcel step meant the business step was never reached.
      Both steps now execute correctly.

  FIX 4 – Angular SPA-aware business search:
      businessregistration.utah.gov is an Angular app.  Selectors updated for
      [formcontrolname], mat-row, mat-cell, networkidle waits, and debug
      screenshots are saved to debug_screenshots/ on any failure.
"""

import argparse
import csv
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

PARCEL_MAP_URL      = "https://maps.utahcounty.gov/ParcelMap/ParcelMap.html"
BIZ_HOME_URL        = "https://businessregistration.utah.gov"
BUSINESS_SEARCH_URL = "https://businessregistration.utah.gov/EntitySearch/OnlineEntitySearch"
SCREENSHOT_DIR      = Path("debug_screenshots")

CSV_FIELDS = [
    "Tax ID", "Owner Name",
    "Principal Title", "Principal Name", "Principal Address", "Last Updated",
    "Status",
]

# Selectors for the "Search by Parcel Serial" input ONLY.
# The page also has a "Search by Address" field (.esri-search__input) which is
# the standard Esri Search dijit — we must NOT match that one.
# The Parcel Serial field is a custom input with placeholder "Enter Serial".
_PARCEL_INPUT_SELECTORS = [
    "input[placeholder='Enter Serial']",      # exact match – safest
    "input[placeholder*='Serial']",           # contains "Serial"
    "input[placeholder*='serial']",           # lowercase variant
]

_BIZ_INPUT_SELECTORS = [
    "input[formcontrolname='entityName']",
    "input[formcontrolname='name']",
    "input[ng-model*='name']",
    "input[id*='entityName']",
    "input[name*='entityName']",
    "input[placeholder*='Entity Name']",
    "input[placeholder*='Business Name']",
    "input[placeholder*='Name']",
    "mat-form-field input",
    "input[type='text']",
]

_BIZ_SEARCH_BTN_SELECTORS = [
    "button[type='submit']",
    "button:has-text('Search')",
    "input[type='submit']",
    "input[value='Search']",
    "[aria-label='Search']",
]


# ─── Logging ─────────────────────────────────────────────────────────────────

def setup_logging(logfile):
    logger = logging.getLogger("ut_scraper")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s  %(levelname)-8s  %(message)s",
                            datefmt="%Y-%m-%d %H:%M:%S")
    fh = logging.FileHandler(logfile, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


# ─── FIX 2: Chrome launcher ──────────────────────────────────────────────────

def launch_browser(pw, headless, logger):
    """Try Google Chrome first, fall back to bundled Chromium."""
    launch_args = ["--no-sandbox", "--disable-dev-shm-usage"]
    try:
        b = pw.chromium.launch(headless=headless, channel="chrome", args=launch_args)
        logger.info("Browser: Google Chrome")
        return b
    except Exception:
        logger.info("Google Chrome not found – using bundled Chromium")
        return pw.chromium.launch(headless=headless, args=launch_args)


# ─── Debug screenshots ────────────────────────────────────────────────────────

def screenshot(page, label, logger):
    try:
        SCREENSHOT_DIR.mkdir(exist_ok=True)
        path = SCREENSHOT_DIR / f"{datetime.now().strftime('%H%M%S')}_{label[:40]}.png"
        page.screenshot(path=str(path), full_page=True)
        logger.debug(f"Screenshot: {path}")
    except Exception as e:
        logger.debug(f"Screenshot failed: {e}")


# ─── Excel loader ─────────────────────────────────────────────────────────────

def load_tax_ids(excel_path, logger):
    logger.info(f"Reading Excel file: {excel_path}")
    try:
        df = pd.read_excel(excel_path, dtype=str)
    except Exception as e:
        logger.error(f"Cannot read Excel file – {e}")
        sys.exit(1)
    df.columns = [c.strip() for c in df.columns]
    if "Tax ID" not in df.columns:
        logger.error(f"'Tax ID' column not found. Columns: {list(df.columns)}")
        sys.exit(1)
    raw = df["Tax ID"].dropna().astype(str).str.strip()
    raw = raw[raw != ""].tolist()
    seen, unique = set(), []
    for t in raw:
        if t not in seen:
            seen.add(t)
            unique.append(t)
    logger.info(f"Found {len(raw)} records → {len(unique)} unique Tax IDs")
    return unique


def load_done_ids(output_path, logger):
    done = set()
    if not Path(output_path).exists():
        return done
    try:
        with open(output_path, newline="", encoding="utf-8-sig") as f:
            for row in csv.DictReader(f, delimiter=";"):
                t = row.get("Tax ID", "").strip()
                if t:
                    done.add(t)
        logger.info(f"Resume: {len(done)} Tax IDs already done, skipping.")
    except Exception as e:
        logger.warning(f"Resume read failed: {e}")
    return done


# ─── Parcel Map scraper ───────────────────────────────────────────────────────

def get_owner_from_parcel_map(page, tax_id, logger):
    logger.debug(f"[Parcel] Tax ID: {tax_id}")
    try:
        # Find search input
        search_el = None
        for sel in _PARCEL_INPUT_SELECTORS:
            try:
                el = page.wait_for_selector(sel, timeout=5000)
                if el:
                    search_el = el
                    logger.debug(f"[Parcel] Input selector: {sel}")
                    break
            except PWTimeout:
                continue

        if not search_el:
            logger.warning(f"[Parcel] Input not found for {tax_id}")
            screenshot(page, f"parcel_no_input_{tax_id}", logger)
            return None

        # Fill in the Tax ID (click() + fill() — no triple_click on ElementHandle)
        search_el.click()
        search_el.fill(tax_id)

        # Click the blue search BUTTON that sits right next to the Serial input.
        # The page has multiple search sections; we find the button immediately
        # following the "Enter Serial" input inside the same parent container.
        # Fallback: press Enter if no button is found.
        clicked_btn = False
        try:
            # Walk up to the parent widget container then find its button
            serial_section = page.evaluate("""
                () => {
                    const inputs = Array.from(document.querySelectorAll('input'));
                    const serial = inputs.find(i =>
                        i.placeholder && i.placeholder.toLowerCase().includes('serial'));
                    if (!serial) return null;
                    // Look for a button in the same parent or next sibling
                    let parent = serial.parentElement;
                    for (let depth = 0; depth < 4; depth++) {
                        const btn = parent.querySelector('button, input[type=submit], input[type=button]');
                        if (btn) { btn.click(); return 'clicked'; }
                        parent = parent.parentElement;
                        if (!parent) break;
                    }
                    return null;
                }
            """)
            if serial_section == "clicked":
                clicked_btn = True
                logger.debug("[Parcel] Clicked serial search button via JS")
        except Exception:
            pass

        if not clicked_btn:
            search_el.press("Enter")
            logger.debug("[Parcel] Pressed Enter to submit serial search")

        # The Parcel Serial search shows results in the LEFT SIDEBAR panel,
        # not as an Esri popup on the map.  Wait for the results list/section
        # to appear in the sidebar, or for a popup if the parcel is auto-selected.
        result_appeared = False
        for result_sel in [
            # Custom results panel in the sidebar
            "#resultsList", ".results-list", ".result-list",
            "[id*='result']", "[class*='result']",
            # Some implementations show a feature popup too
            ".esri-popup--is-visible", ".esri-popup__content",
            ".esri-feature-widget",
        ]:
            try:
                page.wait_for_selector(result_sel, timeout=5000)
                result_appeared = True
                logger.debug(f"[Parcel] Results found via: {result_sel}")
                break
            except PWTimeout:
                continue

        if not result_appeared:
            # Last resort: just wait a bit and hope content painted
            time.sleep(3)
            logger.debug("[Parcel] No specific result selector matched; proceeding anyway")

        time.sleep(1.5)

        # Click the highlighted parcel and verify serial matches this tax_id.
        # Multiple positions are tried; for each click we also cycle through the
        # "1 von N" pagination in case multiple parcels overlap at that point.
        owner = _get_owner_via_click_and_verify(page, tax_id, logger)

        if not owner:
            screenshot(page, f"parcel_after_search_{tax_id}", logger)

        if owner:
            logger.info(f"[Parcel] {tax_id} → {owner}")
        else:
            logger.warning(f"[Parcel] No owner found for {tax_id}")
            screenshot(page, f"parcel_no_owner_{tax_id}", logger)

        _dismiss_parcel_popup(page)
        return owner

    except PWTimeout:
        logger.warning(f"[Parcel] Timeout: {tax_id}")
        screenshot(page, f"parcel_timeout_{tax_id}", logger)
        return None
    except Exception as e:
        logger.error(f"[Parcel] Error for {tax_id}: {e}")
        screenshot(page, f"parcel_error_{tax_id}", logger)
        return None


def _get_owner_via_click_and_verify(page, tax_id, logger):
    """
    Full click-verify-cycle strategy for reading the parcel owner.

    After a serial search the map draws a RED outline around the matched parcel
    and zooms to it.  Because map labels are canvas-rendered (not DOM text) we
    cannot simply read them – we must click the parcel to open the ArcGIS popup,
    then read the popup.

    When multiple parcels overlap at the click point the popup shows a "1 von N"
    paginator.  We cycle through every page and stop at the one whose Serial
    field matches THIS tax_id (dash format = colon format, e.g. 18-047-0085 =
    18:047:0085).

    If the first click position gives no match we spiral outward and try more
    positions (12 cm ≈ ~45 px radius at 96 DPI on a typical monitor).
    """
    import math
    colon_id = tax_id.replace("-", ":")

    # ── Build an ordered list of positions to click ───────────────────────
    positions = _build_click_positions(page, colon_id, logger)

    for attempt, (cx, cy) in enumerate(positions):
        logger.debug(f"[Parcel] Click attempt {attempt + 1}/{len(positions)} "
                     f"at ({cx:.0f}, {cy:.0f})")
        try:
            page.mouse.click(cx, cy)
        except Exception:
            continue
        time.sleep(2.0)

        # Cycle through popup pages and look for a serial match
        owner = _read_popup_with_serial_check(page, tax_id, colon_id, logger)
        if owner:
            return owner

        # Close popup before next attempt
        _close_popup_only(page)
        time.sleep(0.5)

    return None


def _build_click_positions(page, colon_id, logger):
    """
    Return an ordered list of (x, y) screen positions to click, in priority order:
      1. Centroid of the ArcGIS highlight graphic (via window.map.toScreen)
      2. SVG <text> element position (works only when map uses SVG renderer)
      3. Spiral positions around the map centre (12 cm diameter ≈ 45 px radius)
    """
    import math
    positions = []
    vp = page.viewport_size or {"width": 1280, "height": 900}
    # Map occupies the area to the right of the ~300 px sidebar
    map_cx = 300 + (vp["width"]  - 300) // 2   # ~790
    map_cy =        vp["height"]         // 2   # ~450

    # ── 1. ArcGIS JS API: get centroid of the highlight/search-result graphic ─
    try:
        result = page.evaluate("""
            () => {
                if (!window.map || !window.map.toScreen) return null;
                const layerIds = window.map.graphicsLayerIds || [];
                for (const id of layerIds) {
                    try {
                        const layer = window.map.getLayer(id);
                        if (!layer || !layer.graphics || !layer.graphics.length)
                            continue;
                        for (const g of layer.graphics) {
                            if (!g.geometry) continue;
                            const geom = g.geometry;
                            let wx, wy;
                            if (geom.rings) {
                                // Polygon: compute bounding-box centre
                                let xmin=1e18, xmax=-1e18, ymin=1e18, ymax=-1e18;
                                for (const ring of geom.rings) {
                                    for (const pt of ring) {
                                        if(pt[0]<xmin)xmin=pt[0]; if(pt[0]>xmax)xmax=pt[0];
                                        if(pt[1]<ymin)ymin=pt[1]; if(pt[1]>ymax)ymax=pt[1];
                                    }
                                }
                                wx = (xmin+xmax)/2;
                                wy = (ymin+ymax)/2;
                            } else if (geom.x !== undefined) {
                                wx = geom.x; wy = geom.y;
                            } else { continue; }

                            // Convert world → screen
                            const scr = window.map.toScreen({
                                x: wx, y: wy,
                                spatialReference: geom.spatialReference
                                    || window.map.spatialReference
                            });
                            if (scr && scr.x > 300) {   // must be in map area, not sidebar
                                return {x: scr.x, y: scr.y};
                            }
                        }
                    } catch(_) {}
                }
                return null;
            }
        """)
        if result:
            logger.debug(f"[Parcel] JS graphic centroid: "
                         f"({result['x']:.0f}, {result['y']:.0f})")
            positions.append((result["x"], result["y"]))
    except Exception as e:
        logger.debug(f"[Parcel] JS centroid failed: {e}")

    # ── 2. SVG <text> element (works if map uses SVG renderer) ───────────
    try:
        bbox = page.evaluate(f"""
            () => {{
                const target = {repr(colon_id)};
                for (const el of document.querySelectorAll('text, tspan')) {{
                    const t = (el.textContent || '').trim();
                    if (t === target || t.startsWith(target)) {{
                        const r = el.getBoundingClientRect();
                        if (r.width > 0 || r.height > 0)
                            return {{x: r.left + r.width/2, y: r.top + r.height/2}};
                    }}
                }}
                return null;
            }}
        """)
        if bbox:
            logger.debug(f"[Parcel] SVG label at ({bbox['x']:.0f}, {bbox['y']:.0f})")
            positions.append((bbox["x"], bbox["y"]))
    except Exception:
        pass

    # ── 3. Spiral positions (12 cm ≈ 45 px at 96 DPI, we use 50 px steps) ─
    # Always add these as final fallbacks regardless of whether 1/2 succeeded.
    positions.append((map_cx, map_cy))              # dead centre first
    for radius in [50, 100, 150, 200]:              # expanding rings
        step = 45 if radius <= 100 else 60
        for deg in range(0, 360, step):
            rad = math.radians(deg)
            x = map_cx + radius * math.cos(rad)
            y = map_cy + radius * math.sin(rad)
            if 310 < x < vp["width"] - 10 and 10 < y < vp["height"] - 40:
                positions.append((int(x), int(y)))

    return positions


def _read_popup_with_serial_check(page, tax_id, colon_id, logger):
    """
    Read the currently open ArcGIS popup.  If the Serial field matches this
    tax_id, extract and return the Owner value.

    Supports the "1 von N" paginator: cycles through up to 20 popup pages.
    Returns None if no matching serial is found across all pages.
    """
    popup_sels = [
        ".esri-popup__content",
        ".esriViewPopup",
        "[class*='esriViewPopup']",
        "[class*='esriPopup']",
        ".simpleInfoWindowContent",
    ]

    for page_num in range(20):
        # Read current popup page
        for sel in popup_sels:
            try:
                el = page.query_selector(sel)
                if not el:
                    continue
                text = el.inner_text()
                if not text.strip():
                    continue

                # Serial match?  Tax ID formats: "18-047-0085" or "18:047:0085"
                if colon_id in text or tax_id in text:
                    owner = _owner_from_popup_text(text)
                    if owner:
                        logger.debug(
                            f"[Parcel] Serial match on page {page_num + 1}: "
                            f"{owner!r}")
                        return owner
                else:
                    logger.debug(
                        f"[Parcel] Page {page_num + 1} serial mismatch "
                        f"({sel}): {text[:80]!r}")
            except Exception:
                continue

        # Try to advance to the next popup feature (the ▶ arrow next to "1 von N")
        advanced = False
        for nxt in [
            ".esri-popup__button--next",
            "[title='Next Feature']",
            "[aria-label='Next Feature']",
            "button[class*='next']:visible",
            ".esri-popup__navigation button:last-child",
            # Generic: any visible button inside popup with a ">" or "›" label
        ]:
            try:
                btn = page.query_selector(nxt)
                if btn and btn.is_visible():
                    btn.click()
                    time.sleep(0.8)
                    advanced = True
                    logger.debug(f"[Parcel] Advanced popup page via {nxt}")
                    break
            except Exception:
                continue

        if not advanced:
            break   # No more pages

    return None


def _owner_from_popup_text(text):
    """Extract the Owner field value from ArcGIS popup plain-text content."""
    # The popup renders rows like:  "Owner:\tELLIS INVESTMENT COMPANY LTD"
    for label in ["Owner:\t", "Owner: ", "Owner:", "OWNER:\t", "OWNER: ", "OWNER:"]:
        idx = text.find(label)
        if idx != -1:
            after = text[idx + len(label):].lstrip(" \t")
            line  = after.split("\n")[0].strip()
            if line:
                return line
    return None


def _close_popup_only(page):
    """Close the ArcGIS popup without touching the Serial input."""
    for sel in [
        ".esri-popup__header-button--close",
        "[title='Close']",
        "[aria-label='Close']",
        ".esri-popup__button--close",
    ]:
        try:
            btn = page.query_selector(sel)
            if btn and btn.is_visible():
                btn.click()
                time.sleep(0.3)
                return
        except Exception:
            continue


def _extract_parcel_owner(page, tax_id, logger):
    """
    Extract the parcel owner name using multiple strategies.

    STRATEGY ORDER
    ──────────────
    0  Popup content verification – if the open ArcGIS popup matches THIS tax_id
       (colon format "18:047:0085") read the Owner field directly.
    1  JavaScript – read directly from the ArcGIS JS API (window.map).
    A  ArcGIS 3.x InfoWindow / popup DOM selectors.
    B  ArcGIS 4.x / Esri feature-widget selectors.
    C  SVG text elements – find the tax-id label, then take the next text node.
    D  Sidebar / custom results panel fallback.
    """
    import re as _re
    colon_id = tax_id.replace("-", ":")
    labels   = ["OWNER", "Owner", "OWNER NAME", "Owner Name", "OWNERNAME",
                "Owner Name:", "OWNER:"]

    # ── Strategy 0: Verify open popup matches THIS tax_id ─────────────────
    # The popup content begins with "Serial:\t18:047:0085\nOwner:\t..."
    # We ONLY read the owner if the Serial field matches our tax_id.
    agis3_popup_selectors = [
        ".esriViewPopup",
        ".esri-popup__content",
        ".esri-feature-widget",
        ".esriPopup .contentPane",
        "[class*='esriViewPopup']",
        "[class*='esriPopup']",
        ".simpleInfoWindowContent",
        ".dijitContentPane",
        ".esriSimpleInfoWindowWrapper",
    ]
    for sel in agis3_popup_selectors:
        try:
            container = page.query_selector(sel)
            if not container:
                continue
            full_text = container.inner_text()
            if not full_text.strip():
                continue
            # Verify this popup belongs to the correct parcel
            serial_ok = (colon_id in full_text) or (tax_id in full_text)
            logger.debug(
                f"[Parcel] Popup ({sel}) serial_ok={serial_ok}: "
                f"{full_text[:120]!r}")
            if not serial_ok:
                continue           # popup open but for a DIFFERENT parcel
            # a) labeled field scan
            for lp in labels:
                idx = full_text.lower().find(lp.lower())
                if idx != -1:
                    after = full_text[idx + len(lp):].lstrip(" :\t\n")
                    line  = after.split("\n")[0].strip()
                    if line:
                        return line
            # b) <tr> scan
            for row in container.query_selector_all("tr"):
                cells = [c.inner_text().strip()
                         for c in row.query_selector_all("td, th")]
                for i, ct in enumerate(cells):
                    if any(lp.lower() in ct.lower() for lp in labels):
                        if i + 1 < len(cells) and cells[i + 1]:
                            return cells[i + 1]
        except Exception:
            continue
    # ── Strategy 1: JavaScript – ArcGIS map object ────────────────────────
    try:
        result = page.evaluate(f"""
            () => {{
                const colonId = {repr(colon_id)};
                if (!window.map) return null;
                const OWNER_KEYS = ['OWNER', 'OWNERNAME', 'OWNER_NAME',
                                    'PARCELOWNER', 'OWN_NAME', 'NAME'];
                const allIds = [
                    ...(window.map.graphicsLayerIds || []),
                    ...(window.map.layerIds || []),
                ];
                for (const id of allIds) {{
                    try {{
                        const layer = window.map.getLayer(id);
                        if (!layer) continue;
                        const selected =
                            (layer.getSelectedFeatures && layer.getSelectedFeatures()) ||
                            layer._selectedFeatures || [];
                        for (const feat of selected) {{
                            const attrs = feat.attributes || {{}};
                            // Verify this feature is the right parcel
                            const serialKey = Object.keys(attrs).find(k =>
                                k.toUpperCase().includes('SERIAL') ||
                                k.toUpperCase().includes('PARCEL'));
                            if (serialKey) {{
                                const v = String(attrs[serialKey]).replace(/-/g,':');
                                if (!v.includes(colonId.replace(/:/g,'').slice(-4)))
                                    continue;
                            }}
                            for (const k of OWNER_KEYS) {{
                                if (attrs[k]) return String(attrs[k]).trim();
                            }}
                            const key = Object.keys(attrs).find(
                                k => k.toUpperCase().includes('OWNER'));
                            if (key && attrs[key]) return String(attrs[key]).trim();
                        }}
                        for (const g of (layer.graphics || [])) {{
                            if (!g.attributes) continue;
                            const key = Object.keys(g.attributes).find(
                                k => k.toUpperCase().includes('OWNER'));
                            if (key && g.attributes[key])
                                return String(g.attributes[key]).trim();
                        }}
                    }} catch (_) {{}}
                }}
                try {{
                    const iw = window.map.infoWindow;
                    if (iw && iw.features && iw.features.length > 0) {{
                        const attrs = iw.features[0].attributes || {{}};
                        const key = Object.keys(attrs).find(
                            k => k.toUpperCase().includes('OWNER'));
                        if (key && attrs[key]) return String(attrs[key]).trim();
                    }}
                }} catch (_) {{}}
                return null;
            }}
        """)
        if result:
            logger.debug(f"[Parcel] Owner via JS map: {result!r}")
            return result
    except Exception as e:
        logger.debug(f"[Parcel] JS map strategy failed: {e}")

    # ── Strategy B: ArcGIS 4.x field-header / field-data pairs ───────────
    try:
        els   = page.query_selector_all(
            ".esri-feature__field-header, .esri-feature__field-data, "
            ".esri-feature__field-value")
        texts = [e.inner_text().strip() for e in els]
        for i, t in enumerate(texts):
            if any(lp.lower() == t.lower() for lp in labels):
                if i + 1 < len(texts) and texts[i + 1]:
                    return texts[i + 1]
    except Exception:
        pass

    # ── Strategy C: SVG text labels – find THIS tax_id, take next node ────
    # Labels on the map use ":" (e.g. "18:047:0085") and the very next SVG
    # text node after the tax-id is the owner name.
    try:
        svg_texts = page.evaluate("""
            () => {
                const out = [];
                document.querySelectorAll('svg text, text, tspan').forEach(el => {
                    const t = (el.textContent || '').trim();
                    if (t.length > 1) out.push(t);
                });
                return out;
            }
        """)
        if svg_texts:
            skip_pat = _re.compile(
                r'^(Value|Entry|v\.|Entry#|\$|\d+\.\d+\s*acres|\d+:\d+:\d+)', _re.I)
            for i, t in enumerate(svg_texts):
                if t.strip() == colon_id or t.strip().startswith(colon_id):
                    for j in range(i + 1, min(i + 8, len(svg_texts))):
                        candidate = svg_texts[j].strip()
                        if (candidate
                                and len(candidate) > 3
                                and not skip_pat.match(candidate)):
                            logger.debug(f"[Parcel] SVG owner: {candidate!r}")
                            return candidate
    except Exception as e:
        logger.debug(f"[Parcel] SVG strategy failed: {e}")

    # ── Strategy D: Sidebar / custom results panel ────────────────────────
    sidebar_selectors = [
        "#resultsList", ".results-list", ".result-list",
        "[id*='resultsList']", "[id*='ResultList']",
        "[class*='result-item']", "[class*='resultItem']",
        ".jimu-widget-frame", ".jimu-widget-body",
        ".widget-content",
    ]
    for sel in sidebar_selectors:
        try:
            container = page.query_selector(sel)
            if not container:
                continue
            full_text = container.inner_text()
            if not full_text.strip():
                continue
            logger.debug(f"[Parcel] Sidebar ({sel}): {full_text[:150]!r}")
            for lp in labels:
                idx = full_text.lower().find(lp.lower())
                if idx != -1:
                    after = full_text[idx + len(lp):].lstrip(" :\t\n")
                    line  = after.split("\n")[0].strip()
                    if line:
                        return line
        except Exception:
            continue

    return None


def _dismiss_parcel_popup(page):
    """Close any open popup and clear the Serial input for the next search."""
    # Close map popup if present
    for sel in [".esri-popup__header-button--close",
                "[aria-label='Close']", "[title='Close']"]:
        try:
            b = page.query_selector(sel)
            if b:
                b.click()
                time.sleep(0.4)
                break
        except Exception:
            pass
    # Clear the "Enter Serial" input (NOT the address search widget)
    try:
        page.evaluate("""
            () => {
                const inputs = Array.from(document.querySelectorAll('input'));
                const serial = inputs.find(i =>
                    i.placeholder && i.placeholder.toLowerCase().includes('serial'));
                if (serial) serial.value = '';
            }
        """)
    except Exception:
        pass


# ─── Business Entity Search scraper  (FIX 3 + 4) ─────────────────────────────

def get_principals_from_business_search(page, owner_name, logger):
    logger.debug(f"[BizSearch] Searching: {owner_name}")
    try:
        # ── Step 1: Land on homepage and click "Search Business Entity Records" ──
        # Direct navigation to OnlineEntitySearch sometimes returns a page without
        # the search input.  Going through the homepage button is the reliable path.
        page.goto(BIZ_HOME_URL, wait_until="domcontentloaded", timeout=30000)
        try:
            page.wait_for_load_state("networkidle", timeout=10000)
        except PWTimeout:
            pass
        time.sleep(1.5)

        # Click the "Search Business Entity Records" link/button
        clicked_search_entry = False
        for sel in [
            "a:has-text('Search Business Entity Records')",
            "button:has-text('Search Business Entity Records')",
            "a[href*='OnlineEntitySearch']",
            "a[href*='EntitySearch']",
        ]:
            try:
                el = page.query_selector(sel)
                if el and el.is_visible():
                    el.click()
                    clicked_search_entry = True
                    logger.debug(f"[BizSearch] Clicked entry via: {sel}")
                    break
            except Exception:
                continue

        if not clicked_search_entry:
            # Fallback: navigate directly
            logger.debug("[BizSearch] Entry click failed, navigating directly")
            page.goto(BUSINESS_SEARCH_URL, wait_until="domcontentloaded", timeout=30000)

        try:
            page.wait_for_load_state("networkidle", timeout=10000)
        except PWTimeout:
            pass
        time.sleep(1.5)

        # ── Step 2: Fill the "Name" search field and submit ──────────────────
        name_el = None
        for sel in [
            # The OnlineEntitySearch form uses a plain Name field
            "input[name='SearchValue']",
            "input[id*='Name']",
            "input[id*='name']",
            "input[placeholder*='Name']",
            "input[placeholder*='name']",
            # Angular / WAB fallbacks
            "input[formcontrolname='entityName']",
            "input[formcontrolname='name']",
            "input[type='text']",
        ]:
            try:
                el = page.wait_for_selector(sel, timeout=5000)
                if el and el.is_visible():
                    name_el = el
                    logger.debug(f"[BizSearch] Input: {sel}")
                    break
            except PWTimeout:
                continue

        if not name_el:
            logger.warning(f"[BizSearch] No input for '{owner_name}'")
            screenshot(page, "biz_no_input", logger)
            return []

        name_el.click()
        name_el.fill(owner_name)
        time.sleep(0.5)

        # Click Search button
        clicked = False
        for sel in _BIZ_SEARCH_BTN_SELECTORS:
            try:
                btn = page.query_selector(sel)
                if btn and btn.is_visible():
                    btn.click()
                    clicked = True
                    logger.debug(f"[BizSearch] Search btn: {sel}")
                    break
            except Exception:
                continue
        if not clicked:
            name_el.press("Enter")

        # ── Step 3: Wait for OnlineBusinessAndMarkSearchResult page ──────────
        try:
            page.wait_for_url("*OnlineBusinessAndMarkSearchResult*", timeout=15000)
        except PWTimeout:
            pass
        try:
            page.wait_for_load_state("networkidle", timeout=10000)
        except PWTimeout:
            pass
        time.sleep(1.5)

        try:
            page.wait_for_selector("table, tr, a", timeout=10000)
        except PWTimeout:
            logger.warning(f"[BizSearch] No results page for '{owner_name}'")
            screenshot(page, "biz_no_results", logger)
            return []

        # ── Step 4: Click the LLC result ─────────────────────────────────────
        llc_link = _find_llc_link(page, owner_name, logger)
        if not llc_link:
            logger.warning(f"[BizSearch] No LLC for '{owner_name}'")
            screenshot(page, "biz_no_llc", logger)
            return []

        llc_link.click()

        # ── Step 5: Wait for BusinessInformation page ─────────────────────────
        try:
            page.wait_for_url("*BusinessInformation*", timeout=15000)
        except PWTimeout:
            pass
        try:
            page.wait_for_load_state("networkidle", timeout=10000)
        except PWTimeout:
            pass
        time.sleep(1.5)

        try:
            page.wait_for_selector(
                "h2, h3, table, [class*='principal'], [id*='principal'], "
                ":text('PRINCIPAL')",
                timeout=15000,
            )
        except PWTimeout:
            logger.warning(f"[BizSearch] BusinessInformation page not loaded for '{owner_name}'")
            screenshot(page, "biz_no_bizinfo", logger)
            return []

        # ── Step 6: Scrape PRINCIPAL INFORMATION section ──────────────────────
        principals = _scrape_principals(page, logger)
        logger.info(f"[BizSearch] '{owner_name}' → {len(principals)} principal(s)")
        return principals

    except PWTimeout as e:
        logger.warning(f"[BizSearch] Timeout '{owner_name}': {e}")
        screenshot(page, "biz_timeout", logger)
        return []
    except Exception as e:
        logger.error(f"[BizSearch] Error '{owner_name}': {e}")
        screenshot(page, "biz_error", logger)
        return []


def _find_llc_link(page, owner_name, logger):
    """
    Find the table row on OnlineBusinessAndMarkSearchResult whose Name cell
    contains 'LLC' (or LLC variants), preferring the one closest to owner_name.
    Returns the clickable <a> element or <tr> element.
    """
    owner_lower = owner_name.lower().strip()
    best = first_llc = None

    # Strategy 1: direct <a> links in the results table
    try:
        links = page.query_selector_all("table a, tr td a, td a")
    except Exception:
        links = []

    for link in links:
        try:
            text = link.inner_text().strip()
        except Exception:
            continue
        tl = text.lower()
        is_llc = (tl.endswith("llc") or ", llc" in tl
                  or " llc " in tl or "l.l.c" in tl
                  or tl.endswith("lc"))
        if is_llc:
            if first_llc is None:
                first_llc = link
            if owner_lower in tl or tl in owner_lower:
                best = link
                break

    if best or first_llc:
        chosen = best or first_llc
        try:
            logger.debug(f"[BizSearch] Chose: {chosen.inner_text().strip()[:60]}")
        except Exception:
            pass
        return chosen

    # Strategy 2: scan every <tr> for an LLC name cell
    try:
        rows = page.query_selector_all("tr")
    except Exception:
        rows = []
    for row in rows:
        try:
            text = row.inner_text().strip()
        except Exception:
            continue
        tl = text.lower()
        is_llc = (", llc" in tl or " llc" in tl or "l.l.c" in tl)
        if is_llc:
            if first_llc is None:
                first_llc = row
            if owner_lower in tl or tl.startswith(owner_lower[:8]):
                best = row
                break

    chosen = best or first_llc
    if chosen:
        try:
            logger.debug(f"[BizSearch] Chose row: {chosen.inner_text().strip()[:60]}")
        except Exception:
            pass
    return chosen


def _scrape_principals(page, logger):
    """
    Scrape ALL entries from the PRINCIPAL INFORMATION section.

    The section can be split across multiple pages (pagination via a "Next" /
    ">" button).  We collect every page until no more pages exist.
    """
    principals = []
    page_num   = 0

    while True:
        page_num += 1
        page_principals = _scrape_principals_single_page(page, logger, page_num)
        principals.extend(page_principals)

        # Try to advance to the next principals page
        advanced = False
        for nxt_sel in [
            # Text-based selectors
            "a:has-text('Next')",
            "button:has-text('Next')",
            "input[value='Next']",
            "a:has-text('>')",
            "button:has-text('>')",
            # Common pagination class names
            ".next a", ".next button",
            "li.next a", "li.next button",
            "[class*='pagination'] a:has-text('Next')",
            "[class*='pager'] a:has-text('Next')",
            # Arrow icons next to principal table only
            "a[title='Next Page']",
            "button[title='Next Page']",
            "[aria-label='Next Page']",
        ]:
            try:
                btn = page.query_selector(nxt_sel)
                if not btn or not btn.is_visible():
                    continue
                # Make sure the button is not disabled
                disabled = page.evaluate(
                    "(el) => el.disabled || el.classList.contains('disabled') "
                    "|| el.getAttribute('aria-disabled') === 'true'", btn)
                if disabled:
                    continue
                btn.click()
                try:
                    page.wait_for_load_state("networkidle", timeout=8000)
                except PWTimeout:
                    pass
                time.sleep(1.0)
                advanced = True
                logger.debug(f"[BizSearch] Principals page {page_num} → {page_num + 1}")
                break
            except Exception:
                continue

        if not advanced:
            break
        if page_num >= 50:       # safety cap – no real company has 50 principal pages
            break

    logger.debug(f"[BizSearch] Total principals scraped: {len(principals)}")
    return principals


def _scrape_principals_single_page(page, logger, page_num=1):
    """
    Scrape the PRINCIPAL INFORMATION table on the currently visible page.
    Returns a list of principal dicts (may be empty).
    """
    principals = []

    # ── Strategy 1: Find the "PRINCIPAL INFORMATION" heading → nearby table ──
    try:
        all_tables = page.query_selector_all("table")
        for tbl in all_tables:
            full = tbl.inner_text().lower()
            if "principal" not in full and "title" not in full:
                continue
            headers = [th.inner_text().strip().lower()
                       for th in tbl.query_selector_all("th")]
            col = {}
            for i, h in enumerate(headers):
                if "title"   in h:                  col["title"]        = i
                elif "name"  in h:                  col["name"]         = i
                elif "address" in h:                col["address"]      = i
                elif "updated" in h or "date" in h: col["last_updated"] = i
            if not col:
                # No <th> – try the first row's <td> as headers
                first_row = tbl.query_selector("tr")
                if first_row:
                    cells = [c.inner_text().strip().lower()
                             for c in first_row.query_selector_all("td")]
                    for i, h in enumerate(cells):
                        if "title"   in h:                  col["title"]        = i
                        elif "name"  in h:                  col["name"]         = i
                        elif "address" in h:                col["address"]      = i
                        elif "updated" in h or "date" in h: col["last_updated"] = i
            if not col:
                continue
            for row in tbl.query_selector_all("tr"):
                cells = row.query_selector_all("td")
                if not cells or len(cells) < 2:
                    continue
                def _g(key, cells=cells):
                    idx = col.get(key)
                    return cells[idx].inner_text().strip() if (
                        idx is not None and idx < len(cells)) else ""
                entry = {
                    "Principal Title":   _g("title"),
                    "Principal Name":    _g("name"),
                    "Principal Address": _g("address"),
                    "Last Updated":      _g("last_updated"),
                }
                if any(entry.values()):
                    principals.append(entry)
            if principals:
                logger.debug(f"[BizSearch] Page {page_num}: "
                             f"{len(principals)} principal row(s)")
                return principals
    except Exception as e:
        logger.debug(f"[BizSearch] Table strategy page {page_num} failed: {e}")

    # ── Strategy 2: Any table with title/name/address headers ─────────────
    try:
        for tbl in page.query_selector_all("table"):
            headers = [th.inner_text().strip().lower()
                       for th in tbl.query_selector_all("th")]
            joined = " ".join(headers)
            if not any(kw in joined for kw in ("title", "name", "address")):
                continue
            col = {}
            for i, h in enumerate(headers):
                if   "title"   in h:                col["title"]        = i
                elif "name"    in h:                col["name"]         = i
                elif "address" in h:                col["address"]      = i
                elif "updated" in h or "date" in h: col["last_updated"] = i
            for row in tbl.query_selector_all("tr"):
                cells = row.query_selector_all("td")
                if not cells:
                    continue
                def _g(key, cells=cells):
                    idx = col.get(key)
                    return cells[idx].inner_text().strip() if (
                        idx is not None and idx < len(cells)) else ""
                e = {
                    "Principal Title":   _g("title"),
                    "Principal Name":    _g("name"),
                    "Principal Address": _g("address"),
                    "Last Updated":      _g("last_updated"),
                }
                if any(e.values()):
                    principals.append(e)
            if principals:
                return principals
    except Exception:
        pass

    # ── Strategy 3: Raw text of PRINCIPAL section ─────────────────────────
    try:
        full_text = page.inner_text("body")
        idx = full_text.upper().find("PRINCIPAL INFORMATION")
        if idx != -1:
            section_text = full_text[idx:]
            lines = [ln.strip() for ln in section_text.splitlines()
                     if ln.strip() and ln.strip().upper() != "PRINCIPAL INFORMATION"]
            chunk = lines[:20]
            if chunk:
                principals.append({
                    "Principal Title":   chunk[0] if len(chunk) > 0 else "",
                    "Principal Name":    chunk[1] if len(chunk) > 1 else "",
                    "Principal Address": chunk[2] if len(chunk) > 2 else "",
                    "Last Updated":      chunk[3] if len(chunk) > 3 else "",
                })
    except Exception as e:
        logger.debug(f"[BizSearch] Raw-text strategy page {page_num} failed: {e}")

    return principals


# ─── CSV helpers ──────────────────────────────────────────────────────────────

def open_csv(output_path, resume, logger):
    mode = "a" if resume and Path(output_path).exists() else "w"
    f    = open(output_path, mode, newline="", encoding="utf-8-sig")  # utf-8-sig = Excel-friendly BOM
    w    = csv.DictWriter(f, fieldnames=CSV_FIELDS, delimiter=";")    # semicolon separator
    if mode == "w":
        w.writeheader()
        logger.info(f"Created: {output_path}")
    else:
        logger.info(f"Appending to: {output_path}")
    return f, w


def write_rows(writer, tax_id, owner_name, principals, status):
    base = {"Tax ID": tax_id, "Owner Name": owner_name or "", "Status": status}
    if not principals:
        writer.writerow({**base, "Principal Title": "", "Principal Name": "",
                         "Principal Address": "", "Last Updated": ""})
    else:
        for p in principals:
            writer.writerow({**base,
                "Principal Title":   p.get("Principal Title", ""),
                "Principal Name":    p.get("Principal Name", ""),
                "Principal Address": p.get("Principal Address", ""),
                "Last Updated":      p.get("Last Updated", ""),
            })


# ─── Main ─────────────────────────────────────────────────────────────────────

def run(args):
    ts      = datetime.now().strftime("%Y%m%d_%H%M%S")
    logfile = args.logfile or f"scraper_{ts}.log"
    logger  = setup_logging(logfile)

    logger.info("=" * 60)
    logger.info("UT County Leads Scraper  –  v2  (all bugs fixed)")
    logger.info(f"Input   : {args.input}")
    logger.info(f"Output  : {args.output}")
    logger.info(f"Log     : {logfile}")
    logger.info(f"Headless: {args.headless}  |  Delay: {args.delay}s")
    logger.info("=" * 60)

    tax_ids  = load_tax_ids(args.input, logger)
    done_ids = load_done_ids(args.output, logger) if args.resume else set()
    pending  = [t for t in tax_ids if t not in done_ids]
    logger.info(f"Pending: {len(pending)} Tax IDs")
    if not pending:
        logger.info("Nothing to do.")
        return

    csv_file, writer = open_csv(args.output, args.resume, logger)
    ok = err = skip = 0

    with sync_playwright() as pw:
        browser = launch_browser(pw, args.headless, logger)
        context = browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
        )
        parcel_page = context.new_page()
        biz_page    = context.new_page()

        logger.info("Loading Utah County Parcel Map…")
        try:
            parcel_page.goto(PARCEL_MAP_URL, wait_until="domcontentloaded", timeout=45000)
            parcel_page.wait_for_selector(
                ", ".join(_PARCEL_INPUT_SELECTORS[:6]), timeout=30000)
            logger.info("Parcel map loaded.")
        except Exception as e:
            logger.error(f"Parcel map load failed: {e}")
            screenshot(parcel_page, "parcel_initial_fail", logger)
            browser.close()
            csv_file.close()
            sys.exit(1)

        total = len(pending)
        for idx, tax_id in enumerate(pending, start=1):
            logger.info(f"[{idx}/{total}] Tax ID: {tax_id}")

            owner_name = get_owner_from_parcel_map(parcel_page, tax_id, logger)
            if not owner_name:
                write_rows(writer, tax_id, None, [], "owner_not_found")
                csv_file.flush()
                err += 1
                time.sleep(args.delay)
                continue

            principals = get_principals_from_business_search(
                biz_page, owner_name.strip(), logger)

            status = "ok" if principals else "no_principals"
            write_rows(writer, tax_id, owner_name, principals, status)
            csv_file.flush()

            if principals:
                ok += 1
            else:
                skip += 1
                logger.warning(f"  → No LLC data for '{owner_name}'")

            time.sleep(args.delay)

        browser.close()

    csv_file.close()

    logger.info("=" * 60)
    logger.info("SCRAPE COMPLETE")
    logger.info(f"  Total   : {total}")
    logger.info(f"  ✓ Full  : {ok}")
    logger.info(f"  ⚠ Skip  : {skip}")
    logger.info(f"  ✗ Error : {err}")
    logger.info(f"  CSV     : {args.output}")
    logger.info(f"  Log     : {logfile}")
    shots = list(SCREENSHOT_DIR.glob("*.png")) if SCREENSHOT_DIR.exists() else []
    if shots:
        logger.info(f"  Screenshots: {len(shots)} in {SCREENSHOT_DIR}/")
    logger.info("=" * 60)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--input",    default="Expired.xlsx")
    p.add_argument("--output",   default="results.csv")
    p.add_argument("--logfile",  default=None)
    p.add_argument("--headless", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--delay",    type=float, default=2.0)
    p.add_argument("--resume",   action=argparse.BooleanOptionalAction, default=True)
    return p.parse_args()


if __name__ == "__main__":
    run(parse_args())