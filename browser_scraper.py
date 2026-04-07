"""
Browser Scraper — Playwright-based headless browser for JS-heavy sites.
Persistent browser pool — launched once, context-per-request.
"""

import asyncio
from typing import Optional
import re
from urllib.parse import urlparse

import proxy_manager

# Global playwright objects for pooling to bypass 3-second cold starts
_playwright_available = None
_playwright_ctx = None
_browser = None
_browser_lock = asyncio.Lock()

# Semaphore: max 4 concurrent Playwright pages (VPS memory protection)
_browser_semaphore = asyncio.Semaphore(4)


async def init_browser():
    """Start or retrieve the persistent global Playwright chromium instance."""
    global _playwright_ctx, _browser, _playwright_available
    async with _browser_lock:
        try:
            from playwright.async_api import async_playwright
            if _browser is None or not _browser.is_connected():
                if _playwright_ctx is None:
                    _playwright_ctx = await async_playwright().start()
                _browser = await _playwright_ctx.chromium.launch(
                    headless=True,
                    args=[
                        "--no-sandbox",
                        "--disable-setuid-sandbox",
                        "--disable-gpu",
                        "--disable-dev-shm-usage",
                        "--no-first-run",
                        "--disable-background-networking",
                    ],
                )
                _playwright_available = True
                print("[Browser] ✅ Persistent Chromium browser started")
            return _browser
        except Exception as e:
            _playwright_available = False
            print(f"[Browser] Playwright not available: {e}")
            return None


async def check_playwright_available() -> bool:
    global _playwright_available
    if _playwright_available is not None:
        return _playwright_available
    b = await init_browser()
    return b is not None


async def scrape_with_browser(url: str, timeout: int = 15) -> dict:
    """
    Scrape a URL using persistent headless Chromium pool.
    Returns { price, mrp, title, html, success, error }
    """
    result = {
        "url": url,
        "price": None,
        "mrp": None,
        "title": None,
        "html": "",
        "success": False,
        "error": None,
    }

    async with _browser_semaphore:  # max 4 concurrent pages
        try:
            browser = await init_browser()
            if not browser:
                return {**result, "error": "Playwright not installed or failed to boot"}

            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                viewport={"width": 1280, "height": 800},
                extra_http_headers={"Accept-Language": "en-IN,en;q=0.9"},
            )

            page = await context.new_page()

            # Block heavy resources for speed
            await page.route(
                "**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,eot,ico,mp4,mp3}",
                lambda route: route.abort(),
            )
            await page.route(
                "**/{analytics,tracking,ads,doubleclick,facebook,google-analytics,hotjar,clarity}**",
                lambda route: route.abort(),
            )

            # Navigate
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout * 1000)

            # Site-specific waits before price extraction
            hostname = urlparse(url).netloc

            if "myntra" in hostname:
                try:
                    await page.wait_for_function(
                        "() => window.__myx !== undefined || document.querySelector('.pdp-price, .pdp-discount-container') !== null",
                        timeout=7000,
                    )
                except Exception:
                    await page.wait_for_timeout(3000)

            elif "bigbasket" in hostname:
                try:
                    await page.wait_for_function(
                        "() => document.querySelector('[qa=\"product-price\"]') !== null || document.querySelector('[class*=\"PriceWidget\"]') !== null",
                        timeout=7000,
                    )
                except Exception:
                    await page.wait_for_timeout(3000)

            elif "jiomart" in hostname:
                try:
                    await page.wait_for_function(
                        "() => (window.__NEXT_DATA__ && window.__NEXT_DATA__.props) || document.querySelector('[class*=\"Price_sp\"]') !== null",
                        timeout=7000,
                    )
                except Exception:
                    await page.wait_for_timeout(3000)

            elif "meesho" in hostname:
                try:
                    await page.wait_for_selector("[class*='ProductCard'], [class*='product-card']", timeout=7000)
                except Exception:
                    await page.wait_for_timeout(3000)

            elif "flipkart" in hostname:
                try:
                    await page.wait_for_selector(".Nx9bqj, ._30jeq3, [class*='pdp-price']", timeout=7000)
                except Exception:
                    await page.wait_for_timeout(2000)

            elif "nykaa" in hostname:
                try:
                    await page.wait_for_selector("[class*='offer-price'], [class*='selling-price']", timeout=7000)
                except Exception:
                    await page.wait_for_timeout(2500)

            else:
                # Generic: wait for any price indicator
                try:
                    await page.wait_for_selector(
                        ", ".join([
                            "[class*='price']:not([class*='label']):not([class*='range'])",
                            "script[type='application/ld+json']",
                            "[itemprop='price']",
                            "[data-testid*='price']",
                            "meta[property*='price:amount']",
                        ]),
                        timeout=6000,
                        state="attached",
                    )
                except Exception:
                    await page.wait_for_timeout(2500)

            # Price extraction via JS evaluate
            price_data = await page.evaluate("""() => {
                function parseINR(text) {
                    if (!text) return null;
                    if (text.includes('$') || text.toUpperCase().includes('USD')) return null;
                    const m = text.replace(/,/g, '').match(/[₹Rs.]*\\s*(\\d+(?:\\.\\d+)?)/);
                    if (!m) return null;
                    const v = parseFloat(m[1]);
                    return (v > 10 && v < 10000000) ? v : null;
                }

                // ── Strategy 1: JSON-LD ──
                for (const s of document.querySelectorAll('script[type="application/ld+json"]')) {
                    try {
                        let d = JSON.parse(s.textContent);
                        if (Array.isArray(d)) d = d[0];
                        if (d && d['@graph']) {
                            const prod = d['@graph'].find(x => x['@type'] === 'Product');
                            if (prod) d = prod;
                        }
                        if (d && (d['@type'] === 'Product' || d['@type'] === 'IndividualProduct')) {
                            let offers = d.offers || d.offer;
                            if (Array.isArray(offers)) offers = offers[0];
                            if (offers) {
                                const currency = (offers.priceCurrency || '').toUpperCase();
                                if (currency === 'USD') continue;
                                const price = parseFloat(String(offers.price || offers.lowPrice || '0').replace(/,/g, ''));
                                const mrp = parseFloat(String(offers.highPrice || '0').replace(/,/g, ''));
                                if (price > 10) return { price, mrp: mrp > price ? mrp : null, title: d.name || '' };
                            }
                        }
                    } catch {}
                }

                // ── Strategy 2: Meta tags ──
                const metaCurrency = (document.querySelector('meta[property*="price:currency"]')?.content || '').toUpperCase();
                if (metaCurrency !== 'USD') {
                    const metaPrice = document.querySelector('meta[property*="price:amount"]')?.content;
                    if (metaPrice) {
                        const price = parseINR(metaPrice);
                        if (price) return { price, mrp: null, title: document.querySelector('meta[property="og:title"]')?.content || document.title };
                    }
                }

                // ── Strategy 3: Site-specific JS state ──
                const hostname = window.location.hostname;

                // Myntra
                if (hostname.includes('myntra') && window.__myx) {
                    try {
                        const s = JSON.stringify(window.__myx);
                        const m = s.match(/"(?:discounted|selling|mrp|offer)Price"\\s*:\\s*(\\d+)/i);
                        if (m) return { price: parseInt(m[1]), mrp: null, title: document.title };
                    } catch {}
                }

                // BigBasket
                const bbEl = document.querySelector('[qa="product-price"], [class*="PriceWidget__sp"]');
                if (bbEl) {
                    const p = parseINR(bbEl.textContent);
                    if (p) return { price: p, mrp: null, title: document.title };
                }

                // JioMart — __NEXT_DATA__
                if (hostname.includes('jiomart')) {
                    try {
                        const nd = JSON.parse(document.getElementById('__NEXT_DATA__')?.textContent || '{}');
                        const sp = nd?.props?.pageProps?.productDetails?.product?.price?.sp
                                || nd?.props?.pageProps?.product?.sp
                                || nd?.props?.pageProps?.pdpData?.product?.sp
                                || nd?.props?.initialProps?.product?.price;
                        if (sp) {
                            const p = parseFloat(String(sp).replace(/,/g, ''));
                            if (p > 10) return { price: p, mrp: null, title: document.title };
                        }
                    } catch {}
                    const jEl = document.querySelector('[class*="Price_sp"], .sp-price');
                    if (jEl) {
                        const p = parseINR(jEl.textContent);
                        if (p) return { price: p, mrp: null, title: document.title };
                    }
                }

                // Generic window state keys
                for (const key of ['__PRELOADED_STATE__', '__NEXT_DATA__', '__NUXT__', '__nuxt__']) {
                    try {
                        const state = window[key];
                        if (!state) continue;
                        const str = JSON.stringify(state);
                        const m = str.match(/"(?:price|sellingPrice|offerPrice|salePrice|sp)"\\s*:\\s*"?(\\d+(?:\\.\\d+)?)"?/i);
                        if (m) {
                            const p = parseFloat(m[1]);
                            if (p > 10 && p < 10000000) return { price: p, mrp: null, title: document.title };
                        }
                    } catch {}
                }

                // ── Strategy 4: CSS selectors ──
                const selectors = [
                    '._30jeq3._16Jk6d', '.Nx9bqj.CxhGGd', '._30jeq3',      // Flipkart
                    '.a-price .a-offscreen', '.priceToPay .a-offscreen',       // Amazon
                    '.pdp__offerPrice', '[data-testid="selling-price"]',        // Croma
                    '[class*="sellingPrice"]', '[class*="offer-price"]',
                    '[class*="sale-price"]', '[class*="currentPrice"]',
                    '[class*="pdp-price"]', '[class*="PriceText"]',
                    '[class*="discounted_price"]', '[class*="DiscountedPrice"]',
                    '[class*="ProductPrice__offerPrice"]',
                    '.price .money', '.product-price .money',
                    '[class*="priceInfo__packPrice"]', '.discountPrice',
                    '[class*="PriceBox__offer-price"]',
                    '[itemprop="price"]',
                    '.final-price', '.pa-price span', '.selling-price',
                    '.woocommerce-Price-amount',
                ];
                for (const sel of selectors) {
                    const el = document.querySelector(sel);
                    if (!el) continue;
                    const text = el.textContent.trim();
                    if (text.includes('$') || text.toUpperCase().includes('USD')) continue;
                    const p = parseINR(text);
                    if (p) {
                        // Try to find MRP near this element
                        const parent = el.closest('[class*="price"], [class*="Price"]') || el.parentElement;
                        let mrp = null;
                        if (parent) {
                            const mrpEl = parent.querySelector('[class*="mrp"], [class*="strike"], [class*="original"], s, del');
                            if (mrpEl) mrp = parseINR(mrpEl.textContent);
                        }
                        return { price: p, mrp: mrp && mrp > p ? mrp : null, title: document.title };
                    }
                }

                return null;
            }""")

            result["html"] = await page.content()
            result["title"] = await page.title()

            if price_data and price_data.get("price"):
                result["price"] = price_data["price"]
                result["mrp"] = price_data.get("mrp")
                result["title"] = price_data.get("title") or result["title"]
                result["success"] = True
                print(f"[Browser] ✅ {url[:65]} → ₹{result['price']}")
            else:
                # Check if page is blocked/CAPTCHA
                page_html = result["html"]
                if proxy_manager.is_blocked(200, page_html):
                    print(f"[Browser] 🛡️ CAPTCHA detected: {url[:65]} — retrying with proxy")
                    await context.close()
                    # Retry with proxy
                    proxy_url_str = proxy_manager.get_next_proxy()
                    if proxy_url_str:
                        proxy_result = await _browser_scrape_with_proxy(browser, url, timeout, proxy_url_str)
                        if proxy_result["success"]:
                            proxy_manager.report_success(proxy_url_str)
                            return proxy_result
                        else:
                            proxy_manager.report_failure(proxy_url_str)
                else:
                    result["success"] = False
                    print(f"[Browser] ⚠️ {url[:65]} → no price found")

            await context.close()
            # DO NOT close browser — keep persistent pool

        except Exception as e:
            result["error"] = str(e)[:200]
            print(f"[Browser] ❌ {url[:65]} → {str(e)[:80]}")

    return result


async def _browser_scrape_with_proxy(browser, url: str, timeout: int, proxy_url: str) -> dict:
    """Retry a browser scrape using a proxy. Returns the same dict format."""
    result = {
        "url": url,
        "price": None,
        "mrp": None,
        "title": None,
        "html": "",
        "success": False,
        "error": None,
        "used_proxy": True,
    }

    try:
        # Parse proxy URL into Playwright proxy config
        from urllib.parse import urlparse as parse_url
        parsed = parse_url(proxy_url)
        pw_proxy = {
            "server": f"http://{parsed.hostname}:{parsed.port}",
            "username": parsed.username or "",
            "password": parsed.password or "",
        }

        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 800},
            extra_http_headers={"Accept-Language": "en-IN,en;q=0.9"},
            proxy=pw_proxy,
        )

        page = await context.new_page()
        await page.route(
            "**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,eot,ico,mp4,mp3}",
            lambda route: route.abort(),
        )

        await page.goto(url, wait_until="domcontentloaded", timeout=timeout * 1000)
        await page.wait_for_timeout(3000)  # generic wait for proxy scrape

        result["html"] = await page.content()
        result["title"] = await page.title()

        # Quick price check from page title or content
        if not proxy_manager.is_blocked(200, result["html"]):
            result["success"] = True
            print(f"[Browser] ✅ Proxy success: {url[:65]}")
        else:
            print(f"[Browser] 🛡️ Proxy also blocked: {url[:65]}")

        await context.close()

    except Exception as e:
        result["error"] = str(e)[:200]
        print(f"[Browser] ❌ Proxy error: {url[:65]} → {str(e)[:80]}")

    return result