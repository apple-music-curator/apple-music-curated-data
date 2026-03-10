const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');

/**
 * Apple Music US Crawler (cleaned)
 * - Crawl-only (no artist search pipeline)
 * - No radio seeds
 * - No radio pages/items
 * - Keep ONLY pages whose subtitle contains "Apple Music"
 * - Album rule: do NOT parse album tracklist; only use album page sections/featured links
 * - Depth = 2
 * - Conservative throttle detector
 * - Includes multi-room seed and set-list playlist track scraping
 */

const SEED_URLS = [
  'https://music.apple.com/us/top-charts',
  'https://music.apple.com/us/new/top-charts',
  'https://music.apple.com/us/room/6760169562',
  'https://music.apple.com/us/multi-room/1666391966',
];

const MAX_DEPTH = 2;
const MAX_QUEUE_SIZE = 10000;
const MAX_TRACKS_PER_ITEM = 500;

// Tuned concurrency/pacing
const MAX_BROWSERS = 5;            // down from 10
const PARALLEL_PER_BROWSER = 4;    // down from 5
const PAGE_TIMEOUT = 45000;
const DELAY_BETWEEN_PAGES = 600;   // up from 120
const NAV_RETRY_BACKOFF_MS = 2500; // backoff before retry nav

// Conservative throttle detector
const THROTTLE_WINDOW_SIZE = 40;
const THROTTLE_FAIL_THRESHOLD = 0.45;
const THROTTLE_MIN_ATTEMPTS = 20;
const THROTTLE_COOLDOWN_MS = 20000;

const OUTPUT_FILE = path.join(__dirname, 'data', 'us.json');

function ensureDir(d) {
  if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true });
}
function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}
function generateId(url) {
  const hash = url.split('').reduce((a, b) => {
    a = ((a << 5) - a) + b.charCodeAt(0);
    return a & a;
  }, 0);
  return Math.abs(hash).toString(36);
}
function detectType(url = '') {
  const u = url.toLowerCase();
  if (u.includes('/artist/')) return 'artist';
  if (u.includes('/song/')) return 'song';
  if (u.includes('/album/')) return 'album';
  if (u.includes('/single/') || u.includes('/ep/')) return 'single';
  if (u.includes('/playlist/')) return 'playlist';
  if (u.includes('/chart/')) return 'chart';
  if (u.includes('/radio') || u.includes('/station/')) return 'radio';
  if (u.includes('/multi-room/')) return 'multi-room';
  if (u.includes('/room/')) return 'room';
  return 'other';
}
function isAppleMusicSubtitle(subtitle = '') {
  return subtitle.toLowerCase().includes('apple music');
}
function shouldKeepPage(subtitle) {
  return isAppleMusicSubtitle(subtitle || '');
}
function dedupeByUrl(items) {
  const out = [];
  const seen = new Set();
  for (const x of items || []) {
    if (!x?.url || seen.has(x.url)) continue;
    seen.add(x.url);
    out.push(x);
  }
  return out;
}
function normalizeOutputItem(item) {
  return {
    id: generateId(item.url),
    name: item.name || 'Unknown',
    type: item.type || detectType(item.url),
    country: 'us',
    url: item.url,
    searchTerms: (item.name || '').toLowerCase().replace(/[^a-z0-9\s]/g, ' ').trim(),
    scrapedAt: new Date().toISOString(),
    creator: item.creator || '',
    metadata: item.metadata || '',
    description: item.description || '',
    tracks: (item.tracks || []).slice(0, MAX_TRACKS_PER_ITEM),
    sections: item.sections || [],
    featuredItems: item.featuredItems || [],
  };
}

class ThrottleDetector {
  constructor() {
    this.outcomes = []; // true=success, false=fail
    this.cooldownUntil = 0;
  }

  record(ok) {
    this.outcomes.push(Boolean(ok));
    if (this.outcomes.length > THROTTLE_WINDOW_SIZE) this.outcomes.shift();
  }

  stats() {
    const attempts = this.outcomes.length;
    const fails = this.outcomes.filter(x => !x).length;
    const failRate = attempts ? fails / attempts : 0;
    return { attempts, fails, failRate };
  }

  shouldTrip() {
    const { attempts, failRate } = this.stats();
    return attempts >= THROTTLE_MIN_ATTEMPTS && failRate >= THROTTLE_FAIL_THRESHOLD;
  }

  trip() {
    this.cooldownUntil = Date.now() + THROTTLE_COOLDOWN_MS;
  }

  inCooldown() {
    return Date.now() < this.cooldownUntil;
  }

  cooldownRemaining() {
    return Math.max(0, this.cooldownUntil - Date.now());
  }
}

async function safeGoto(page, url, timeout = PAGE_TIMEOUT) {
  try {
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout });
    if (String(page.url()).startsWith('chrome-error://')) throw new Error('chrome-error-page');
    return true;
  } catch (e1) {
    if (url.includes('/album/')) {
      console.error(`safeGoto failed ${url}: ${e1.message} (album no-retry)`);
      return false;
    }
  }

  await sleep(NAV_RETRY_BACKOFF_MS);

  try {
    await page.goto(url, { waitUntil: 'load', timeout: timeout + 10000 });
    if (String(page.url()).startsWith('chrome-error://')) throw new Error('chrome-error-page');
    return true;
  } catch (e2) {
    console.error(`safeGoto failed ${url}: ${e2.message}`);
    return false;
  }
}

async function scrollUntilExhausted(page, direction = 'vertical') {
  await page.evaluate(async (dir) => {
    const delay = 700;
    const max = 25;
    let last = dir === 'vertical' ? window.scrollY : window.scrollX;
    let same = 0;

    for (let i = 0; i < max; i++) {
      if (dir === 'vertical') window.scrollBy(0, 900);
      else {
        const containers = document.querySelectorAll('[style*="overflow-x"], .shelf-grid, [class*="carousel"]');
        for (const c of containers) c.scrollBy({ left: 450, behavior: 'smooth' });
        window.scrollBy({ left: 450, top: 0, behavior: 'smooth' });
      }

      await new Promise(r => setTimeout(r, delay));
      const cur = dir === 'vertical' ? window.scrollY : window.scrollX;
      if (cur === last) {
        same++;
        if (same >= 3) break;
      } else {
        same = 0;
      }
      last = cur;
    }

    if (dir === 'vertical') window.scrollTo(0, 0);
  }, direction);
}

async function extractLinksSectionsAndTracks(page, pageType = 'other') {
  return page.evaluate((pt) => {
    const r = {
      links: [],
      sections: [],
      tracks: [],
      featuredItems: [],
      pageTitle: '',
      pageSubtitle: '',
      pageMetadata: '',
      pageDescription: '',
    };

    function typeOf(url = '') {
      const u = url.toLowerCase();
      if (u.includes('/artist/')) return 'artist';
      if (u.includes('/song/')) return 'song';
      if (u.includes('/album/')) return 'album';
      if (u.includes('/single/') || u.includes('/ep/')) return 'single';
      if (u.includes('/playlist/')) return 'playlist';
      if (u.includes('/chart/')) return 'chart';
      if (u.includes('/radio') || u.includes('/station/')) return 'radio';
      if (u.includes('/multi-room/')) return 'multi-room';
      if (u.includes('/room/')) return 'room';
      return 'other';
    }

    r.pageTitle = document.querySelector('[class*="headings__title"]')?.textContent?.trim() || '';
    r.pageSubtitle = document.querySelector('[class*="headings__subtitles"]')?.textContent?.trim() || '';
    r.pageMetadata = document.querySelector('[class*="headings__metadata-bottom"]')?.textContent?.trim() || '';
    r.pageDescription = document.querySelector('[class*="description"]')?.textContent?.trim() || '';

    const anchors = document.querySelectorAll('a[href]');
    for (const a of anchors) {
      if (!a.href?.includes('music.apple.com/us/')) continue;
      const u = a.href.split('?')[0].split('#')[0];
      if (!r.links.includes(u)) r.links.push(u);
    }

    const sectionEls = document.querySelectorAll('[data-testid="section-container"]');
    for (const sec of sectionEls) {
      const name = sec.querySelector('h2, h3, [class*="title"]')?.textContent?.trim() || 'Section';
      const items = [];
      const itemLinks = sec.querySelectorAll(
        'a[href*="/album/"],a[href*="/playlist/"],a[href*="/artist/"],a[href*="/chart/"],a[href*="/room/"],a[href*="/multi-room/"]'
      );
      for (const l of itemLinks) {
        const u = l.href?.split('?')[0]?.split('#')[0];
        if (!u || !u.includes('music.apple.com')) continue;
        items.push({
          name: (l.textContent || '').trim().substring(0, 200) || u,
          url: u,
          type: typeOf(u),
        });
      }
      if (items.length) r.sections.push({ name, items: dedupe(items) });
    }

    const featSections = document.querySelectorAll('[data-testid="section-container"][aria-label="Featured"]');
    for (const sec of featSections) {
      const cards = sec.querySelectorAll('[class*="lockup"], a[href*="/playlist/"], a[href*="/album/"], a[href*="/chart/"], a[href*="/room/"], a[href*="/multi-room/"]');
      for (const c of cards) {
        const linkEl = c.tagName === 'A' ? c : c.querySelector('a[href]');
        const raw = linkEl?.href || '';
        if (!raw.includes('music.apple.com')) continue;
        const url = raw.split('?')[0].split('#')[0];
        const title = c.querySelector('[class*="headings__title"], [class*="title"]')?.textContent?.trim() || '';
        const subtitle = c.querySelector('[class*="headings__subtitles"]')?.textContent?.trim() || '';
        const metadata = c.querySelector('[class*="headings__metadata-bottom"]')?.textContent?.trim() || '';
        const description = c.querySelector('[class*="description"]')?.textContent?.trim() || '';
        if (title && url) {
          r.featuredItems.push({ name: title, url, type: typeOf(url), creator: subtitle, metadata, description });
        }
      }
    }

    // IMPORTANT: skip album tracklist extraction ONLY for albums.
    // For playlists (including set-list playlists under multi-room), do extract tracks.
    if (pt !== 'album') {
      const selectors = ['div.songs-list-row', 'li.songs-list-item', '[data-testid="track-row"]', 'div[class*="track"]'];
      let trackEls = [];
      for (const s of selectors) {
        const f = document.querySelectorAll(s);
        if (f.length) {
          trackEls = Array.from(f);
          break;
        }
      }
      if (!trackEls.length) trackEls = Array.from(document.querySelectorAll('a[href*="/song/"]'));

      const seen = new Set();
      trackEls.forEach((el, idx) => {
        let url = '';
        if (el.href?.includes('/song/')) url = el.href.split('?')[0];
        else {
          const sl = el.querySelector('a[href*="/song/"]');
          if (sl) url = sl.href.split('?')[0];
        }
        if (!url || seen.has(url)) return;
        seen.add(url);

        const name = (
          el.querySelector('[class*="title"],[class*="name"]')?.textContent?.trim() ||
          el.textContent?.trim() ||
          ''
        ).substring(0, 200);
        if (!name) return;

        r.tracks.push({
          name,
          url,
          artist: el.querySelector('[class*="artist"], .by-line')?.textContent?.trim() || '',
          album: el.querySelector('[class*="album"]')?.textContent?.trim() || '',
          duration: el.querySelector('[class*="duration"], .time')?.textContent?.trim() || '',
          position: idx + 1,
        });
      });
    }

    function dedupe(a) {
      const out = [];
      const seen = new Set();
      for (const x of a) {
        if (!x?.url || seen.has(x.url)) continue;
        seen.add(x.url);
        out.push(x);
      }
      return out;
    }

    return r;
  }, pageType);
}

async function crawlPage(page, url) {
  const pageType = detectType(url);
  const ok = await safeGoto(page, url);
  if (!ok) {
    return {
      links: [], sections: [], tracks: [], featuredItems: [],
      pageTitle: '', pageSubtitle: '', pageMetadata: '', pageDescription: '',
      navOk: false,
      pageType,
    };
  }

  await page.waitForTimeout(1200);
  await scrollUntilExhausted(page, 'vertical');
  await scrollUntilExhausted(page, 'horizontal');

  const x = await extractLinksSectionsAndTracks(page, pageType);
  return { ...x, navOk: true, pageType };
}

async function createBrowserPool() {
  const browsers = [];
  for (let i = 0; i < MAX_BROWSERS; i++) {
    browsers.push(await chromium.launch({ headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox'] }));
  }
  return browsers;
}
async function closeBrowserPool(pool) {
  for (const b of pool) {
    try { await b.close(); } catch {}
  }
}

async function main() {
  ensureDir(path.dirname(OUTPUT_FILE));

  const state = {
    visited: new Set(),
    itemsByUrl: new Map(),
  };

  const queue = SEED_URLS.map(url => ({ url, depth: 1 }));
  let pageCount = 0;

  const throttle = new ThrottleDetector();

  const browsers = await createBrowserPool();

  let queueLock = Promise.resolve();
  async function takeTask() {
    let task = null;
    await (queueLock = queueLock.then(() => {
      if (queue.length > MAX_QUEUE_SIZE) queue.length = MAX_QUEUE_SIZE;
      task = queue.shift() || null;
    }));
    return task;
  }

  async function loop(browserIndex) {
    const browser = browsers[browserIndex];
    while (true) {
      if (throttle.inCooldown()) {
        await sleep(Math.min(2000, throttle.cooldownRemaining()));
        continue;
      }

      const task = await takeTask();
      if (!task) break;

      const { url, depth } = task;
      if (!url || state.visited.has(url)) continue;
      state.visited.add(url);

      const t = detectType(url);
      if (t === 'song' || t === 'radio') continue;

      const page = await browser.newPage();
      let navOkForThrottle = false;
      try {
        const x = await crawlPage(page, url);
        navOkForThrottle = x.navOk;
        throttle.record(x.navOk);
        if (throttle.shouldTrip()) {
          const s = throttle.stats();
          throttle.trip();
          console.warn(`Throttle cooldown: attempts=${s.attempts}, fails=${s.fails}, failRate=${s.failRate.toFixed(2)} for ${THROTTLE_COOLDOWN_MS}ms`);
        }

        pageCount++;

        const subtitle = x.pageSubtitle || '';
        if (x.navOk && shouldKeepPage(subtitle) && (x.featuredItems.length || x.sections.length || x.tracks.length)) {
          state.itemsByUrl.set(url, normalizeOutputItem({
            name: x.pageTitle || url.split('/').pop() || 'Unknown',
            url,
            type: t,
            creator: subtitle || 'Apple Music',
            metadata: x.pageMetadata || '',
            description: x.pageDescription || '',
            tracks: x.tracks || [], // [] for album pages
            sections: x.sections || [],
            featuredItems: x.featuredItems || [],
          }));
        }

        if (x.navOk && depth < MAX_DEPTH) {
          // No radio crawling
          const allowedChildTypes = new Set(['artist', 'album', 'single', 'playlist', 'chart', 'room', 'multi-room']);
          const candidates = [];
          for (const l of x.links || []) {
            if (state.visited.has(l)) continue;
            const lt = detectType(l);
            if (!allowedChildTypes.has(lt)) continue;
            candidates.push({ url: l, depth: depth + 1 });
          }
          const deduped = dedupeByUrl(candidates);
          for (const nl of deduped) {
            if (queue.length < MAX_QUEUE_SIZE) queue.push(nl);
          }
        }

        if (pageCount % 25 === 0) {
          const items = Array.from(state.itemsByUrl.values());
          const totalTracks = items.reduce((s, i) => s + (i.tracks?.length || 0), 0);
          const ts = throttle.stats();
          console.log(`Pages Crawled: ${pageCount} | Queue: ${queue.length}/${MAX_QUEUE_SIZE} | Items: ${items.length} | Total Tracks: ${totalTracks} | Throttle failRate=${ts.failRate.toFixed(2)} (${ts.fails}/${ts.attempts})`);
        }
      } catch (e) {
        throttle.record(false);
        if (throttle.shouldTrip()) throttle.trip();
        console.error(`task error ${url}: ${e.message}`);
      } finally {
        await page.close();
      }

      if (!navOkForThrottle && throttle.inCooldown()) {
        await sleep(Math.max(DELAY_BETWEEN_PAGES, 1200));
      } else {
        await sleep(DELAY_BETWEEN_PAGES);
      }
    }
  }

  const loops = [];
  for (let b = 0; b < MAX_BROWSERS; b++) {
    for (let p = 0; p < PARALLEL_PER_BROWSER; p++) loops.push(loop(b));
  }

  await Promise.all(loops);
  await closeBrowserPool(browsers);

  const items = Array.from(state.itemsByUrl.values());
  const payload = {
    lastUpdated: new Date().toISOString(),
    country: 'us',
    phase: 'crawl-only-apple-music-subtitle-no-radio-album-sections-only-multi-room-setlists',
    totalItems: items.length,
    totalTracks: items.reduce((s, i) => s + (i.tracks?.length || 0), 0),
    items,
  };

  fs.writeFileSync(OUTPUT_FILE, JSON.stringify(payload, null, 2));
  console.log(`Done -> ${OUTPUT_FILE} (${payload.totalItems} items)`);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
