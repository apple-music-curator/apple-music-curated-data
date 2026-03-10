const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');
const glob = require('glob');
const { spawn } = require('child_process');

/**
 * ============================================================
 * Apple Music US Crawler + Artist Pipeline (Full File)
 * ============================================================
 *
 * Includes:
 *  - Depth-1 split into 3 workers
 *  - Depth-2 split into 9 workers (3x3)
 *  - MAX_DEPTH = 2 for non-artist crawl
 *  - MAX_QUEUE_SIZE = 10000
 *  - Apple Music / Artist filtering rule
 *  - MAX_SUBPAGES_PER_ARTIST = 1
 *  - Scroll max halved (25)
 *  - Robust navigation with safeGoto (domcontentloaded + fallback load)
 *  - Album no-retry rule on hard failure
 *  - Lower per-worker parallel pressure (PARALLEL_PER_BROWSER = 2)
 *  - Better logging with worker label in metrics
 *  - Throttle detection + requeue handoff
 *  - Heartbeats + streamed child logs
 */

const SEED_URLS = [
  'https://music.apple.com/us/top-charts',
  'https://music.apple.com/us/new/top-charts',
  'https://music.apple.com/us/radio',
  'https://music.apple.com/us/room/6760169562',
];

const MANDATORY_ARTISTS = [
  { name: 'Coco Jones', url: 'https://music.apple.com/us/artist/coco-jones/401400095' },
  { name: 'Amelia Moore', url: 'https://music.apple.com/us/artist/amelia-moore/1348611202' },
  { name: 'SAILORR', url: 'https://music.apple.com/us/artist/sailorr/1741604584' },
];

// Keep your complete list in production
const TOP_ARTISTS = [
  'Bruno Mars', 'Bad Bunny', 'The Weeknd', 'Rihanna', 'Taylor Swift',
  'Justin Bieber', 'Lady Gaga', 'Coldplay', 'Billie Eilish', 'Drake',
  'J Balvin', 'Ariana Grande', 'Ed Sheeran', 'David Guetta', 'Shakira',
  'Kendrick Lamar', 'Maroon 5', 'Eminem', 'Calvin Harris', 'SZA',
];

const MAX_DEPTH = 2;
const DEPTH1_SPLITS = 3;
const DEPTH2_SPLITS_PER_DEPTH1 = 3; // total depth2 workers = 9
const MAX_PAGES_TO_CRAWL = 25; // test cap per crawl worker

const ARTIST_WORKERS = 12;
const MAX_BROWSERS = 10;
const PARALLEL_PER_BROWSER = 2; // lowered for stability/rate-limit mitigation
const TOTAL_TASK_LOOPS = MAX_BROWSERS * PARALLEL_PER_BROWSER;

const MAX_QUEUE_SIZE = 50;
const MAX_TRACKS_PER_ITEM = 500;
const MAX_SUBPAGES_PER_ARTIST = 1;
const PAGE_TIMEOUT = 45000;
const DELAY_BETWEEN_PAGES = 120;
const HEARTBEAT_MS = 30000;
const NAV_RETRY_BACKOFF_MS = 1200;

// throttle detection / handoff
const THROTTLE_WINDOW_SIZE = 60;
const THROTTLE_FAIL_THRESHOLD = 0.75;
const THROTTLE_MIN_ATTEMPTS = 30;
const THROTTLE_REQUEUE_EXIT_CODE = 85;

const ROOT_DIR = __dirname;
const DATA_DIR = path.join(ROOT_DIR, 'data');
const WORKFLOW_DIR = path.join(DATA_DIR, 'workflows');
const OUTPUT_DIR = path.join(DATA_DIR, 'outputs');
const LOG_DIR = path.join(DATA_DIR, 'logs');
const REQUEUE_DIR = path.join(DATA_DIR, 'requeue');

const args = process.argv.slice(2);
const MODE = getArg('--mode') || 'orchestrator';
const WORKFLOW_FILE = getArg('--workflow') || null;
const FINAL_OUTPUT = getArg('--out') || path.join(DATA_DIR, 'us.json');
const MERGE_GLOB = getArg('--merge-glob') || path.join(OUTPUT_DIR, '*.json');

function getArg(flag) {
  const i = args.findIndex(a => a === flag);
  return i >= 0 ? (args[i + 1] || null) : null;
}
function ensureDir(d) { if (!fs.existsSync(d)) fs.mkdirSync(d, { recursive: true }); }
function ensureDataDirs() { [DATA_DIR, WORKFLOW_DIR, OUTPUT_DIR, LOG_DIR, REQUEUE_DIR].forEach(ensureDir); }
function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

function generateId(url) {
  const hash = url.split('').reduce((a, b) => {
    a = ((a << 5) - a) + b.charCodeAt(0);
    return a & a;
  }, 0);
  return Math.abs(hash).toString(36);
}
function saveJson(file, data) { fs.writeFileSync(file, JSON.stringify(data, null, 2)); }
function loadJson(file, fallback = null) {
  try {
    if (fs.existsSync(file)) return JSON.parse(fs.readFileSync(file, 'utf8'));
  } catch {}
  return fallback;
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
  if (u.includes('/room/')) return 'room';
  return 'other';
}
function classifyUrl(url = '') {
  if (url.includes('/album/')) return 'album';
  if (url.includes('/artist/')) return 'artist';
  if (url.includes('/playlist/')) return 'playlist';
  return 'other';
}
function splitArray(arr, parts) {
  const out = Array.from({ length: parts }, () => []);
  for (let i = 0; i < arr.length; i++) out[i % parts].push(arr[i]);
  return out;
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
    subItems: item.subItems || [],
  };
}
function isAppleMusicSubtitle(subtitle = '') {
  return subtitle.toLowerCase().includes('apple music');
}
function shouldKeepPage(type, subtitle) {
  // strict rule requested earlier
  return type === 'artist' || isAppleMusicSubtitle(subtitle || '');
}

function startHeartbeat(label, getSnapshot) {
  const timer = setInterval(() => {
    try {
      const snap = getSnapshot ? getSnapshot() : {};
      console.log(`[HEARTBEAT:${label}] ${new Date().toISOString()} ${JSON.stringify(snap)}`);
    } catch {
      console.log(`[HEARTBEAT:${label}] ${new Date().toISOString()} alive`);
    }
  }, HEARTBEAT_MS);
  return () => clearInterval(timer);
}

function createThrottleTracker() {
  const results = []; // true=success, false=fail
  return {
    record(ok) {
      results.push(!!ok);
      if (results.length > THROTTLE_WINDOW_SIZE) results.shift();
    },
    score() {
      if (!results.length) return { attempts: 0, failRate: 0 };
      const fails = results.filter(x => !x).length;
      return { attempts: results.length, failRate: fails / results.length };
    },
    isThrottled(globalAttempts) {
      const { attempts, failRate } = this.score();
      if (globalAttempts < THROTTLE_MIN_ATTEMPTS) return false;
      if (attempts < Math.min(THROTTLE_WINDOW_SIZE, 20)) return false;
      return failRate >= THROTTLE_FAIL_THRESHOLD;
    },
  };
}

async function safeGoto(page, url, timeout = PAGE_TIMEOUT) {
  const kind = classifyUrl(url);

  // attempt 1: fast
  try {
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout });
    if (String(page.url()).startsWith('chrome-error://')) throw new Error('chrome-error-page');
    return { ok: true, reason: 'ok-domcontentloaded' };
  } catch (e1) {
    // album no-retry rule
    if (kind === 'album') {
      console.error(`safeGoto failed ${url}: ${e1.message} (album no-retry)`);
      return { ok: false, reason: 'album-no-retry-fail' };
    }
  }

  // attempt 2: fallback for non-album
  await page.waitForTimeout(NAV_RETRY_BACKOFF_MS + Math.floor(Math.random() * 800));
  try {
    await page.goto(url, { waitUntil: 'load', timeout: timeout + 10000 });
    if (String(page.url()).startsWith('chrome-error://')) throw new Error('chrome-error-page');
    return { ok: true, reason: 'ok-load-fallback' };
  } catch (e2) {
    console.error(`safeGoto failed ${url}: ${e2.message}`);
    return { ok: false, reason: 'fallback-fail' };
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
      } else same = 0;
      last = cur;
    }

    if (dir === 'vertical') window.scrollTo(0, 0);
  }, direction);
}

async function extractLinksSectionsAndTracks(page) {
  return page.evaluate(() => {
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
      const itemLinks = sec.querySelectorAll('a[href*="/album/"],a[href*="/playlist/"],a[href*="/artist/"],a[href*="/chart/"],a[href*="/room/"]');
      for (const l of itemLinks) {
        const u = l.href?.split('?')[0]?.split('#')[0];
        if (!u || !u.includes('music.apple.com')) continue;
        items.push({ name: (l.textContent || '').trim().substring(0, 200) || u, url: u, type: typeOf(u) });
      }
      if (items.length) r.sections.push({ name, items: dedupe(items) });
    }

    const featSections = document.querySelectorAll('[data-testid="section-container"][aria-label="Featured"]');
    for (const sec of featSections) {
      const cards = sec.querySelectorAll('[class*="lockup"], a[href*="/playlist/"], a[href*="/album/"], a[href*="/chart/"], a[href*="/room/"]');
      for (const c of cards) {
        const linkEl = c.tagName === 'A' ? c : c.querySelector('a[href]');
        const raw = linkEl?.href || '';
        if (!raw.includes('music.apple.com')) continue;
        const url = raw.split('?')[0].split('#')[0];
        const title = c.querySelector('[class*="headings__title"], [class*="title"]')?.textContent?.trim() || '';
        const subtitle = c.querySelector('[class*="headings__subtitles"]')?.textContent?.trim() || '';
        const metadata = c.querySelector('[class*="headings__metadata-bottom"]')?.textContent?.trim() || '';
        const description = c.querySelector('[class*="description"]')?.textContent?.trim() || '';
        if (title && url) r.featuredItems.push({ name: title, url, type: typeOf(url), creator: subtitle, metadata, description });
      }
    }

    const selectors = ['div.songs-list-row', 'li.songs-list-item', '[data-testid="track-row"]', 'div[class*="track"]'];
    let trackEls = [];
    for (const s of selectors) {
      const f = document.querySelectorAll(s);
      if (f.length) { trackEls = Array.from(f); break; }
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

      const name = (el.querySelector('[class*="title"],[class*="name"]')?.textContent?.trim() || el.textContent?.trim() || '').substring(0, 200);
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

    function dedupe(a) {
      const o = [];
      const s = new Set();
      for (const x of a) {
        if (!x?.url || s.has(x.url)) continue;
        s.add(x.url);
        o.push(x);
      }
      return o;
    }

    return r;
  });
}

async function crawlPage(page, url) {
  try {
    const nav = await safeGoto(page, url);
    if (!nav.ok) {
      return {
        links: [], sections: [], tracks: [], featuredItems: [],
        pageTitle: '', pageSubtitle: '', pageMetadata: '', pageDescription: '',
        navOk: false, navReason: nav.reason,
      };
    }

    if (String(page.url()).startsWith('chrome-error://')) {
      return {
        links: [], sections: [], tracks: [], featuredItems: [],
        pageTitle: '', pageSubtitle: '', pageMetadata: '', pageDescription: '',
        navOk: false, navReason: 'chrome-error-page',
      };
    }

    await page.waitForTimeout(1200); // settle dynamic content
    await scrollUntilExhausted(page, 'vertical');
    await scrollUntilExhausted(page, 'horizontal');

    const data = await extractLinksSectionsAndTracks(page);
    return { ...data, navOk: true, navReason: nav.reason };
  } catch (e) {
    console.error(`Error crawling ${url}: ${e.message}`);
    return {
      links: [], sections: [], tracks: [], featuredItems: [],
      pageTitle: '', pageSubtitle: '', pageMetadata: '', pageDescription: '',
      navOk: false, navReason: 'crawl-exception',
    };
  }
}

async function createBrowserPool() {
  const list = [];
  for (let i = 0; i < MAX_BROWSERS; i++) {
    list.push(await chromium.launch({ headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox'] }));
  }
  return list;
}
async function closeBrowserPool(pool) {
  for (const b of pool) {
    try { await b.close(); } catch {}
  }
}

async function processPageTask(task, browser, state) {
  const { url, depth } = task;
  if (!url || state.visited.has(url)) {
    return { processed: false, links: [], tracksCount: 0, subtitle: '', depth, navOk: true };
  }
  state.visited.add(url);

  const t = detectType(url);
  if (t === 'song' || t === 'radio') {
    return { processed: false, links: [], tracksCount: 0, subtitle: '', depth, navOk: true };
  }

  const page = await browser.newPage();
  try {
    const x = await crawlPage(page, url);
    const subtitle = x.pageSubtitle || '';
    const isAllowed = shouldKeepPage(t, subtitle);

    if (x.navOk && isAllowed && (x.tracks.length || x.featuredItems.length || x.sections.length)) {
      state.itemsByUrl.set(url, normalizeOutputItem({
        name: x.pageTitle || url.split('/').pop() || 'Unknown',
        url,
        type: t,
        creator: subtitle || 'Apple Music',
        metadata: x.pageMetadata || '',
        description: x.pageDescription || '',
        tracks: x.tracks || [],
        sections: x.sections || [],
        featuredItems: x.featuredItems || [],
      }));
    }

    const allowedChildTypes = new Set(['artist', 'album', 'single', 'playlist', 'chart', 'room']);
    const newLinks = [];
    if (x.navOk && depth < MAX_DEPTH) {
      for (const l of x.links || []) {
        if (state.visited.has(l)) continue;
        const lt = detectType(l);
        if (!allowedChildTypes.has(lt)) continue;
        newLinks.push({ url: l, depth: depth + 1 });
      }
    }

    return {
      processed: true,
      links: dedupeByUrl(newLinks),
      tracksCount: (x.tracks || []).length,
      subtitle,
      depth,
      navOk: x.navOk,
      navReason: x.navReason || '',
    };
  } finally {
    await page.close();
  }
}

async function loop(browserIndex) {
  const browser = browsers[browserIndex];
  while (true) {
    if (throttled) break;
    if (pageCount >= MAX_PAGES_TO_CRAWL) break; // cap

    const task = await takeTask();
    if (!task) break;

    try {
      const r = await processPageTask(task, browser, state);
      if (r.processed) pageCount += 1;

      globalAttempts += 1;
      throttleTracker.record(!!r.navOk);

      for (const nl of r.links || []) {
        if (!state.visited.has(nl.url) && queue.length < MAX_QUEUE_SIZE) queue.push(nl);
      }

      await addMetrics(r);
      if (pageCount > 0 && pageCount % 10 === 0) await flushMetrics(false);

      if (throttleTracker.isThrottled(globalAttempts) && queue.length > 0) {
        throttled = true;
        const sc = throttleTracker.score();
        console.error(`[${workerLabel}] THROTTLED detected. failRate=${sc.failRate.toFixed(3)} attemptsWindow=${sc.attempts} globalAttempts=${globalAttempts}. Will requeue remaining ${queue.length} tasks.`);
        break;
      }
    } catch (e) {
      console.error(`[${workerLabel}] task error: ${e.message}`);
    }
    await sleep(DELAY_BETWEEN_PAGES);
  }
}

  let metricLock = Promise.resolve();
  let batchProcessed = 0;
  let batchTracks = 0;
  let batchDepth = null;
  const batchSubtitles = new Set();

  const stopHeartbeat = startHeartbeat(`crawl:${workerLabel}`, () => {
    const sc = throttleTracker.score();
    return {
      pageCount,
      queue: queue.length,
      maxQueue: MAX_QUEUE_SIZE,
      items: state.itemsByUrl.size,
      maxPagesToCrawl: MAX_PAGES_TO_CRAWL,
      batchProcessed,
      batchTracks,
      batchDepth: batchDepth ?? null,
      throttleAttemptsWindow: sc.attempts,
      throttleFailRateWindow: Number(sc.failRate.toFixed(3)),
      globalAttempts,
      throttled,
    };
  });

  async function addMetrics(result) {
    await (metricLock = metricLock.then(() => {
      if (batchDepth === null && typeof result.depth === 'number') batchDepth = result.depth;
      batchProcessed += result.processed ? 1 : 0;
      batchTracks += result.tracksCount || 0;
      if (result.subtitle) batchSubtitles.add(result.subtitle);
    }));
  }

  async function flushMetrics(force = false) {
    await (metricLock = metricLock.then(() => {
      if (!force && batchProcessed < 10) return;
      if (batchProcessed === 0) return;

      const items = Array.from(state.itemsByUrl.values());
      const allTotalTracks = items.reduce((sum, item) => sum + (item.tracks?.length || 0), 0);
      const subtitles = [...batchSubtitles].join(', ') || 'N/A';
      const depthForLog = batchDepth ?? '?';
      const sc = throttleTracker.score();

      console.log(
        `[${workerLabel}] Pages Crawled: ${pageCount} | Depth: ${depthForLog}/${MAX_DEPTH} | Processed Pages: ${batchProcessed} | Total Processed Pages: ${pageCount} | Queue: ${queue.length}/${MAX_QUEUE_SIZE} | Items: ${items.length} | Tracks from Batch: ${batchTracks} | All Total Tracks: ${allTotalTracks} | Subtitle of Batch: ${subtitles} | ThrottleFailRateWindow: ${sc.failRate.toFixed(2)}`
      );

      batchProcessed = 0;
      batchTracks = 0;
      batchDepth = null;
      batchSubtitles.clear();
    }));
  }

  async function loop(browserIndex) {
    const browser = browsers[browserIndex];
    while (true) {
      if (throttled) break;
      const task = await takeTask();
      if (!task) break;

      try {
        const r = await processPageTask(task, browser, state);
        if (r.processed) pageCount += 1;

        globalAttempts += 1;
        throttleTracker.record(!!r.navOk);

        for (const nl of r.links || []) {
          if (!state.visited.has(nl.url) && queue.length < MAX_QUEUE_SIZE) queue.push(nl);
        }

        await addMetrics(r);
        if (pageCount > 0 && pageCount % 10 === 0) await flushMetrics(false);

        if (throttleTracker.isThrottled(globalAttempts) && queue.length > 0) {
          throttled = true;
          const sc = throttleTracker.score();
          console.error(`[${workerLabel}] THROTTLED detected. failRate=${sc.failRate.toFixed(3)} attemptsWindow=${sc.attempts} globalAttempts=${globalAttempts}. Will requeue remaining ${queue.length} tasks.`);
          break;
        }
      } catch (e) {
        console.error(`[${workerLabel}] task error: ${e.message}`);
      }
      await sleep(DELAY_BETWEEN_PAGES);
    }
  }

  const loops = [];
  for (let b = 0; b < MAX_BROWSERS; b++) {
    for (let p = 0; p < PARALLEL_PER_BROWSER; p++) loops.push(loop(b));
  }
  await Promise.all(loops);

  await flushMetrics(true);
  stopHeartbeat();

  return {
    throttled,
    remainingQueue: queue,
    stats: {
      pageCount,
      items: state.itemsByUrl.size,
      globalAttempts,
      throttle: throttleTracker.score(),
    },
  };
}

async function findArtistUrl(browser, artistName) {
  const page = await browser.newPage();
  try {
    const nav = await safeGoto(page, `https://music.apple.com/us/search?term=${encodeURIComponent(artistName)}`);
    if (!nav.ok) return null;

    await scrollUntilExhausted(page, 'vertical');

    return await page.evaluate((name) => {
      const links = document.querySelectorAll('a[href]');
      for (const l of links) {
        const href = l.href;
        const txt = (l.textContent || '').trim();
        if (href?.includes('/artist/') && txt.toLowerCase() === name.toLowerCase()) return href.split('?')[0];
      }
      const first = Array.from(links).find(l => l.href?.includes('/artist/'));
      return first ? first.href.split('?')[0] : null;
    }, artistName);
  } finally {
    await page.close();
  }
}

async function processArtistPage(browser, artistName, artistUrl, visited) {
  const page = await browser.newPage();
  const artistData = {
    name: artistName,
    url: artistUrl,
    type: 'artist',
    creator: artistName,
    metadata: '',
    description: '',
    tracks: [],
    sections: [],
    featuredItems: [],
    subItems: [],
  };

  try {
    const nav = await safeGoto(page, artistUrl);
    if (!nav.ok) return artistData;

    await page.waitForTimeout(1200);
    await scrollUntilExhausted(page, 'vertical');
    await scrollUntilExhausted(page, 'horizontal');

    const x = await extractLinksSectionsAndTracks(page);
    artistData.sections = x.sections || [];
    artistData.featuredItems = x.featuredItems || [];
    artistData.metadata = x.pageMetadata || '';
    artistData.description = x.pageDescription || '';

    const subUrls = new Set();
    for (const sec of artistData.sections) {
      for (const it of sec.items || []) {
        const t = detectType(it.url);
        if (['album', 'single', 'playlist'].includes(t)) {
          if (!visited.has(it.url)) subUrls.add(it.url);
        }
      }
    }

    const toProcess = Array.from(subUrls).slice(0, MAX_SUBPAGES_PER_ARTIST);
    for (const subUrl of toProcess) {
      const sp = await browser.newPage();
      try {
        const subNav = await safeGoto(sp, subUrl);
        if (!subNav.ok) continue;

        await sp.waitForTimeout(900);
        await scrollUntilExhausted(sp, 'vertical');

        const sd = await extractLinksSectionsAndTracks(sp);
        const subType = detectType(subUrl);
        const subSubtitle = sd.pageSubtitle || '';

        if (shouldKeepPage(subType, subSubtitle)) {
          artistData.subItems.push({
            name: sd.pageTitle || subUrl.split('/').pop() || 'Unknown',
            url: subUrl,
            type: subType,
            creator: subSubtitle || '',
            metadata: sd.pageMetadata || '',
            description: sd.pageDescription || '',
            tracks: (sd.tracks || []).slice(0, MAX_TRACKS_PER_ITEM),
          });
        }

        visited.add(subUrl);
      } catch (e) {
        console.error(`subpage error ${subUrl}: ${e.message}`);
      } finally {
        await sp.close();
      }
    }

    return artistData;
  } finally {
    await page.close();
  }
}

function workflowPath(name) { return path.join(WORKFLOW_DIR, name); }
function outputPath(name) { return path.join(OUTPUT_DIR, name); }
function requeuePath(name) { return path.join(REQUEUE_DIR, name); }

function writeWorkflow(name, payload) {
  const file = workflowPath(name);
  saveJson(file, payload);
  return file;
}
function writeOutput(name, items, meta = {}) {
  const unique = dedupeByUrl(items);
  const normalized = unique.map(normalizeOutputItem);
  const payload = {
    lastUpdated: new Date().toISOString(),
    country: 'us',
    totalItems: normalized.length,
    totalTracks: normalized.reduce((s, i) => s + (i.tracks?.length || 0), 0),
    items: normalized,
    ...meta,
  };
  const file = outputPath(name);
  saveJson(file, payload);
  return file;
}
function writeRequeue(workerLabel, queue, wf, stats) {
  const file = requeuePath(`${workerLabel}-requeue.json`);
  saveJson(file, {
    createdAt: new Date().toISOString(),
    workerId: workerLabel,
    originalWorkflow: wf,
    remainingQueue: dedupeByUrl(queue || []),
    stats,
  });
  return file;
}

async function runWorker(workflowFile) {
  ensureDataDirs();
  if (!workflowFile || !fs.existsSync(workflowFile)) throw new Error(`Workflow missing: ${workflowFile}`);
  const wf = loadJson(workflowFile, null);
  if (!wf) throw new Error(`Invalid workflow: ${workflowFile}`);

  const workerLabel = wf.workerId || path.basename(workflowFile);
  const state = { visited: new Set(wf.visited || []), itemsByUrl: new Map() };
  for (const si of wf.seedItems || []) if (si?.url) state.itemsByUrl.set(si.url, normalizeOutputItem(si));

  const stopHeartbeat = startHeartbeat(`worker:${workerLabel}`, () => ({
    kind: wf.kind,
    depth: wf.depth || null,
    items: state.itemsByUrl.size,
    visited: state.visited.size,
  }));

  const pool = await createBrowserPool();
  let throttledExit = false;
  let throttleResult = null;

  try {
    if (wf.kind === 'crawl') {
      throttleResult = await runQueueWithPool(wf.queue || [], state, pool, workerLabel);

      if (throttleResult.throttled) {
        const rq = writeRequeue(workerLabel, throttleResult.remainingQueue, wf, throttleResult.stats);
        console.error(`[${workerLabel}] wrote requeue file -> ${rq}`);
        throttledExit = true;
      }

      if (!throttledExit && wf.includeMandatoryArtists) {
        const b = pool[0];
        for (const a of MANDATORY_ARTISTS) {
          const ad = await processArtistPage(b, a.name, a.url, state.visited);
          state.itemsByUrl.set(ad.url, normalizeOutputItem(ad));
        }
      }
    } else if (wf.kind === 'artist-batch') {
      const artistQueue = [...(wf.artists || [])];
      const stopArtistHeartbeat = startHeartbeat(`artists:${workerLabel}`, () => ({
        remainingArtists: artistQueue.length,
        items: state.itemsByUrl.size,
        visited: state.visited.size,
      }));

      async function artistLoop(browserIndex) {
        const b = pool[browserIndex];
        while (true) {
          const artistName = artistQueue.shift();
          if (!artistName) break;
          try {
            const artistUrl = await findArtistUrl(b, artistName);
            if (!artistUrl) continue;
            const ad = await processArtistPage(b, artistName, artistUrl, state.visited);
            state.itemsByUrl.set(ad.url, normalizeOutputItem(ad));
          } catch (e) {
            console.error(`[${workerLabel}] artist ${artistName} error: ${e.message}`);
          }
        }
      }

      const loops = [];
      for (let b = 0; b < MAX_BROWSERS; b++) {
        for (let p = 0; p < PARALLEL_PER_BROWSER; p++) loops.push(artistLoop(b));
      }
      await Promise.all(loops);
      stopArtistHeartbeat();
    }
  } finally {
    await closeBrowserPool(pool);
    stopHeartbeat();
  }

  const items = Array.from(state.itemsByUrl.values());
  const outFile = writeOutput(wf.outputName, items, {
    phase: wf.phase || 'worker',
    workerId: workerLabel,
    kind: wf.kind,
    depth: wf.depth || null,
    throttled: throttledExit,
    throttleStats: throttleResult?.stats || null,
  });

  if (!throttledExit && wf.emitNextDepthLinksFile) {
    const next = [];
    for (const it of items) {
      for (const sec of it.sections || []) {
        for (const si of sec.items || []) {
          const t = detectType(si.url);
          if (['artist', 'album', 'single', 'playlist', 'chart', 'room'].includes(t)) {
            // emit clamped depth so depth2 stays depth2 targets
            next.push({ url: si.url, depth: Math.min(MAX_DEPTH, 2) });
          }
        }
      }
      for (const fi of it.featuredItems || []) {
        const t = detectType(fi.url);
        if (['artist', 'album', 'single', 'playlist', 'chart', 'room'].includes(t)) {
          next.push({ url: fi.url, depth: Math.min(MAX_DEPTH, 2) });
        }
      }
    }
    saveJson(wf.emitNextDepthLinksFile, {
      generatedAt: new Date().toISOString(),
      from: workerLabel,
      links: dedupeByUrl(next),
    });
  }

  console.log(`[${workerLabel}] done -> ${outFile}`);

  if (throttledExit) {
    process.exit(THROTTLE_REQUEUE_EXIT_CODE);
  }

  return outFile;
}

function spawnNode(childArgs, logFile) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [__filename, ...childArgs], {
      cwd: ROOT_DIR,
      stdio: ['ignore', 'pipe', 'pipe'],
      env: process.env,
    });

    const log = fs.createWriteStream(logFile, { flags: 'a' });

    child.stdout.on('data', (d) => {
      const s = d.toString();
      process.stdout.write(s);
      log.write(s);
    });

    child.stderr.on('data', (d) => {
      const s = d.toString();
      process.stderr.write(s);
      log.write(s);
    });

    child.on('error', (err) => {
      log.end();
      reject(err);
    });

    child.on('close', (code) => {
      log.end();
      if (code === 0 || code === THROTTLE_REQUEUE_EXIT_CODE) resolve({ code });
      else reject(new Error(`Child failed (${code}): ${childArgs.join(' ')}`));
    });
  });
}

async function runMerge(inputGlob, outFile) {
  ensureDataDirs();
  const files = glob.sync(inputGlob).filter(f => path.resolve(f) !== path.resolve(outFile));
  const byUrl = new Map();

  for (const f of files) {
    const d = loadJson(f, null);
    if (!d?.items) continue;
    for (const it of d.items) {
      if (!it?.url) continue;
      const n = normalizeOutputItem(it);
      if (!byUrl.has(n.url)) byUrl.set(n.url, n);
      else {
        const cur = byUrl.get(n.url);
        byUrl.set(n.url, {
          ...cur, ...n,
          tracks: (n.tracks.length > cur.tracks.length) ? n.tracks : cur.tracks,
          sections: (n.sections.length > cur.sections.length) ? n.sections : cur.sections,
          featuredItems: (n.featuredItems.length > cur.featuredItems.length) ? n.featuredItems : cur.featuredItems,
          subItems: (n.subItems.length > cur.subItems.length) ? n.subItems : cur.subItems,
        });
      }
    }
  }

  const items = Array.from(byUrl.values());
  const merged = {
    lastUpdated: new Date().toISOString(),
    country: 'us',
    phase: 'merged',
    totalFiles: files.length,
    totalItems: items.length,
    totalTracks: items.reduce((s, i) => s + (i.tracks?.length || 0), 0),
    items,
    sourceFiles: files.map(f => path.basename(f)),
  };
  saveJson(outFile, merged);
  console.log(`[merge] ${files.length} files -> ${outFile} (${items.length} items)`);
  return merged;
}

function buildRequeueWorkflows() {
  const files = glob.sync(path.join(REQUEUE_DIR, '*-requeue.json'));
  const workflows = [];

  for (const f of files) {
    const rq = loadJson(f, null);
    if (!rq?.remainingQueue?.length) continue;

    const wid = `${rq.workerId}-retry-${Date.now()}`;
    const wfName = `${wid}.json`;
    const outName = `${wid}.json`;

    workflows.push(writeWorkflow(wfName, {
      kind: 'crawl',
      phase: 'requeue',
      workerId: wid,
      depth: 2,
      queue: rq.remainingQueue.map(x => ({ url: x.url, depth: 2 })),
      outputName: outName,
    }));
  }
  return workflows;
}

async function runOrchestrator() {
  ensureDataDirs();

  console.log('========================================');
  console.log('Apple Music Orchestrator');
  console.log(`Per worker: ${MAX_BROWSERS} browsers, ${PARALLEL_PER_BROWSER} per browser (${TOTAL_TASK_LOOPS} loops)`);
  console.log('Flow: 3x depth1 -> 9x depth2 -> requeue retries -> merge -> artist workers -> final merge');
  console.log('========================================');

  const totalWorkers = DEPTH1_SPLITS + (DEPTH1_SPLITS * DEPTH2_SPLITS_PER_DEPTH1) + ARTIST_WORKERS;
  const orchestratorState = { stage: 'starting', completedWorkers: 0, totalWorkers, requeueWorkers: 0 };
  const stopHeartbeat = startHeartbeat('orchestrator', () => orchestratorState);

  // 1) Depth1
  orchestratorState.stage = 'depth1';
  const seedSplits = splitArray(SEED_URLS.map(url => ({ url, depth: 1 })), DEPTH1_SPLITS);
  const d1wfs = [];

  for (let i = 0; i < DEPTH1_SPLITS; i++) {
    d1wfs.push(writeWorkflow(`depth1-w${i + 1}.json`, {
      kind: 'crawl',
      phase: 'depth1',
      workerId: `depth1-w${i + 1}`,
      depth: 1,
      queue: seedSplits[i],
      includeMandatoryArtists: i === 0,
      outputName: `us-depth1-w${i + 1}.json`,
      emitNextDepthLinksFile: workflowPath(`depth1-w${i + 1}-next-links.json`),
    }));
  }

  await Promise.all(d1wfs.map((wf, i) =>
    spawnNode(['--mode', 'worker', '--workflow', wf], path.join(LOG_DIR, `depth1-w${i + 1}.log`))
      .then(() => { orchestratorState.completedWorkers += 1; })
  ));

  // 2) Depth2 (9 workers)
  orchestratorState.stage = 'depth2';
  const d2wfs = [];
  for (let i = 0; i < DEPTH1_SPLITS; i++) {
    const d1next = loadJson(workflowPath(`depth1-w${i + 1}-next-links.json`), { links: [] });
    const d2candidates = dedupeByUrl((d1next.links || []).map(l => ({ url: l.url, depth: 2 })).filter(x => x.url));
    const d2splits = splitArray(d2candidates, DEPTH2_SPLITS_PER_DEPTH1);

    for (let j = 0; j < DEPTH2_SPLITS_PER_DEPTH1; j++) {
      d2wfs.push(writeWorkflow(`depth2-w${i + 1}-${j + 1}.json`, {
        kind: 'crawl',
        phase: 'depth2',
        workerId: `depth2-w${i + 1}-${j + 1}`,
        depth: 2,
        queue: d2splits[j],
        outputName: `us-depth2-w${i + 1}-${j + 1}.json`,
      }));
    }
  }

  await Promise.all(d2wfs.map((wf) => {
    const id = path.basename(wf, '.json');
    return spawnNode(['--mode', 'worker', '--workflow', wf], path.join(LOG_DIR, `${id}.log`))
      .then(() => { orchestratorState.completedWorkers += 1; });
  }));

  // 2.5) Requeue retry pass (new workers for throttled leftovers)
  orchestratorState.stage = 'requeue';
  const rqWorkflows = buildRequeueWorkflows();
  orchestratorState.requeueWorkers = rqWorkflows.length;

  if (rqWorkflows.length) {
    console.log(`[orchestrator] requeue workers: ${rqWorkflows.length}`);
    await Promise.all(rqWorkflows.map((wf) => {
      const id = path.basename(wf, '.json');
      return spawnNode(['--mode', 'worker', '--workflow', wf], path.join(LOG_DIR, `${id}.log`))
        .then(() => { orchestratorState.completedWorkers += 1; });
    }));
  }

  // 3) Merge crawl
  orchestratorState.stage = 'merge-crawl';
  const crawlMergedFile = path.join(DATA_DIR, 'us-crawl-only.json');
  await runMerge(path.join(OUTPUT_DIR, 'us-depth*.json'), crawlMergedFile);

  // include requeue outputs in crawl-only merge as well
  const requeueMergedFile = path.join(DATA_DIR, 'us-crawl-requeue-only.json');
  await runMerge(path.join(OUTPUT_DIR, '*retry-*.json'), requeueMergedFile);

  // 4) Artists
  orchestratorState.stage = 'artists';
  const artistSplits = splitArray(TOP_ARTISTS, ARTIST_WORKERS);
  const artistWfs = [];

  for (let i = 0; i < ARTIST_WORKERS; i++) {
    artistWfs.push(writeWorkflow(`artists-w${i + 1}.json`, {
      kind: 'artist-batch',
      phase: 'artists',
      workerId: `artists-w${i + 1}`,
      artists: artistSplits[i],
      outputName: `us-artists-w${i + 1}.json`,
    }));
  }

  await Promise.all(artistWfs.map((wf, i) =>
    spawnNode(['--mode', 'worker', '--workflow', wf], path.join(LOG_DIR, `artists-w${i + 1}.log`))
      .then(() => { orchestratorState.completedWorkers += 1; })
  ));

  // 5) Final merge
  orchestratorState.stage = 'merge-final';
  await runMerge(path.join(OUTPUT_DIR, '*.json'), FINAL_OUTPUT);

  orchestratorState.stage = 'done';
  stopHeartbeat();
  console.log(`Done. Final output: ${FINAL_OUTPUT}`);
}

async function main() {
  try {
    if (MODE === 'orchestrator') return runOrchestrator();
    if (MODE === 'worker') return runWorker(WORKFLOW_FILE);
    if (MODE === 'merge') return runMerge(MERGE_GLOB, FINAL_OUTPUT);
    throw new Error(`Unknown --mode ${MODE}`);
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
}

main();
