import fs from "node:fs/promises";
import path from "node:path";

const DEFAULT_BASE_URL = "https://edge-api-layer-aura-prod.stagingleem.workers.dev";
const DEFAULT_REGION = "ae";
const DEFAULT_LANGUAGE = "en";
const DEFAULT_CONCURRENCY = 6;

const inputPath = process.argv[2] || "decor.json";
const baseUrl = process.env.BASE_URL || DEFAULT_BASE_URL;
const region = (process.env.REGION || DEFAULT_REGION).toLowerCase();
const language = (process.env.LANGUAGE || DEFAULT_LANGUAGE).toLowerCase();
const concurrency = Number.parseInt(process.env.CONCURRENCY || "", 10) || DEFAULT_CONCURRENCY;
const delayMs = Number.parseInt(process.env.DELAY_MS || "", 10) || 0;
const dryRun = process.env.DRY_RUN === "1";

const headers = {
  accept: "*/*",
  "accept-language": "en-US,en;q=0.9",
  "cache-control": "no-cache",
  origin: "https://www.auraliving.com",
  pragma: "no-cache",
  priority: "u=1, i",
  referer: "https://www.auraliving.com/",
  "sec-ch-ua": '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
  "sec-ch-ua-mobile": "?0",
  "sec-ch-ua-platform": '"macOS"',
  "sec-fetch-dest": "empty",
  "sec-fetch-mode": "cors",
  "sec-fetch-site": "cross-site",
  "user-agent":
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
  "x-fbc": process.env.X_FBC || "undefined",
  "x-fbp": process.env.X_FBP || "fb.1.1766401536269.750652419898527328",
};

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function loadSlugs(filePath) {
  const resolved = path.resolve(process.cwd(), filePath);
  const raw = await fs.readFile(resolved, "utf8");
  const data = JSON.parse(raw);
  const products = Array.isArray(data.products) ? data.products : [];
  const slugs = products
    .map((item) => String(item.slug || "").trim())
    .filter(Boolean);
  return Array.from(new Set(slugs));
}

async function runPool(items, poolSize, worker) {
  let index = 0;
  const results = [];
  const runners = Array.from({ length: poolSize }, async () => {
    while (index < items.length) {
      const currentIndex = index;
      index += 1;
      results[currentIndex] = await worker(items[currentIndex], currentIndex);
    }
  });
  await Promise.all(runners);
  return results;
}

async function fetchPdp(slug, idx) {
  const url = new URL(`/products/${encodeURIComponent(slug)}`, baseUrl);
  url.searchParams.set("region", region);
  url.searchParams.set("language", language);

  if (dryRun) {
    console.log(`[dry-run] ${idx + 1} ${url.toString()}`);
    return { slug, ok: true, status: 0 };
  }

  const res = await fetch(url, { headers, method: "GET" });
  const ok = res.ok;
  console.log(`${idx + 1} ${ok ? "OK" : "FAIL"} ${res.status} ${slug}`);

  if (delayMs > 0) {
    await sleep(delayMs);
  }

  return { slug, ok, status: res.status };
}

const slugs = await loadSlugs(inputPath);
if (slugs.length === 0) {
  console.error(`No slugs found in ${inputPath}`);
  process.exit(1);
}

console.log(
  `Prewarming ${slugs.length} PDPs with concurrency=${concurrency} region=${region} language=${language}`
);

const results = await runPool(slugs, Math.max(1, concurrency), fetchPdp);
const okCount = results.filter((r) => r && r.ok).length;
const failCount = results.length - okCount;

console.log(`Done. ok=${okCount} failed=${failCount}`);
if (failCount > 0) {
  process.exitCode = 1;
}
