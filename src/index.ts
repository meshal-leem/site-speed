/**
 * Interface for Environment Variables
 * Ensures type safety when accessing variables defined in wrangler.toml
 */
interface Env {
  BACKEND_PRODUCT_URL: string;
  BACKEND_PRODUCT_LIST_URL: string;
  BACKEND_CATEGORY_TREE_URL: string;
  ALLOWED_ORIGIN: string;
  ADMIN_USER: string;
  ADMIN_PASS: string;
  WEBHOOK_TOKEN: string;
  CACHE_META: KVNamespace;
  VISIT_COUNTER: DurableObjectNamespace;
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    try {
      if (url.pathname === "/webhook/sync-all" && request.method === "POST") {
        const authError = requireWebhookAuth(request, env);
        if (authError) {
          return authError;
        }
        const syncPromise = syncAllCache(env, ctx, null);
        ctx.waitUntil(syncPromise);
        return jsonResponse({ status: "started" }, 202);
      }

      if (url.pathname.startsWith("/admin")) {
        const authError = requireAdminAuth(request, env);
        if (authError) {
          return authError;
        }
        return await handleAdminRequest(request, env, ctx);
      }

      // 1. Handle CORS preflight for browser calls
      if (request.method === "OPTIONS") {
        return handleOptions(env, request);
      }

      // 2. Route Matching
      if (request.method === "GET") {
        
        // Match exact "/products" or "/products/"
        if (url.pathname === "/products" || url.pathname === "/products/") {
          return await handleProductListRequest(request, env, ctx);
        }

        // Match "/products/:slug" using Regex
        // This captures the slug and ensures we don't match deep paths like /products/123/extra
        const slugMatch = url.pathname.match(/^\/products\/([^\/]+)\/?$/);
        if (slugMatch) {
          const slug = slugMatch[1]; // The captured slug
          return await handleProductRequest(request, env, ctx, slug);
        }
      }

      if (request.method === "POST") {
        if (url.pathname === "/products" || url.pathname === "/products/") {
          return await handleProductListPostRequest(request, env, ctx);
        }

        const slugMatch = url.pathname.match(/^\/products\/([^\/]+)\/?$/);
        if (slugMatch) {
          const slug = slugMatch[1];
          return await handleProductPostRequest(request, env, ctx, slug);
        }
      }

      // 3. 404 Not Found for unmatched routes
      return new Response(JSON.stringify({ error: "Not found" }), {
        status: 404,
        headers: { "Content-Type": "application/json" },
      });

    } catch (err: any) {
      // 4. Global Error Trap (Safety Net)
      console.error(`Unhandled Worker Error: ${err.message}`, err.stack);
      return new Response(JSON.stringify({ error: "Internal Server Error" }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
    }
  },
};

export class VisitCounter {
  private state: DurableObjectState;

  constructor(state: DurableObjectState) {
    this.state = state;
  }

  async fetch(request: Request): Promise<Response> {
    const url = new URL(request.url);
    const key = "count";
    if (request.method === "POST" && url.pathname === "/increment") {
      const current = (await this.state.storage.get<number>(key)) ?? 0;
      const next = current + 1;
      await this.state.storage.put(key, next);
      return jsonResponse({ count: next }, 200);
    }
    if (request.method === "GET" && url.pathname === "/count") {
      const current = (await this.state.storage.get<number>(key)) ?? 0;
      return jsonResponse({ count: current }, 200);
    }
    return new Response("Not found", { status: 404 });
  }
}

/**
 * Handle OPTIONS requests for CORS
 */
function handleOptions(env: Env, request: Request): Response {
  const headers: Record<string, string> = {
    "access-control-allow-methods": "GET, OPTIONS",
    "access-control-allow-headers": "Content-Type, X-FBC, X-FBP",
    "access-control-max-age": "86400",
  };

  const origin = request.headers.get("origin");
  if (origin && origin === env.ALLOWED_ORIGIN) {
    headers["access-control-allow-origin"] = origin;
    headers["vary"] = "Origin"; // Important for caching
  }

  return new Response(null, {
    status: 204,
    headers,
  });
}

async function handleProductPostRequest(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
  slug: string
): Promise<Response> {
  const url = new URL(request.url);
  const body = await parseJsonBody(request);
  if (!body) {
    return new Response(JSON.stringify({ error: "Invalid JSON body" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }
  const requestBodyHash = await hashRequestBody(body);

  const region = (String(body.region || url.searchParams.get("region") || "sa")).toLowerCase();
  const language = (String(body.language || url.searchParams.get("language") || "en")).toLowerCase();

  const cacheKeyUrl = `${url.origin}/products/${slug}?region=${region}&language=${language}`;
  const cacheKey = new Request(cacheKeyUrl, { method: "GET" });
  const cache = caches.default;
  ctx.waitUntil(incrementVisit(env, "pdp", cacheKeyUrl));

  let cachedResponse = await cache.match(cacheKey);
  if (cachedResponse) {
    const res = new Response(cachedResponse.body, cachedResponse);
    res.headers.set("x-edge-cache", "HIT");
    addCorsHeaders(res, env, request);
    return res;
  }

  try {
    const backendResponse = await fetchPdpBackend(env, request, slug, region, language);
    if (!backendResponse.ok) {
      console.error(`Backend returned ${backendResponse.status} for slug: ${slug}`);
      return new Response(backendResponse.body, {
        status: backendResponse.status,
        headers: backendResponse.headers
      });
    }

    const textBody = await backendResponse.text();
    const edgeResponse = new Response(textBody, {
      status: backendResponse.status,
      headers: {
        "content-type": backendResponse.headers.get("content-type") || "application/json",
        "cache-control": "public, max-age=31536000, stale-while-revalidate=31536000",
      },
    });

    edgeResponse.headers.set("x-edge-cache", "MISS");
    addCorsHeaders(edgeResponse, env, request);

    ctx.waitUntil(cache.put(cacheKey, edgeResponse.clone()));
    ctx.waitUntil(recordCacheMeta(env, "pdp", cacheKeyUrl, {
      synced: false,
      source: "post",
      requestBodyHash,
    }));

    return edgeResponse;
  } catch (error) {
    console.error(`Fetch failed for PDP ${slug}:`, error);
    return new Response(JSON.stringify({ error: "Service Unavailable" }), {
      status: 502,
      headers: { "Content-Type": "application/json" }
    });
  }
}

async function handleProductListPostRequest(
  request: Request,
  env: Env,
  ctx: ExecutionContext
): Promise<Response> {
  const url = new URL(request.url);
  const body = await parseJsonBody(request);
  if (!body) {
    return new Response(JSON.stringify({ error: "Invalid JSON body" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
  }
  const requestBodyHash = await hashRequestBody(body);

  const backendBody = normalizePlpBody(body);
  const cacheKeyUrl = buildPlpCacheKeyUrl(url.origin, backendBody);
  const cacheKey = new Request(cacheKeyUrl, { method: "GET" });
  const cache = caches.default;
  ctx.waitUntil(incrementVisit(env, "plp", cacheKeyUrl));

  let cachedResponse = await cache.match(cacheKey);
  if (cachedResponse) {
    const res = new Response(cachedResponse.body, cachedResponse);
    res.headers.set("x-edge-cache", "HIT");
    addCorsHeaders(res, env, request);
    return res;
  }

  const incomingHeaders = request.headers;
  const backendHeaders: Record<string, string> = {
    "content-type": "application/json",
    "accept": "application/json",
  };

  const xFbc = incomingHeaders.get("x-fbc");
  const xFbp = incomingHeaders.get("x-fbp");
  if (xFbc) backendHeaders["x-fbc"] = xFbc;
  if (xFbp) backendHeaders["x-fbp"] = xFbp;

  try {
    const debug = request.headers.get("x-prewarm-debug") === "1";
    const backendResponse = await fetchPlpBackend(env, request, backendBody, backendHeaders, {
      debug,
    });
    if (!backendResponse.ok) {
      console.error(`Backend list returned ${backendResponse.status}`);
      return new Response(backendResponse.body, {
        status: backendResponse.status,
        headers: backendResponse.headers
      });
    }

    const textBody = await backendResponse.text();
    const edgeResponse = new Response(textBody, {
      status: backendResponse.status,
      headers: {
        "content-type": backendResponse.headers.get("content-type") || "application/json",
        "cache-control": "public, max-age=31536000, stale-while-revalidate=31536000",
      },
    });

    edgeResponse.headers.set("x-edge-cache", "MISS");
    addCorsHeaders(edgeResponse, env, request);

    ctx.waitUntil(cache.put(cacheKey, edgeResponse.clone()));
    ctx.waitUntil(recordCacheMeta(env, "plp", cacheKeyUrl, {
      synced: false,
      source: "post",
      requestBodyHash,
    }));

    return edgeResponse;
  } catch (error) {
    console.error(`Fetch failed for PLP:`, error);
    return new Response(JSON.stringify({ error: "Service Unavailable" }), {
      status: 502,
      headers: { "Content-Type": "application/json" }
    });
  }
}

/**
 * PDP handler: GET /products/:slug
 */
async function handleProductRequest(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
  slug: string
): Promise<Response> {
  const url = new URL(request.url);

  const region = (url.searchParams.get("region") || "sa").toLowerCase();
  const language = (url.searchParams.get("language") || "en").toLowerCase();

  // Create a cache key specific to this request variation
  const cacheKeyUrl = `${url.origin}/products/${slug}?region=${region}&language=${language}`;
  const cacheKey = new Request(cacheKeyUrl, { method: "GET" });
  const cache = caches.default;
  ctx.waitUntil(incrementVisit(env, "pdp", cacheKeyUrl));

  // 1. Try Cache
  let cachedResponse = await cache.match(cacheKey);
  if (cachedResponse) {
    const res = new Response(cachedResponse.body, cachedResponse);
    res.headers.set("x-edge-cache", "HIT");
    addCorsHeaders(res, env, request);
    return res;
  }

  // 2. Prepare Backend Request
  try {
    const backendResponse = await fetchPdpBackend(env, request, slug, region, language);

    // 3. Handle Backend Errors
    // If backend returns 4xx/5xx, we typically want to pass that through but NOT cache it aggressively
    // or strictly avoid caching errors depending on requirements.
    if (!backendResponse.ok) {
        console.error(`Backend returned ${backendResponse.status} for slug: ${slug}`);
        // We clone here because we might read the body or pass it through
        return new Response(backendResponse.body, {
            status: backendResponse.status,
            headers: backendResponse.headers
        });
    }

    const textBody = await backendResponse.text();

    const edgeResponse = new Response(textBody, {
      status: backendResponse.status,
      headers: {
        "content-type": backendResponse.headers.get("content-type") || "application/json",
        "cache-control": "public, max-age=31536000, stale-while-revalidate=31536000",
      },
    });

    edgeResponse.headers.set("x-edge-cache", "MISS");
    addCorsHeaders(edgeResponse, env, request);

    // 4. Update Cache (Non-blocking)
    ctx.waitUntil(cache.put(cacheKey, edgeResponse.clone()));
    ctx.waitUntil(recordCacheMeta(env, "pdp", cacheKeyUrl, { synced: false, source: "get" }));

    return edgeResponse;

  } catch (error) {
    console.error(`Fetch failed for PDP ${slug}:`, error);
    return new Response(JSON.stringify({ error: "Service Unavailable" }), {
        status: 502,
        headers: { "Content-Type": "application/json" }
    });
  }
}

/**
 * PLP handler: GET /products
 */
async function handleProductListRequest(
  request: Request,
  env: Env,
  ctx: ExecutionContext
): Promise<Response> {
  const url = new URL(request.url);
  const cache = caches.default;

  // Normalize cache key so equivalent query params don't create duplicates.
  const backendBody = buildPlpBodyFromUrl(url);
  const cacheKeyUrl = buildPlpCacheKeyUrl(url.origin, backendBody);
  const cacheKey = new Request(cacheKeyUrl, { method: "GET" });
  ctx.waitUntil(incrementVisit(env, "plp", cacheKeyUrl));

  // 1. Try Cache
  let cachedResponse = await cache.match(cacheKey);
  if (cachedResponse) {
    const res = new Response(cachedResponse.body, cachedResponse);
    res.headers.set("x-edge-cache", "HIT");
    addCorsHeaders(res, env, request);
    return res;
  }

  const incomingHeaders = request.headers;
  const backendHeaders: Record<string, string> = {
    "content-type": "application/json",
    "accept": "application/json",
  };
  
  const xFbc = incomingHeaders.get("x-fbc");
  const xFbp = incomingHeaders.get("x-fbp");
  if (xFbc) backendHeaders["x-fbc"] = xFbc;
  if (xFbp) backendHeaders["x-fbp"] = xFbp;

  try {
    const backendResponse = await fetchPlpBackend(env, request, backendBody, backendHeaders);

    if (!backendResponse.ok) {
        console.error(`Backend list returned ${backendResponse.status}`);
        return new Response(backendResponse.body, {
            status: backendResponse.status,
            headers: backendResponse.headers
        });
    }

    const textBody = await backendResponse.text();

    const edgeResponse = new Response(textBody, {
      status: backendResponse.status,
      headers: {
        "content-type": backendResponse.headers.get("content-type") || "application/json",
        "cache-control": "public, max-age=31536000, stale-while-revalidate=31536000",
      },
    });

    edgeResponse.headers.set("x-edge-cache", "MISS");
    addCorsHeaders(edgeResponse, env, request);

    ctx.waitUntil(cache.put(cacheKey, edgeResponse.clone()));
    ctx.waitUntil(recordCacheMeta(env, "plp", cacheKeyUrl, { synced: false, source: "get" }));

    return edgeResponse;

  } catch (error) {
    console.error(`Fetch failed for PLP:`, error);
    return new Response(JSON.stringify({ error: "Service Unavailable" }), {
        status: 502,
        headers: { "Content-Type": "application/json" }
    });
  }
}

function addCorsHeaders(response: Response, env: Env, request: Request) {
  const origin = request.headers.get("origin");
  if (origin && origin === env.ALLOWED_ORIGIN) {
    response.headers.set("access-control-allow-origin", origin);
    response.headers.set("vary", "Origin");
  }
  response.headers.set("access-control-allow-credentials", "true");
}

type CacheType = "plp" | "pdp";

interface CacheMeta {
  id: string;
  type: CacheType;
  cacheKeyUrl: string;
  createdAt: string;
  updatedAt: string;
  lastSyncedAt?: string | null;
  source?: "get" | "post";
  requestBodyHash?: string | null;
  visits?: number;
}

interface PrewarmJob {
  id: string;
  status: "running" | "completed" | "failed";
  startedAt: string;
  finishedAt?: string | null;
  processed: number;
  total: number;
  errors: number;
  regions: string[];
  languages: string[];
  categories: string[];
  regionIndex: number;
  languageIndex: number;
  categoryIndex: number;
  nextPage: number;
  lastPage: number;
  maxPages: number | null;
  start: number;
  limit: number;
  category?: string | null;
  message?: string | null;
}

function requireAdminAuth(request: Request, env: Env): Response | null {
  const authHeader = request.headers.get("authorization");
  if (!authHeader || !authHeader.startsWith("Basic ")) {
    return unauthorizedResponse();
  }

  const decoded = atob(authHeader.slice(6));
  const separator = decoded.indexOf(":");
  if (separator === -1) {
    return unauthorizedResponse();
  }

  const user = decoded.slice(0, separator);
  const pass = decoded.slice(separator + 1);
  if (user !== env.ADMIN_USER || pass !== env.ADMIN_PASS) {
    return unauthorizedResponse();
  }

  return null;
}

function unauthorizedResponse(): Response {
  return new Response("Unauthorized", {
    status: 401,
    headers: {
      "www-authenticate": 'Basic realm="Admin"',
      "cache-control": "no-store",
    },
  });
}

function requireWebhookAuth(request: Request, env: Env): Response | null {
  const tokenParam = new URL(request.url).searchParams.get("token");
  const tokenHeader = request.headers.get("x-webhook-token");
  const token = tokenHeader || tokenParam;
  if (!token || token !== env.WEBHOOK_TOKEN) {
    return new Response("Unauthorized", { status: 401 });
  }
  return null;
}

async function handleAdminRequest(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  const url = new URL(request.url);
  const pathname = url.pathname;

  if (pathname === "/admin" || pathname === "/admin/") {
    return new Response(getAdminHtml(), {
      headers: {
        "content-type": "text/html; charset=utf-8",
        "cache-control": "no-store",
      },
    });
  }

  if (pathname === "/admin/cache" && request.method === "GET") {
    const type = (url.searchParams.get("type") || "all").toLowerCase();
    const list = await listCacheMeta(env, type === "all" ? null : (type as CacheType));
    const items = await Promise.all(list.map(async (item) => {
      const visits = await getVisitCount(env, item.type, item.cacheKeyUrl);
      return { ...item, visits };
    }));
    return jsonResponse({ items }, 200);
  }

  if (pathname === "/admin/cache/item" && request.method === "GET") {
    const id = url.searchParams.get("id");
    if (!id) {
      return jsonResponse({ error: "Missing id" }, 400);
    }
    const meta = await getCacheMeta(env, id);
    if (!meta) {
      return jsonResponse({ error: "Not found" }, 404);
    }

    const cacheResult = await getCachedResponse(meta.cacheKeyUrl);
    const freshResult = await getFreshResponseForMeta(env, meta);

    return jsonResponse({
      meta,
      cache: cacheResult,
      fresh: freshResult,
    }, 200);
  }

  if (pathname === "/admin/cache/sync" && request.method === "POST") {
    const id = url.searchParams.get("id");
    if (!id) {
      return jsonResponse({ error: "Missing id" }, 400);
    }
    const meta = await getCacheMeta(env, id);
    if (!meta) {
      return jsonResponse({ error: "Not found" }, 404);
    }

    const freshResult = await syncCacheForMeta(env, ctx, meta);
    return jsonResponse(freshResult, 200);
  }

  if (pathname === "/admin/cache/sync-all" && request.method === "POST") {
    const type = (url.searchParams.get("type") || "all").toLowerCase();
    const syncPromise = syncAllCache(env, ctx, type === "all" ? null : (type as CacheType));
    ctx.waitUntil(syncPromise);
    return jsonResponse({ status: "started" }, 202);
  }

  if (pathname === "/admin/cache/clear" && request.method === "POST") {
    const type = (url.searchParams.get("type") || "all").toLowerCase();
    const cleared = await clearCacheByType(env, ctx, type === "all" ? null : (type as CacheType));
    return jsonResponse({ cleared }, 200);
  }

  if (pathname === "/admin/prewarm" && request.method === "POST") {
    const regionParam = url.searchParams.get("regions");
    const regionSingle = url.searchParams.get("region");
    const languageParam = url.searchParams.get("languages");
    const languageSingle = url.searchParams.get("language");
    const categoryParam = url.searchParams.get("category");
    const maxPagesParam = url.searchParams.get("maxPages");
    const maxPages = maxPagesParam ? parseInt(maxPagesParam, 10) : null;
    const startParam = url.searchParams.get("start");
    const limitParam = url.searchParams.get("limit");
    const start = startParam ? Math.max(0, parseInt(startParam, 10)) : 0;
    const limit = limitParam ? Math.max(1, parseInt(limitParam, 10)) : 25;

    const regions = regionSingle
      ? [regionSingle.trim().toLowerCase()]
      : regionParam
        ? regionParam.split(",").map((r) => r.trim().toLowerCase()).filter(Boolean)
        : getDefaultRegions(env);
    const languages = languageSingle
      ? [languageSingle.trim().toLowerCase()]
      : languageParam
        ? languageParam.split(",").map((l) => l.trim().toLowerCase()).filter(Boolean)
        : ["en", "ar"];

    const categories = await getPrewarmCategories(env, categoryParam, start, limit);
    const jobId = await createPrewarmJob(env, regions, languages, categories, maxPages, start, limit, categoryParam);
    const job = runPrewarmBatch(env, request, jobId, 1);
    ctx.waitUntil(job);

    return jsonResponse({
      status: "started",
      jobId,
      regions,
      languages,
      maxPages,
      start,
      limit,
      category: categoryParam || null,
    }, 202);
  }

  if (pathname === "/admin/prewarm/continue" && request.method === "POST") {
    const id = url.searchParams.get("id");
    if (!id) {
      return jsonResponse({ error: "Missing id" }, 400);
    }
    const batchParam = url.searchParams.get("batch");
    const batch = batchParam ? Math.max(1, parseInt(batchParam, 10)) : 1;
    const job = await runPrewarmBatch(env, request, id, batch);
    if (!job) {
      return jsonResponse({ error: "Not found" }, 404);
    }
    return jsonResponse(job, 200);
  }

  if (pathname === "/admin/prewarm/status" && request.method === "GET") {
    const id = url.searchParams.get("id");
    if (!id) {
      return jsonResponse({ error: "Missing id" }, 400);
    }
    const job = await getPrewarmJob(env, id);
    if (!job) {
      return jsonResponse({ error: "Not found" }, 404);
    }
    return jsonResponse(job, 200);
  }

  if (pathname === "/admin/prewarm/list" && request.method === "GET") {
    const jobs = await listPrewarmJobs(env);
    return jsonResponse({ items: jobs }, 200);
  }

  if (pathname === "/admin/categories" && request.method === "GET") {
    const categories = await fetchCategoryTree(env);
    const items = categories
      .filter((cat) => typeof cat.value === "string")
      .map((cat) => ({
        name: typeof cat.name === "string" ? cat.name : cat.value,
        value: cat.value,
        id: typeof cat.id === "string" ? cat.id : null,
      }));
    return jsonResponse({ items }, 200);
  }

  if (pathname === "/admin/prewarm/clear" && request.method === "POST") {
    const status = url.searchParams.get("status");
    const cleared = await clearPrewarmJobs(env, status);
    return jsonResponse({ cleared }, 200);
  }

  return jsonResponse({ error: "Not found" }, 404);
}

function jsonResponse(body: unknown, status: number): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "content-type": "application/json",
      "cache-control": "no-store",
    },
  });
}

async function fetchPdpBackend(
  env: Env,
  request: Request,
  slug: string,
  region: string,
  language: string,
  options: { noCache?: boolean } = {}
): Promise<Response> {
  const backendBody = { slug, region, language };
  const incomingHeaders = request.headers;

  const backendHeaders: Record<string, string> = {
    "content-type": "application/json",
    "accept": "application/json",
  };

  const xFbc = incomingHeaders.get("x-fbc");
  const xFbp = incomingHeaders.get("x-fbp");
  if (xFbc) backendHeaders["x-fbc"] = xFbc;
  if (xFbp) backendHeaders["x-fbp"] = xFbp;

  return fetch(env.BACKEND_PRODUCT_URL, {
    method: "POST",
    headers: backendHeaders,
    body: JSON.stringify(backendBody),
    cache: options.noCache ? "no-store" : undefined,
  });
}

async function fetchPlpBackend(
  env: Env,
  request: Request,
  backendBody: unknown,
  backendHeaders: Record<string, string>,
  options: { noCache?: boolean; debug?: boolean } = {}
): Promise<Response> {
  if (options.debug) {
    console.log("Backend PLP request", {
      url: env.BACKEND_PRODUCT_LIST_URL,
      body: backendBody,
    });
  }

  const response = await fetch(env.BACKEND_PRODUCT_LIST_URL, {
    method: "POST",
    headers: backendHeaders,
    body: JSON.stringify(backendBody),
    cache: options.noCache ? "no-store" : undefined,
  });

  if (options.debug) {
    try {
      const text = await response.clone().text();
      console.log("Backend PLP response", {
        status: response.status,
        body: text.slice(0, 500),
      });
    } catch (error) {
      console.log("Backend PLP response read failed", { error: String(error) });
    }
  }

  return response;
}

function getDefaultRegions(env: Env): string[] {
  if (env.BACKEND_PRODUCT_URL.includes("api.auraliving.com")) {
    return ["ae", "sa"];
  }
  return ["ae", "sa", "uk", "int"];
}

async function recordCacheMeta(
  env: Env,
  type: CacheType,
  cacheKeyUrl: string,
  options: { synced: boolean; source?: "get" | "post"; requestBodyHash?: string | null }
): Promise<void> {
  const id = await hashCacheKey(type, cacheKeyUrl);
  const key = `${type}:${id}`;
  const now = formatUaeNow();
  const existing = await env.CACHE_META.get(key, "json") as CacheMeta | null;

  const meta: CacheMeta = {
    id,
    type,
    cacheKeyUrl,
    createdAt: existing?.createdAt || now,
    updatedAt: now,
    lastSyncedAt: options.synced ? now : existing?.lastSyncedAt ?? null,
    source: options.source ?? existing?.source ?? "get",
    requestBodyHash: options.requestBodyHash ?? existing?.requestBodyHash ?? null,
  };

  await kvPutWithRetry(env, key, JSON.stringify(meta));
}

function formatUaeNow(): string {
  const now = new Date();
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Asia/Dubai",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });

  const parts = formatter.formatToParts(now);
  const lookup: Record<string, string> = {};
  for (const part of parts) {
    if (part.type !== "literal") {
      lookup[part.type] = part.value;
    }
  }

  return `${lookup.year}-${lookup.month}-${lookup.day}T${lookup.hour}:${lookup.minute}:${lookup.second}+04:00`;
}

async function hashCacheKey(type: CacheType, cacheKeyUrl: string): Promise<string> {
  const encoder = new TextEncoder();
  const data = encoder.encode(`${type}:${cacheKeyUrl}`);
  const digest = await crypto.subtle.digest("SHA-256", data);
  const bytes = Array.from(new Uint8Array(digest));
  return bytes.map((b) => b.toString(16).padStart(2, "0")).join("");
}

async function getVisitStub(env: Env, type: CacheType, cacheKeyUrl: string): Promise<DurableObjectStub> {
  const id = await hashCacheKey(type, cacheKeyUrl);
  const name = `${type}:${id}`;
  return env.VISIT_COUNTER.get(env.VISIT_COUNTER.idFromName(name));
}

async function incrementVisit(env: Env, type: CacheType, cacheKeyUrl: string): Promise<void> {
  const stub = await getVisitStub(env, type, cacheKeyUrl);
  await stub.fetch("https://visit-counter/increment", { method: "POST" });
}

async function getVisitCount(env: Env, type: CacheType, cacheKeyUrl: string): Promise<number> {
  try {
    const stub = await getVisitStub(env, type, cacheKeyUrl);
    const res = await stub.fetch("https://visit-counter/count");
    if (!res.ok) {
      return 0;
    }
    const data = await res.json() as { count?: number };
    return typeof data.count === "number" ? data.count : 0;
  } catch {
    return 0;
  }
}

async function listCacheMeta(env: Env, type: CacheType | null): Promise<CacheMeta[]> {
  const prefixes = type ? [`${type}:`] : ["plp:", "pdp:"];
  const items: CacheMeta[] = [];

  for (const prefix of prefixes) {
    const listResult = await env.CACHE_META.list({ prefix, limit: 1000 });
    for (const key of listResult.keys) {
      const value = await env.CACHE_META.get(key.name, "json") as CacheMeta | null;
      if (value && value.cacheKeyUrl) {
        items.push(value);
      }
    }
  }

  items.sort((a, b) => b.updatedAt.localeCompare(a.updatedAt));
  return items;
}

async function clearCacheByType(
  env: Env,
  ctx: ExecutionContext,
  type: CacheType | null
): Promise<number> {
  const items = await listCacheMeta(env, type);
  let cleared = 0;
  for (const item of items) {
    const cacheKey = new Request(item.cacheKeyUrl, { method: "GET" });
    await caches.default.delete(cacheKey);
    const metaKey = `${item.type}:${item.id}`;
    await env.CACHE_META.delete(metaKey);
    cleared += 1;
  }
  return cleared;
}

async function getCacheMeta(env: Env, id: string): Promise<CacheMeta | null> {
  const plp = await env.CACHE_META.get(`plp:${id}`, "json") as CacheMeta | null;
  if (plp) {
    return plp;
  }
  const pdp = await env.CACHE_META.get(`pdp:${id}`, "json") as CacheMeta | null;
  return pdp || null;
}

async function getCachedResponse(cacheKeyUrl: string): Promise<unknown> {
  const cacheKey = new Request(cacheKeyUrl, { method: "GET" });
  const cachedResponse = await caches.default.match(cacheKey);
  if (!cachedResponse) {
    return { hit: false };
  }

  const bodyText = await cachedResponse.clone().text();
  return {
    hit: true,
    status: cachedResponse.status,
    headers: Object.fromEntries(cachedResponse.headers),
    body: safeJsonParse(bodyText),
    raw: bodyText,
  };
}

async function getFreshResponseForMeta(env: Env, meta: CacheMeta): Promise<unknown> {
  const requestUrl = new URL(meta.cacheKeyUrl);
  const fakeRequest = new Request(requestUrl.toString(), { method: "GET" });

  if (meta.type === "pdp") {
    const slugMatch = requestUrl.pathname.match(/^\/products\/([^\/]+)\/?$/);
    if (!slugMatch) {
      return { error: "Invalid PDP cache key" };
    }
    const slug = slugMatch[1];
    const region = (requestUrl.searchParams.get("region") || "sa").toLowerCase();
    const language = (requestUrl.searchParams.get("language") || "en").toLowerCase();
    const backendResponse = await fetchPdpBackend(env, fakeRequest, slug, region, language, { noCache: true });
    return await describeResponse(backendResponse);
  }

  const backendBody = buildPlpBodyFromUrl(requestUrl);
  const backendHeaders = {
    "content-type": "application/json",
    "accept": "application/json",
  };
  const backendResponse = await fetchPlpBackend(env, fakeRequest, backendBody, backendHeaders, { noCache: true });
  return await describeResponse(backendResponse);
}

async function syncCacheForMeta(env: Env, ctx: ExecutionContext, meta: CacheMeta): Promise<unknown> {
  const fresh = await getFreshResponseForMeta(env, meta);
  if ("status" in (fresh as Record<string, unknown>) && (fresh as any).raw) {
    const responseInfo = fresh as { status: number; headers: Record<string, string>; raw: string };
    const cacheResponse = buildEdgeCacheResponse(responseInfo.raw, responseInfo.status, responseInfo.headers);
    const cacheKey = new Request(meta.cacheKeyUrl, { method: "GET" });
    await caches.default.put(cacheKey, cacheResponse);
    await recordCacheMeta(env, meta.type, meta.cacheKeyUrl, {
      synced: true,
      source: meta.source,
      requestBodyHash: meta.requestBodyHash,
    });
  }
  return fresh;
}

async function syncAllCache(
  env: Env,
  ctx: ExecutionContext,
  type: CacheType | null
): Promise<{ total: number; synced: number; failed: number }> {
  const items = await listCacheMeta(env, type);
  let synced = 0;
  let failed = 0;
  for (const item of items) {
    try {
      await syncCacheForMeta(env, ctx, item);
      synced += 1;
    } catch {
      failed += 1;
    }
  }
  return { total: items.length, synced, failed };
}

function buildPlpBodyFromUrl(url: URL): Record<string, unknown> {
  const page = parseInt(url.searchParams.get("page") || "1", 10);
  const count = parseInt(url.searchParams.get("count") || "24", 10);
  const category = url.searchParams.get("category") || "";
  const region = (url.searchParams.get("region") || "sa").toLowerCase();
  const language = (url.searchParams.get("language") || "en").toLowerCase();
  const order = url.searchParams.get("order") || "OrderByScoreDESC";
  const collection = url.searchParams.get("collection") || undefined;
  const calculateTotalPrice = url.searchParams.get("calculateTotalPrice") === "true";

  let filters: unknown[] = [];
  try {
    const filtersParam = url.searchParams.get("filters");
    if (filtersParam) {
      filters = JSON.parse(filtersParam);
    }
  } catch {
    filters = [];
  }

  const backendBody: Record<string, unknown> = {
    page,
    count,
    category,
    region,
    language,
    order,
    filters,
    calculateTotalPrice,
  };
  if (collection) {
    backendBody.collection = collection;
  }
  return backendBody;
}

async function describeResponse(response: Response): Promise<unknown> {
  const bodyText = await response.clone().text();
  return {
    status: response.status,
    headers: Object.fromEntries(response.headers),
    body: safeJsonParse(bodyText),
    raw: bodyText,
  };
}

function safeJsonParse(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    return null;
  }
}

async function hashRequestBody(body: Record<string, unknown>): Promise<string> {
  const text = stableStringify(body);
  return await hashText(text);
}

function stableStringify(value: unknown): string {
  if (value === undefined) {
    return "null";
  }
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableStringify(item)).join(",")}]`;
  }
  const entries = Object.entries(value as Record<string, unknown>).sort(([a], [b]) => a.localeCompare(b));
  return `{${entries.map(([key, val]) => `${JSON.stringify(key)}:${stableStringify(val)}`).join(",")}}`;
}

async function hashText(text: string): Promise<string> {
  const encoder = new TextEncoder();
  const data = encoder.encode(text);
  const digest = await crypto.subtle.digest("SHA-256", data);
  const bytes = Array.from(new Uint8Array(digest));
  return bytes.map((b) => b.toString(16).padStart(2, "0")).join("");
}

function getPrewarmKey(id: string): string {
  return `prewarm:${id}`;
}

async function kvPutWithRetry(env: Env, key: string, value: string, retries = 3): Promise<void> {
  let attempt = 0;
  let delay = 200;
  while (true) {
    try {
      await env.CACHE_META.put(key, value);
      return;
    } catch (error) {
      attempt += 1;
      if (attempt > retries) {
        throw error;
      }
      await sleep(delay);
      delay *= 2;
    }
  }
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function createPrewarmJob(
  env: Env,
  regions: string[],
  languages: string[],
  categories: string[],
  maxPages: number | null,
  start: number,
  limit: number,
  category: string | null
): Promise<string> {
  const id = crypto.randomUUID();
  const now = formatUaeNow();
  const job: PrewarmJob = {
    id,
    status: "running",
    startedAt: now,
    finishedAt: null,
    processed: 0,
    total: 0,
    errors: 0,
    regions,
    languages,
    categories,
    regionIndex: 0,
    languageIndex: 0,
    categoryIndex: 0,
    nextPage: 1,
    lastPage: 0,
    maxPages,
    start,
    limit,
    category,
    message: null,
  };
  await kvPutWithRetry(env, getPrewarmKey(id), JSON.stringify(job));
  return id;
}

async function getPrewarmJob(env: Env, id: string): Promise<PrewarmJob | null> {
  return await env.CACHE_META.get(getPrewarmKey(id), "json") as PrewarmJob | null;
}

async function listPrewarmJobs(env: Env): Promise<PrewarmJob[]> {
  const listResult = await env.CACHE_META.list({ prefix: "prewarm:", limit: 100 });
  const items: PrewarmJob[] = [];
  for (const key of listResult.keys) {
    const value = await env.CACHE_META.get(key.name, "json") as PrewarmJob | null;
    if (value) {
      items.push(value);
    }
  }
  items.sort((a, b) => b.startedAt.localeCompare(a.startedAt));
  return items;
}

async function clearPrewarmJobs(env: Env, status: string | null): Promise<number> {
  const jobs = await listPrewarmJobs(env);
  let cleared = 0;
  for (const job of jobs) {
    if (status && job.status !== status) {
      continue;
    }
    await env.CACHE_META.delete(getPrewarmKey(job.id));
    cleared += 1;
  }
  return cleared;
}

async function updatePrewarmProgress(env: Env, id: string, increment: number): Promise<void> {
  const job = await getPrewarmJob(env, id);
  if (!job) {
    return;
  }
  job.processed += increment;
  await kvPutWithRetry(env, getPrewarmKey(id), JSON.stringify(job));
}

async function updatePrewarmTotal(env: Env, id: string, increment: number): Promise<void> {
  const job = await getPrewarmJob(env, id);
  if (!job) {
    return;
  }
  job.total += increment;
  await kvPutWithRetry(env, getPrewarmKey(id), JSON.stringify(job));
}

async function updatePrewarmError(env: Env, id: string, message: string): Promise<void> {
  const job = await getPrewarmJob(env, id);
  if (!job) {
    return;
  }
  job.errors += 1;
  job.message = message;
  await kvPutWithRetry(env, getPrewarmKey(id), JSON.stringify(job));
}

async function finalizePrewarmJob(
  env: Env,
  id: string,
  status: "completed" | "failed",
  message: string | null = null
): Promise<void> {
  const job = await getPrewarmJob(env, id);
  if (!job) {
    return;
  }
  job.status = status;
  job.finishedAt = formatUaeNow();
  if (message) {
    job.message = message;
  }
  await kvPutWithRetry(env, getPrewarmKey(id), JSON.stringify(job));
}

async function fetchCategoryTree(env: Env): Promise<Array<{ value?: string; name?: string; id?: string }>> {
  const response = await fetch(env.BACKEND_CATEGORY_TREE_URL, {
    method: "GET",
    headers: { "accept": "application/json" },
    cache: "no-store",
  });
  if (!response.ok) {
    throw new Error(`Category tree fetch failed: ${response.status}`);
  }
  const data = await response.json();
  return Array.isArray(data) ? data : [];
}

async function getPrewarmCategories(
  env: Env,
  category: string | null,
  start: number,
  limit: number
): Promise<string[]> {
  if (category) {
    return [category];
  }
  const categories = await fetchCategoryTree(env);
  const categoryValues = categories
    .map((cat) => (typeof cat.value === "string" ? cat.value : ""))
    .filter(Boolean);
  return categoryValues.slice(start, start + limit);
}

function advancePrewarmPointer(job: PrewarmJob) {
  job.categoryIndex += 1;
  job.nextPage = 1;
  job.lastPage = 0;
  if (job.categoryIndex >= job.categories.length) {
    job.categoryIndex = 0;
    job.languageIndex += 1;
    if (job.languageIndex >= job.languages.length) {
      job.languageIndex = 0;
      job.regionIndex += 1;
    }
  }
}

async function runPrewarmBatch(
  env: Env,
  request: Request,
  jobId: string,
  batchSize: number
): Promise<PrewarmJob | null> {
  const job = await getPrewarmJob(env, jobId);
  if (!job) {
    return null;
  }
  if (job.status !== "running") {
    return job;
  }

  if (job.categories.length === 0) {
    job.status = "completed";
    job.finishedAt = formatUaeNow();
    await kvPutWithRetry(env, getPrewarmKey(job.id), JSON.stringify(job));
    return job;
  }

  let processedThisBatch = 0;
  const edgeOrigin = new URL(request.url).origin;
  const backendHeaders: Record<string, string> = {
    "content-type": "application/json",
    "accept": "application/json",
  };

  while (processedThisBatch < batchSize && job.status === "running") {
    if (job.regionIndex >= job.regions.length) {
      job.status = "completed";
      job.finishedAt = formatUaeNow();
      break;
    }

    const region = job.regions[job.regionIndex];
    const language = job.languages[job.languageIndex];
    const category = job.categories[job.categoryIndex];

    if (!category) {
      advancePrewarmPointer(job);
      continue;
    }

    if (job.nextPage <= 1) {
      const baseBody = {
        page: 1,
        count: 24,
        category,
        region,
        language,
        order: "OrderByScoreDESC",
        filters: [],
        calculateTotalPrice: false,
      };

      const baseCacheKeyUrl = buildPlpCacheKeyUrl(edgeOrigin, baseBody);
      const baseCacheKey = new Request(baseCacheKeyUrl, { method: "GET" });
      const cachedFirstPage = await caches.default.match(baseCacheKey);
      if (cachedFirstPage) {
        advancePrewarmPointer(job);
        processedThisBatch += 1;
        await kvPutWithRetry(env, getPrewarmKey(job.id), JSON.stringify(job));
        continue;
      }

      const firstResponse = await fetchPlpBackend(env, request, baseBody, backendHeaders, {
        noCache: true,
        debug: true,
      });

      if (!firstResponse.ok) {
        const errorText = await firstResponse.text();
        console.error("Prewarm first page failed", {
          jobId,
          status: firstResponse.status,
          response: errorText.slice(0, 200),
          region,
          language,
          category,
        });
        await updatePrewarmError(
          env,
          jobId,
          `PLP fetch failed ${firstResponse.status} | POST ${env.BACKEND_PRODUCT_LIST_URL} | body: ${JSON.stringify(baseBody)} | response: ${errorText}`
        );
        advancePrewarmPointer(job);
        await kvPutWithRetry(env, getPrewarmKey(job.id), JSON.stringify(job));
        continue;
      }

      const firstBodyText = await firstResponse.text();
      const data = safeJsonParse(firstBodyText) as Record<string, unknown> | null;
      const pagination = data?.pagination;
      const zeroProducts = isZeroProductResponse(data);
      let lastPage = 1;
      if (pagination && typeof pagination.lastPageIndex === "number") {
        lastPage = pagination.lastPageIndex;
      }
      if (pagination && typeof pagination.limit === "number") {
        baseBody.count = pagination.limit;
      }
      job.lastPage = job.maxPages && job.maxPages > 0 ? Math.min(lastPage, job.maxPages) : lastPage;
      if (zeroProducts) {
        job.lastPage = 0;
      }
      job.total += Math.max(job.lastPage, 1);

      const cacheKeyUrl = buildPlpCacheKeyUrl(edgeOrigin, baseBody);
      const cacheKey = new Request(cacheKeyUrl, { method: "GET" });
      const cacheResponse = buildEdgeCacheResponse(firstBodyText, firstResponse.status, Object.fromEntries(firstResponse.headers));
      await caches.default.put(cacheKey, cacheResponse);
      await recordCacheMeta(env, "plp", cacheKeyUrl, { synced: false, source: "post" });

      job.processed += 1;
      job.nextPage = 2;
      processedThisBatch += 1;

      if (job.nextPage > job.lastPage) {
        advancePrewarmPointer(job);
      }

      await kvPutWithRetry(env, getPrewarmKey(job.id), JSON.stringify(job));
      continue;
    }

    if (job.nextPage <= job.lastPage) {
      const body = {
        page: job.nextPage,
        count: 24,
        category,
        region,
        language,
        order: "OrderByScoreDESC",
        filters: [],
        calculateTotalPrice: false,
      };

      const pageResponse = await fetchPlpBackend(env, request, body, backendHeaders, {
        noCache: true,
        debug: true,
      });
      if (!pageResponse.ok) {
        const errorText = await pageResponse.text();
        console.error("Prewarm page failed", {
          jobId,
          status: pageResponse.status,
          response: errorText.slice(0, 200),
          region,
          language,
          category,
          page: job.nextPage,
        });
        await updatePrewarmError(
          env,
          jobId,
          `PLP fetch failed ${pageResponse.status} | POST ${env.BACKEND_PRODUCT_LIST_URL} | body: ${JSON.stringify(body)} | response: ${errorText}`
        );
      } else {
        const pageBodyText = await pageResponse.text();
        const pageCacheKeyUrl = buildPlpCacheKeyUrl(edgeOrigin, body);
        const pageCacheKey = new Request(pageCacheKeyUrl, { method: "GET" });
        const pageCacheResponse = buildEdgeCacheResponse(pageBodyText, pageResponse.status, Object.fromEntries(pageResponse.headers));
        await caches.default.put(pageCacheKey, pageCacheResponse);
        await recordCacheMeta(env, "plp", pageCacheKeyUrl, { synced: false, source: "post" });
      }

      job.processed += 1;
      job.nextPage += 1;
      processedThisBatch += 1;

      if (job.nextPage > job.lastPage) {
        advancePrewarmPointer(job);
      }

      await kvPutWithRetry(env, getPrewarmKey(job.id), JSON.stringify(job));
      continue;
    }

    advancePrewarmPointer(job);
    await kvPutWithRetry(env, getPrewarmKey(job.id), JSON.stringify(job));
  }

  if (job.status !== "running") {
    await kvPutWithRetry(env, getPrewarmKey(job.id), JSON.stringify(job));
  }

  return job;
}

function isZeroProductResponse(data: Record<string, unknown> | null): boolean {
  if (!data) {
    return false;
  }
  const pagination = data.pagination as Record<string, unknown> | undefined;
  const totals = [
    pagination?.totalItems,
    pagination?.totalProducts,
    pagination?.total,
    pagination?.totalCount,
    data.totalProducts,
    data.totalItems,
    data.total,
  ];
  for (const value of totals) {
    if (typeof value === "number") {
      return value <= 0;
    }
  }
  const listCandidates = [data.products, data.items, data.data];
  for (const list of listCandidates) {
    if (Array.isArray(list)) {
      return list.length === 0;
    }
  }
  return false;
}

function buildEdgeCacheResponse(
  rawBody: string,
  status: number,
  headers: Record<string, string>
): Response {
  const responseHeaders = new Headers(headers);
  if (!responseHeaders.get("content-type")) {
    responseHeaders.set("content-type", "application/json");
  }
  responseHeaders.set("cache-control", "public, max-age=31536000, stale-while-revalidate=31536000");

  return new Response(rawBody, {
    status,
    headers: responseHeaders,
  });
}

async function parseJsonBody(request: Request): Promise<Record<string, unknown> | null> {
  try {
    const text = await request.text();
    if (!text) {
      return {};
    }
    const parsed = JSON.parse(text);
    if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
      return parsed as Record<string, unknown>;
    }
    return null;
  } catch {
    return null;
  }
}

function normalizePlpBody(body: Record<string, unknown>): Record<string, unknown> {
  const page = Number.isFinite(Number(body.page)) ? Number(body.page) : 1;
  const count = Number.isFinite(Number(body.count)) ? Number(body.count) : 24;
  const category = typeof body.category === "string" ? body.category : "";
  const region = (typeof body.region === "string" ? body.region : "sa").toLowerCase();
  const language = (typeof body.language === "string" ? body.language : "en").toLowerCase();
  const order = typeof body.order === "string" ? body.order : "OrderByScoreDESC";
  const collection = typeof body.collection === "string" ? body.collection : undefined;
  const calculateTotalPrice = body.calculateTotalPrice === true;
  const filters = Array.isArray(body.filters) ? body.filters : [];

  const normalized: Record<string, unknown> = {
    page,
    count,
    category,
    region,
    language,
    order,
    filters,
    calculateTotalPrice,
  };

  if (collection) {
    normalized.collection = collection;
  }

  return normalized;
}

function buildPlpCacheKeyUrl(origin: string, backendBody: Record<string, unknown>): string {
  const params = new URLSearchParams();
  params.set("page", String(backendBody.page ?? 1));
  params.set("count", String(backendBody.count ?? 24));
  params.set("category", String(backendBody.category ?? ""));
  params.set("region", String(backendBody.region ?? "sa"));
  params.set("language", String(backendBody.language ?? "en"));
  params.set("order", String(backendBody.order ?? "OrderByScoreDESC"));

  if (backendBody.collection) {
    params.set("collection", String(backendBody.collection));
  }

  params.set("filters", JSON.stringify(backendBody.filters ?? []));
  params.set("calculateTotalPrice", String(backendBody.calculateTotalPrice === true));

  return `${origin}/products?${params.toString()}`;
}

function getAdminHtml(): string {
  return `<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>Edge Cache Admin</title>
    <style>
      @import url("https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@400;500;600&display=swap");
      :root {
        --bg: #ffffff;
        --panel: #ffffff;
        --text: #111111;
        --muted: #6b7280;
        --border: #e5e7eb;
        --accent: #111111;
        --danger: #b91c1c;
        --shadow: 0 10px 30px rgba(0, 0, 0, 0.06);
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        font-family: "IBM Plex Sans", "Segoe UI", Tahoma, sans-serif;
        background: var(--bg);
        color: var(--text);
        padding: 32px;
      }
      h1 { margin: 0 0 16px; font-weight: 600; letter-spacing: -0.02em; }
      .controls { display: flex; gap: 8px; margin-bottom: 16px; flex-wrap: wrap; }
      button {
        background: var(--accent);
        border: 1px solid var(--accent);
        padding: 8px 14px;
        border-radius: 8px;
        cursor: pointer;
        color: #ffffff;
        font-weight: 600;
      }
      button.secondary {
        background: #ffffff;
        border: 1px solid var(--border);
        color: var(--text);
      }
      button.danger {
        background: #ffffff;
        border: 1px solid var(--danger);
        color: var(--danger);
      }
      .layout { display: grid; gap: 16px; grid-template-columns: 1.2fr 1fr; }
      .panel {
        background: var(--panel);
        padding: 16px;
        border-radius: 12px;
        border: 1px solid var(--border);
        box-shadow: var(--shadow);
      }
      .list { max-height: 70vh; overflow: auto; }
      .item {
        padding: 12px;
        border-bottom: 1px solid var(--border);
      }
      .item:last-child { border-bottom: none; }
      .item-title { font-size: 14px; word-break: break-all; }
      .meta { font-size: 12px; color: var(--muted); margin-top: 6px; }
      .actions { display: flex; gap: 8px; margin-top: 8px; flex-wrap: wrap; }
      table {
        width: 100%;
        border-collapse: collapse;
        font-size: 13px;
      }
      th, td {
        text-align: left;
        padding: 10px 8px;
        border-bottom: 1px solid var(--border);
        vertical-align: top;
      }
      th {
        font-weight: 600;
        font-size: 12px;
        color: var(--muted);
        text-transform: uppercase;
        letter-spacing: 0.04em;
      }
      .table-wrap { overflow-x: auto; }
      .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace; }
      .modal-backdrop {
        position: fixed;
        inset: 0;
        background: rgba(17, 24, 39, 0.5);
        display: flex;
        align-items: center;
        justify-content: center;
        padding: 24px;
        z-index: 50;
      }
      .modal {
        background: #ffffff;
        border-radius: 12px;
        border: 1px solid var(--border);
        max-width: 900px;
        width: 100%;
        max-height: 85vh;
        overflow: auto;
        padding: 16px;
        box-shadow: var(--shadow);
      }
      .modal-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        margin-bottom: 12px;
      }
      .modal-header h3 { margin: 0; font-size: 16px; }
      pre {
        white-space: pre-wrap;
        background: #f8fafc;
        padding: 12px;
        border-radius: 8px;
        max-height: 240px;
        overflow: auto;
        border: 1px solid var(--border);
      }
      @media (max-width: 900px) {
        .layout { grid-template-columns: 1fr; }
      }
    </style>
  </head>
  <body>
    <div id="root"></div>
    <script src="https://unpkg.com/react@18/umd/react.production.min.js"></script>
    <script src="https://unpkg.com/react-dom@18/umd/react-dom.production.min.js"></script>
    <script>
      const e = React.createElement;

      function App() {
        const [items, setItems] = React.useState([]);
        const [selected, setSelected] = React.useState(null);
        const [loading, setLoading] = React.useState(false);
        const [prewarmJobs, setPrewarmJobs] = React.useState([]);
        const [prewarmLoading, setPrewarmLoading] = React.useState(false);
        const [error, setError] = React.useState('');
        const [prewarmRegion, setPrewarmRegion] = React.useState('sa');
        const [prewarmLanguage, setPrewarmLanguage] = React.useState('en');
        const [prewarmStart, setPrewarmStart] = React.useState('0');
        const [prewarmLimit, setPrewarmLimit] = React.useState('25');
        const [prewarmMaxPages, setPrewarmMaxPages] = React.useState('');
        const [prewarmCategory, setPrewarmCategory] = React.useState('');
        const [showPrewarm, setShowPrewarm] = React.useState(true);
        const [categories, setCategories] = React.useState([]);
        const [modalOpen, setModalOpen] = React.useState(false);
        const [autoContinue, setAutoContinue] = React.useState(true);
        const [activeJobId, setActiveJobId] = React.useState('');
        const [filterRegion, setFilterRegion] = React.useState('');
        const [filterCategory, setFilterCategory] = React.useState('');
        const [searchTerm, setSearchTerm] = React.useState('');

        const load = async (type) => {
          setLoading(true);
          setError('');
          try {
            const res = await fetch('/admin/cache?type=' + type, { credentials: 'same-origin' });
            const data = await res.json();
            setItems(data.items || []);
            setSelected(null);
          } catch (err) {
            setError('Failed to load cache list.');
          } finally {
            setLoading(false);
          }
        };
        const syncAll = async () => {
          setLoading(true);
          setError('');
          try {
            const res = await fetch('/admin/cache/sync-all', { method: 'POST', credentials: 'same-origin' });
            if (!res.ok) {
              setError('Failed to sync all cache.');
            }
          } catch {
            setError('Failed to sync all cache.');
          } finally {
            setLoading(false);
          }
        };

        const loadPrewarm = async () => {
          setPrewarmLoading(true);
          const res = await fetch('/admin/prewarm/list', { credentials: 'same-origin' });
          const data = await res.json();
          setPrewarmJobs(data.items || []);
          setPrewarmLoading(false);
        };

        const loadCategories = async () => {
          try {
            const res = await fetch('/admin/categories', { credentials: 'same-origin' });
            const data = await res.json();
            setCategories(data.items || []);
          } catch {
            setCategories([]);
          }
        };

        const startPrewarm = async () => {
          setPrewarmLoading(true);
          const params = new URLSearchParams();
          if (prewarmRegion) params.set('region', prewarmRegion);
          if (prewarmLanguage) params.set('language', prewarmLanguage);
          if (prewarmStart) params.set('start', prewarmStart);
          if (prewarmLimit) params.set('limit', prewarmLimit);
          if (prewarmMaxPages) params.set('maxPages', prewarmMaxPages);
          if (prewarmCategory) params.set('category', prewarmCategory);
          const res = await fetch('/admin/prewarm?' + params.toString(), { method: 'POST', credentials: 'same-origin' });
          const data = await res.json();
          if (data.jobId) {
            setActiveJobId(data.jobId);
          }
          await loadPrewarm();
        };

        const runNextBatch = async (jobId) => {
          if (!jobId) return null;
          const res = await fetch('/admin/prewarm/continue?id=' + jobId + '&batch=1', {
            method: 'POST',
            credentials: 'same-origin'
          });
          return await res.json();
        };

        const clearPrewarm = async (status) => {
          setPrewarmLoading(true);
          const res = await fetch('/admin/prewarm/clear' + (status ? '?status=' + status : ''), {
            method: 'POST',
            credentials: 'same-origin'
          });
          await res.json();
          await loadPrewarm();
        };

        const clear = async (type) => {
          const res = await fetch('/admin/cache/clear?type=' + type, { method: 'POST', credentials: 'same-origin' });
          await res.json();
          await load('all');
        };

        const view = async (id) => {
          const res = await fetch('/admin/cache/item?id=' + id, { credentials: 'same-origin' });
          const data = await res.json();
          setSelected(data);
          setModalOpen(true);
        };

        const sync = async (id) => {
          const res = await fetch('/admin/cache/sync?id=' + id, { method: 'POST', credentials: 'same-origin' });
          const data = await res.json();
          setSelected((prev) => ({ ...prev, fresh: data }));
          await load('all');
        };

        React.useEffect(() => {
          load('all');
          loadPrewarm();
          loadCategories();
        }, []);

        React.useEffect(() => {
          if (!autoContinue || !activeJobId) return;
          const interval = setInterval(async () => {
            const job = await runNextBatch(activeJobId);
            await loadPrewarm();
            if (job && job.status && job.status !== 'running') {
              setAutoContinue(false);
            }
          }, 5000);
          return () => clearInterval(interval);
        }, [autoContinue, activeJobId]);

        const ttlSeconds = 31536000;
        const formatUaeDate = (value) => {
          if (!value) return '-';
          const date = new Date(value);
          if (Number.isNaN(date.getTime())) return value;
          return new Intl.DateTimeFormat('en-CA', {
            timeZone: 'Asia/Dubai',
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            hour12: false
          }).format(date).replace(' ', 'T') + '+04:00';
        };
        const computeExpiry = (updatedAt) => {
          if (!updatedAt) return '-';
          const date = new Date(updatedAt);
          if (Number.isNaN(date.getTime())) return '-';
          return formatUaeDate(new Date(date.getTime() + ttlSeconds * 1000));
        };
        const getCategoryName = (item) => {
          try {
            const url = new URL(item.cacheKeyUrl);
            if (item.type === 'pdp') {
              const parts = url.pathname.split('/');
              return decodeURIComponent(parts[parts.length - 1] || '');
            }
            const category = url.searchParams.get('category') || '';
            return decodeURIComponent(category);
          } catch {
            return '';
          }
        };
        const getCollection = (item) => {
          try {
            if (item.type === 'pdp') {
              return '-';
            }
            const url = new URL(item.cacheKeyUrl);
            return url.searchParams.get('collection') || '-';
          } catch {
            return '-';
          }
        };
        const getCategoryDisplay = (item) => {
          const name = getCategoryName(item) || '-';
          const collection = getCollection(item);
          if (collection && collection !== '-') {
            return name + ' / ' + collection;
          }
          return name;
        };
        const getOrder = (item) => {
          try {
            const url = new URL(item.cacheKeyUrl);
            return url.searchParams.get('order') || '-';
          } catch {
            return '-';
          }
        };
        const getSlug = (item) => {
          if (item.type !== 'pdp') {
            return '';
          }
          return getCategoryName(item);
        };
        const getPageNumber = (item) => {
          try {
            const url = new URL(item.cacheKeyUrl);
            const page = url.searchParams.get('page');
            return page ? page : '-';
          } catch {
            return '-';
          }
        };
        const getRegion = (item) => {
          try {
            const url = new URL(item.cacheKeyUrl);
            return url.searchParams.get('region') || '-';
          } catch {
            return '-';
          }
        };
        const getLanguage = (item) => {
          try {
            const url = new URL(item.cacheKeyUrl);
            return url.searchParams.get('language') || '-';
          } catch {
            return '-';
          }
        };

        const filteredItems = items.filter((item) => {
          const region = getRegion(item);
          const category = getCategoryName(item);
          const regionOk = !filterRegion || region === filterRegion;
          const categoryOk = !filterCategory || category === filterCategory;
          const search = searchTerm.trim().toLowerCase();
          if (!search) {
            return regionOk && categoryOk;
          }
          const collection = getCollection(item);
          const slug = getSlug(item);
          const match =
            (category || '').toLowerCase().includes(search) ||
            (collection || '').toLowerCase().includes(search) ||
            (slug || '').toLowerCase().includes(search);
          return regionOk && categoryOk && match;
        });

        return e('div', null,
          e('h1', null, 'Edge Cache Admin'),
          e('div', { className: 'controls' },
            e('button', { onClick: () => load('all') }, 'Load All'),
            e('button', { className: 'secondary', onClick: () => syncAll() }, 'Sync All'),
            e('button', { className: 'secondary', onClick: () => setShowPrewarm(!showPrewarm) },
              showPrewarm ? 'Hide Prewarm' : 'Show Prewarm'
            ),
            loading ? e('span', { style: { marginLeft: '8px' } }, 'Loading...') : null,
            error ? e('span', { style: { marginLeft: '8px', color: '#b91c1c' } }, error) : null
          ),
          showPrewarm ? e('div', { className: 'panel', style: { marginBottom: '16px' } },
            e('div', { className: 'controls' },
              e('input', {
                value: prewarmRegion,
                onChange: (e) => setPrewarmRegion(e.target.value),
                placeholder: 'region',
                style: { padding: '8px', borderRadius: '8px', border: '1px solid #e5e7eb' }
              }),
              e('input', {
                value: prewarmLanguage,
                onChange: (e) => setPrewarmLanguage(e.target.value),
                placeholder: 'language',
                style: { padding: '8px', borderRadius: '8px', border: '1px solid #e5e7eb' }
              }),
              e('input', {
                value: prewarmStart,
                onChange: (e) => setPrewarmStart(e.target.value),
                placeholder: 'start',
                style: { padding: '8px', borderRadius: '8px', border: '1px solid #e5e7eb', width: '90px' }
              }),
              e('input', {
                value: prewarmLimit,
                onChange: (e) => setPrewarmLimit(e.target.value),
                placeholder: 'limit',
                style: { padding: '8px', borderRadius: '8px', border: '1px solid #e5e7eb', width: '90px' }
              }),
              e('input', {
                value: prewarmMaxPages,
                onChange: (e) => setPrewarmMaxPages(e.target.value),
                placeholder: 'maxPages',
                style: { padding: '8px', borderRadius: '8px', border: '1px solid #e5e7eb', width: '110px' }
              }),
              e('select', {
                value: prewarmCategory,
                onChange: (e) => setPrewarmCategory(e.target.value),
                style: { padding: '8px', borderRadius: '8px', border: '1px solid #e5e7eb', minWidth: '260px' }
              },
                e('option', { value: '' }, 'Select category'),
                categories.map((cat) =>
                  e('option', { key: cat.value, value: cat.value }, (cat.name || cat.value))
                )
              ),
              prewarmLoading ? e('span', { style: { marginLeft: '8px' } }, 'Loading...') : null
            ),
            prewarmJobs.length === 0
              ? e('div', { className: 'meta' }, 'No prewarm jobs yet.')
              : prewarmJobs.map((job) =>
                e('div', { key: job.id, className: 'item' },
                  e('div', { className: 'item-title' }, 'Job ', job.id),
                  e('div', { className: 'meta' },
                    'Status: ', job.status,
                    ' | Progress: ', job.processed, '/', job.total,
                    ' | Errors: ', job.errors,
                    ' | Region(s): ', (job.regions || []).join(','),
                    ' | Language(s): ', (job.languages || []).join(','),
                    ' | Range: ', job.start, '-', (job.start + job.limit - 1),
                    job.category ? ' | Category: ' + job.category : '',
                    ' | Page: ', job.nextPage, '/', (job.lastPage || '-'),
                    ' | Started: ', job.startedAt,
                    job.finishedAt ? ' | Finished: ' + job.finishedAt : '',
                    job.message ? ' | Last message: ' + job.message : ''
                  )
                )
              )
          ) : null,
          e('div', { className: 'panel list' },
            e('div', { className: 'controls' },
              e('input', {
                value: filterRegion,
                onChange: (e) => setFilterRegion(e.target.value),
                placeholder: 'filter region',
                style: { padding: '8px', borderRadius: '8px', border: '1px solid #e5e7eb', width: '140px' }
              }),
              e('input', {
                value: searchTerm,
                onChange: (e) => setSearchTerm(e.target.value),
                placeholder: 'search slug/category/collection',
                style: { padding: '8px', borderRadius: '8px', border: '1px solid #e5e7eb', minWidth: '240px' }
              }),
              e('select', {
                value: filterCategory,
                onChange: (e) => setFilterCategory(e.target.value),
                style: { padding: '8px', borderRadius: '8px', border: '1px solid #e5e7eb', minWidth: '260px' }
              },
                e('option', { value: '' }, 'All categories'),
                categories.map((cat) =>
                  e('option', { key: cat.value, value: cat.value }, (cat.name || cat.value))
                )
              )
            ),
            e('div', { className: 'table-wrap' },
              e('table', null,
                e('thead', null,
                    e('tr', null,
                      e('th', null, 'Type'),
                      e('th', null, 'Category/Slug/Collection'),
                      e('th', null, 'Page'),
                      e('th', null, 'Order'),
                      e('th', null, 'Region'),
                      e('th', null, 'Language'),
                      e('th', null, 'Created'),
                      e('th', null, 'Updated'),
                      e('th', null, 'Visits'),
                      e('th', null, 'Last Sync'),
                      e('th', null, 'Expire'),
                      e('th', null, 'Source'),
                      e('th', null, 'Actions')
                    )
                  ),
                  e('tbody', null,
                    filteredItems.map((item) =>
                      e('tr', { key: item.id },
                        e('td', null, item.type),
                        e('td', { className: 'mono' }, getCategoryDisplay(item)),
                        e('td', null, getPageNumber(item)),
                        e('td', null, getOrder(item)),
                        e('td', null, getRegion(item)),
                        e('td', null, getLanguage(item)),
                        e('td', null, formatUaeDate(item.createdAt)),
                        e('td', null, formatUaeDate(item.updatedAt)),
                        e('td', null, typeof item.visits === 'number' ? item.visits : '-'),
                        e('td', null, item.lastSyncedAt ? formatUaeDate(item.lastSyncedAt) : '-'),
                        e('td', null, computeExpiry(item.updatedAt)),
                        e('td', null, item.source || 'get'),
                        e('td', null,
                          e('button', { className: 'secondary', onClick: () => view(item.id) }, 'View Cache'),
                          ' ',
                          e('button', { className: 'danger', onClick: () => sync(item.id) }, 'Sync')
                        )
                    )
                  )
                )
              )
            ),
            modalOpen && selected ? e('div', { className: 'modal-backdrop', onClick: () => setModalOpen(false) },
              e('div', { className: 'modal', onClick: (event) => event.stopPropagation() },
                e('div', { className: 'modal-header' },
                  e('h3', null, 'Cached Response'),
                  e('button', { className: 'secondary', onClick: () => setModalOpen(false) }, 'Close')
                ),
                e('div', { className: 'meta' },
                  'Cache ID: ', selected.meta && selected.meta.id ? selected.meta.id : '-',
                  ' | Cache Key: ', selected.meta && selected.meta.cacheKeyUrl ? selected.meta.cacheKeyUrl : '-'
                ),
                e('pre', null, JSON.stringify(selected.cache, null, 2))
              )
            ) : null
          )
        );
      }

      ReactDOM.createRoot(document.getElementById('root')).render(React.createElement(App));
    </script>
  </body>
</html>`;
}
