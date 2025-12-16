/**
 * Interface for Environment Variables
 */
interface Env {
  BACKEND_PRODUCT_URL: string;
  BACKEND_PRODUCT_LIST_URL: string;
  RANKING_SERVICE_URL?: string;
  RANKING_SERVICE_TOKEN?: string;
  BRAND?: string;
  CACHE_PURGE_TOKEN?: string;
  DISABLE_WORKER_CACHE?: string;
  ALLOWED_ORIGINS: string; // Changed from ALLOWED_ORIGIN to ALLOWED_ORIGINS
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    try {
      // 1. Handle CORS preflight for browser calls
      if (request.method === "OPTIONS") {
        return handleOptions(env, request);
      }

      // 2. Route Matching
      if (request.method === "POST") {
        if (url.pathname === "/cache/purge" || url.pathname === "/cache/purge/") {
          return await handleCachePurge(request, env);
        }
      }

      if (request.method === "GET") {

        // Simple health check to verify worker deployment/config
        if (url.pathname === "/health" || url.pathname === "/health/") {
          return handleHealth(request, env);
        }
        
        // Match exact "/products" or "/products/"
        if (url.pathname === "/products" || url.pathname === "/products/") {
          return await handleProductListRequest(request, env, ctx);
        }

        // Match "/products/:slug" using Regex
        const slugMatch = url.pathname.match(/^\/products\/([^\/]+)\/?$/);
        if (slugMatch) {
          const slug = slugMatch[1];
          return await handleProductRequest(request, env, ctx, slug);
        }
      }

      // 3. 404 Not Found
      return new Response(JSON.stringify({ error: "Not found" }), {
        status: 404,
        headers: { "Content-Type": "application/json" },
      });

    } catch (err: any) {
      console.error(`Unhandled Worker Error: ${err.message}`, err.stack);
      return new Response(JSON.stringify({ error: "Internal Server Error" }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
    }
  },
};

// Cache epoch to bust stale entries after force updates
let cacheEpoch = 0;
// Window to force bypass of cache reads after purge
let forceRefreshUntil = 0;

/**
 * Helper: Check if origin is allowed (supports comma-separated list)
 */
function isOriginAllowed(origin: string | null, allowedOrigins: string): boolean {
  if (!origin) return false;
  // Split by comma and trim whitespace to handle "url1, url2"
  const allowedList = allowedOrigins.split(",").map(o => o.trim());
  return allowedList.includes(origin);
}

/**
 * Handle OPTIONS requests for CORS
 */
function handleOptions(env: Env, request: Request): Response {
  const headers: Record<string, string> = {
    "access-control-allow-methods": "GET, POST, OPTIONS",
    "access-control-allow-headers": "Content-Type, X-FBC, X-FBP, X-Cache-Purge-Token",
    "access-control-max-age": "86400",
  };

  const origin = request.headers.get("origin");
  
  // Use new helper to check list
  if (isOriginAllowed(origin, env.ALLOWED_ORIGINS)) {
    headers["access-control-allow-origin"] = origin!; // We know origin is not null here
    headers["vary"] = "Origin";
  }

  return new Response(null, {
    status: 204,
    headers,
  });
}

/**
 * Helper: Add CORS headers to existing response
 */
function addCorsHeaders(response: Response, env: Env, request: Request) {
  const origin = request.headers.get("origin");
  
  // Use new helper to check list
  if (isOriginAllowed(origin, env.ALLOWED_ORIGINS)) {
    response.headers.set("access-control-allow-origin", origin!);
    response.headers.set("vary", "Origin");
  }
  response.headers.set("access-control-allow-credentials", "true");
}

/**
 * Health/diagnostic endpoint
 */
function handleHealth(request: Request, env: Env): Response {
  const payload = {
    status: "ok",
    timestamp: new Date().toISOString(),
    rankingServiceConfigured: Boolean(env.RANKING_SERVICE_URL),
    brand: env.BRAND || "leem",
  };

  const response = new Response(JSON.stringify(payload), {
    status: 200,
    headers: {
      "content-type": "application/json",
      "cache-control": "no-store",
    },
  });

  addCorsHeaders(response, env, request);
  return response;
}

/**
 * Purge cache entries for /products (used by admin "Force Update")
 */
async function handleCachePurge(request: Request, env: Env): Promise<Response> {
  try {
    if (env.CACHE_PURGE_TOKEN) {
      const provided = request.headers.get("x-cache-purge-token");
      if (provided !== env.CACHE_PURGE_TOKEN) {
        const resp = new Response(JSON.stringify({ error: "Unauthorized" }), {
          status: 401,
          headers: { "content-type": "application/json" },
        });
        addCorsHeaders(resp, env, request);
        return resp;
      }
    }

    let body: any = {};
    try {
      body = await request.json();
    } catch (_) {
      body = {};
    }

    const category = (body.category || "").trim();
    if (!category) {
      const resp = new Response(JSON.stringify({ error: "category is required" }), {
        status: 400,
        headers: { "content-type": "application/json" },
      });
      addCorsHeaders(resp, env, request);
      return resp;
    }

    const region = String(body.region || "sa").toLowerCase();
    const language = String(body.language || "en").toLowerCase();
    const order = body.order || "OrderByScoreDESC";
    const count = body.count !== undefined ? parseInt(body.count, 10) || 24 : undefined;
    const pages = Math.min(Math.max(parseInt(body.pages ?? "3", 10) || 3, 1), 20);
    const collection = body.collection ? String(body.collection) : undefined;
    const calculateTotalPrice = Boolean(body.calculateTotalPrice);
    const filters = Array.isArray(body.filters) ? body.filters : undefined;
    const warm = body.warm !== false; // default true

    const cache = caches.default;
    const base = new URL(request.url);
    base.pathname = "/products";

    let purged = 0;
    for (let page = 1; page <= pages; page++) {
      const attemptUrls: string[] = [];

      const addVariants = (pathname: string) => {
        const fullUrl = new URL(base.toString());
        fullUrl.pathname = pathname;
        fullUrl.searchParams.set("category", category);
        fullUrl.searchParams.set("region", region);
        fullUrl.searchParams.set("language", language);
        fullUrl.searchParams.set("order", order);
        fullUrl.searchParams.set("page", String(page));
        if (count !== undefined) fullUrl.searchParams.set("count", String(count));
        if (calculateTotalPrice) fullUrl.searchParams.set("calculateTotalPrice", "true");
        if (collection) fullUrl.searchParams.set("collection", collection);
        if (filters) fullUrl.searchParams.set("filters", JSON.stringify(filters));
        attemptUrls.push(fullUrl.toString());

        // If count was omitted, also try the default (24) to catch cached entries created with defaults
        if (count === undefined) {
          const withDefaultCount = new URL(fullUrl.toString());
          withDefaultCount.searchParams.set("count", "24");
          attemptUrls.push(withDefaultCount.toString());
        }

        // Minimal key with defaults (to catch cache entries created from defaults)
        const minimalUrl = new URL(base.toString());
        minimalUrl.pathname = pathname;
        minimalUrl.searchParams.set("category", category);
        minimalUrl.searchParams.set("region", region);
        minimalUrl.searchParams.set("language", language);
        minimalUrl.searchParams.set("order", order);
        minimalUrl.searchParams.set("page", String(page));
        if (count === undefined) {
          minimalUrl.searchParams.set("count", "24");
        } else {
          minimalUrl.searchParams.set("count", String(count));
        }
        attemptUrls.push(minimalUrl.toString());
      };

      addVariants("/products");
      addVariants("/products/");

      let deletedAny = false;
      for (const attempt of attemptUrls) {
        const deleted = await cache.delete(new Request(attempt, { method: "GET" }));
        if (deleted) {
          purged += 1;
          deletedAny = true;
          // Keep deleting other variants to ensure full invalidation
        }
      }
    }

    // Bump cache epoch and force refresh window (5 minutes) so new requests bypass old entries
    cacheEpoch = Date.now();
    forceRefreshUntil = cacheEpoch + 5 * 60 * 1000;

    // Warm cache after purge to ensure fresh data on next request
    let warmed = 0;
    if (warm) {
      for (let page = 1; page <= pages; page++) {
        const warmUrl = new URL(base.toString());
        warmUrl.searchParams.set("category", category);
        warmUrl.searchParams.set("region", region);
        warmUrl.searchParams.set("language", language);
        warmUrl.searchParams.set("order", order);
        warmUrl.searchParams.set("page", String(page));
        warmUrl.searchParams.set("forceRefresh", "true");
        warmUrl.searchParams.set("_cacheEpoch", String(cacheEpoch));
        if (count !== undefined) warmUrl.searchParams.set("count", String(count));
        if (calculateTotalPrice) warmUrl.searchParams.set("calculateTotalPrice", "true");
        if (collection) warmUrl.searchParams.set("collection", collection);
        if (filters) warmUrl.searchParams.set("filters", JSON.stringify(filters));

        try {
          const warmResp = await fetch(warmUrl.toString(), { method: "GET" });
          if (warmResp.ok) warmed += 1;
        } catch (e) {
          console.warn("Warm fetch failed for", warmUrl.toString(), e);
        }
      }
    }

    const response = new Response(
      JSON.stringify({
        status: "success",
        message: "Edge cache purged",
        attempted: pages,
        purged,
        warmed,
        cacheEpoch,
      }),
      {
        status: 200,
        headers: {
          "content-type": "application/json",
          "cache-control": "no-store",
        },
      }
    );
    addCorsHeaders(response, env, request);
    return response;
  } catch (err: any) {
    console.error("Cache purge failed", err);
    const response = new Response(JSON.stringify({ error: "Failed to purge cache" }), {
      status: 500,
      headers: { "content-type": "application/json" },
    });
    addCorsHeaders(response, env, request);
    return response;
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
  // Default: caching off unless explicitly enabled with DISABLE_WORKER_CACHE="false"
  const cacheEnabled = env.DISABLE_WORKER_CACHE === "false";
  const region = (url.searchParams.get("region") || "sa").toLowerCase();
  const language = (url.searchParams.get("language") || "en").toLowerCase();

  const cacheKeyUrl = `${url.origin}/products/${slug}?region=${region}&language=${language}`;
  const cacheKey = new Request(cacheKeyUrl, { method: "GET" });
  const cache = caches.default;

  // 1. Try Cache
  if (cacheEnabled) {
    let cachedResponse = await cache.match(cacheKey);
  
    if (cachedResponse) {
      // CACHE HIT: Fire background update
      ctx.waitUntil(
        fetchAndCacheProduct(request, env, ctx, cacheKey, slug, region, language)
      );
  
      const res = new Response(cachedResponse.body, cachedResponse);
      res.headers.set("x-edge-cache", "HIT-STALE"); 
      addCorsHeaders(res, env, request);
      return res;
    }
  }

  // 2. CACHE MISS
  return await fetchAndCacheProduct(request, env, ctx, cacheKey, slug, region, language, cacheEnabled);
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
  // Default: caching off unless explicitly enabled with DISABLE_WORKER_CACHE="false"
  const cacheEnabled = env.DISABLE_WORKER_CACHE === "false";
  const globalForceRefresh = Date.now() < forceRefreshUntil;
  const cache = caches.default;
  const cacheKeyUrl = new URL(url.toString());
  if (cacheEpoch) {
    cacheKeyUrl.searchParams.set("_cacheEpoch", String(cacheEpoch));
  }
  const cacheKey = new Request(cacheKeyUrl.toString(), { method: "GET" });

  const page = parseInt(url.searchParams.get("page") || "1", 10);
  const count = parseInt(url.searchParams.get("count") || "24", 10);
  const category = url.searchParams.get("category") || "";
  const brand = (env.BRAND || "leem").toLowerCase();
  const region = (url.searchParams.get("region") || "sa").toLowerCase();
  const language = (url.searchParams.get("language") || "en").toLowerCase();
  const order = url.searchParams.get("order") || "OrderByScoreDESC";
  const collection = url.searchParams.get("collection") || undefined;
  const calculateTotalPrice = url.searchParams.get("calculateTotalPrice") === "true";
  const forceRefresh = url.searchParams.get("forceRefresh") === "true";

  let filters: unknown[] = [];
  try {
    const filtersParam = url.searchParams.get("filters");
    if (filtersParam) filters = JSON.parse(filtersParam);
  } catch (e) {
    console.warn("Invalid filters param received");
    filters = [];
  }

  const backendBody: any = {
    page, count, category, brand, region, language, order, filters, calculateTotalPrice,
  };
  if (collection) backendBody.collection = collection;

  // 1. Try Cache (unless forceRefresh)
  if (!forceRefresh && !globalForceRefresh && cacheEnabled) {
    let cachedResponse = await cache.match(cacheKey);
  
    if (cachedResponse) {
      // CACHE HIT: Fire background update
      ctx.waitUntil(
          fetchAndCacheProductList(request, env, ctx, cacheKey, backendBody, cacheEnabled)
      );
  
      const res = new Response(cachedResponse.body, cachedResponse);
      res.headers.set("x-edge-cache", "HIT-STALE");
      addCorsHeaders(res, env, request);
      return res;
    }
  }

  // 2. CACHE MISS or forced refresh
  return await fetchAndCacheProductList(request, env, ctx, cacheKey, backendBody, cacheEnabled);
}

// ============================================================================
// BACKEND FETCHERS (with Stale-While-Revalidate)
// ============================================================================

async function fetchAndCacheProduct(
  request: Request,
  env: Env,
  ctx: ExecutionContext,
  cacheKey: Request,
  slug: string,
  region: string,
  language: string,
  cacheEnabled: boolean
): Promise<Response> {
  const cache = caches.default;
  
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
    const backendResponse = await fetch(env.BACKEND_PRODUCT_URL, {
      method: "POST",
      headers: backendHeaders,
      body: JSON.stringify({ slug, region, language }),
      cf: { cacheTtl: 0, cacheEverything: false },
    });

    if (!backendResponse.ok) {
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
        "cache-control": "private, max-age=0, s-maxage=0, must-revalidate",
      },
    });

    edgeResponse.headers.set("x-edge-cache", "MISS");
    addCorsHeaders(edgeResponse, env, request);

    if (cacheEnabled) {
      ctx.waitUntil(cache.put(cacheKey, edgeResponse.clone()));
    }

    return edgeResponse;

  } catch (error) {
    console.error(`Fetch failed for PDP ${slug}:`, error);
    return new Response(JSON.stringify({ error: "Service Unavailable" }), { 
        status: 502,
        headers: { "Content-Type": "application/json" }
    });
  }
}

async function fetchAndCacheProductList(
    request: Request,
    env: Env,
    ctx: ExecutionContext,
    cacheKey: Request,
    backendBody: any,
    cacheEnabled: boolean
  ): Promise<Response> {
    const cache = caches.default;
  
    const incomingHeaders = request.headers;
    const baseHeaders: Record<string, string> = {
      "content-type": "application/json",
      "accept": "application/json",
    };
    
    const xFbc = incomingHeaders.get("x-fbc");
    const xFbp = incomingHeaders.get("x-fbp");
    if (xFbc) baseHeaders["x-fbc"] = xFbc;
    if (xFbp) baseHeaders["x-fbp"] = xFbp;

    const sendCatalogRequest = () => {
      return fetch(env.BACKEND_PRODUCT_LIST_URL, {
        method: "POST",
        headers: { ...baseHeaders },
        body: JSON.stringify(backendBody),
        // Ensure Cloudflare does not edge-cache upstream responses
        cf: { cacheTtl: 0, cacheEverything: false },
      });
    };

    const sendRankingRequest = () => {
      if (!env.RANKING_SERVICE_URL) {
        throw new Error("Missing RANKING_SERVICE_URL");
      }
      const rankingUrl = new URL(env.RANKING_SERVICE_URL);
      const brand = backendBody.brand || env.BRAND || "leem";

      rankingUrl.searchParams.set("brand", brand);
      if (backendBody.category) rankingUrl.searchParams.set("category", backendBody.category);
      rankingUrl.searchParams.set("region", backendBody.region);
      rankingUrl.searchParams.set("page", String(backendBody.page));
      rankingUrl.searchParams.set("count", String(backendBody.count));
      if (backendBody.language) rankingUrl.searchParams.set("language", backendBody.language);
      if (backendBody.collection) rankingUrl.searchParams.set("collection", backendBody.collection);
      if (backendBody.order) rankingUrl.searchParams.set("order", backendBody.order);
      rankingUrl.searchParams.set("calculateTotalPrice", String(Boolean(backendBody.calculateTotalPrice)));
      if (Array.isArray(backendBody.filters) && backendBody.filters.length > 0) {
        rankingUrl.searchParams.set("filters", JSON.stringify(backendBody.filters));
      }

      const rankingHeaders: Record<string, string> = { ...baseHeaders };
      if (env.RANKING_SERVICE_TOKEN) {
        rankingHeaders["authorization"] = `Bearer ${env.RANKING_SERVICE_TOKEN}`;
      }

      return fetch(rankingUrl.toString(), {
        method: "GET",
        headers: rankingHeaders,
        cf: { cacheTtl: 0, cacheEverything: false },
      });
    };
  
    try {
      const shouldUseRanking = Boolean(env.RANKING_SERVICE_URL && backendBody.category);
      let backendResponse: Response | null = null;

      if (shouldUseRanking) {
        try {
          backendResponse = await sendRankingRequest();
          if (backendResponse && !backendResponse.ok) {
            console.warn(`Ranking service responded with ${backendResponse.status}; falling back to catalog list`);
          }
        } catch (err) {
          console.error("Ranking service request failed; falling back to catalog list", err);
        }
      }

      if (!backendResponse || !backendResponse.ok) {
        backendResponse = await sendCatalogRequest();
      }
  
      if (!backendResponse.ok) {
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
          // Prevent Cloudflare CDN/browser caching; we rely on Worker Cache API instead
          "cache-control": "private, max-age=0, s-maxage=0, must-revalidate",
        },
      });
  
      edgeResponse.headers.set("x-edge-cache", "MISS");
      addCorsHeaders(edgeResponse, env, request);
  
      if (cacheEnabled) {
        ctx.waitUntil(cache.put(cacheKey, edgeResponse.clone()));
      }
  
      return edgeResponse;
  
    } catch (error) {
      console.error(`Fetch failed for PLP:`, error);
      return new Response(JSON.stringify({ error: "Service Unavailable" }), {
          status: 502,
          headers: { "Content-Type": "application/json" }
      });
    }
  }
