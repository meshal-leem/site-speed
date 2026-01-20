/**
 * Interface for Environment Variables
 * Ensures type safety when accessing variables defined in wrangler.toml
 */
interface Env {
  BACKEND_PRODUCT_URL: string;
  BACKEND_PRODUCT_LIST_URL: string;
  BACKEND_CATEGORY_TREE_URL: string;
  ALLOWED_ORIGIN: string;
  ALLOWED_ORIGINS?: string;
  ADMIN_USER: string;
  ADMIN_PASS: string;
  WEBHOOK_TOKEN: string;
  CACHE_META: KVNamespace;
  VISIT_COUNTER: DurableObjectNamespace;
  CACHE_DB: D1Database;
}

const CACHE_TTL_SECONDS = 60 * 60 * 24 * 7;

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

      if (url.pathname === "/webhook/vtex" && request.method === "POST") {
        const authError = requireWebhookAuth(request, env);
        if (authError) {
          return authError;
        }
        return await handleVtexWebhook(request, env, ctx);
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
      const notFoundResponse = new Response(JSON.stringify({ error: "Not found" }), {
        status: 404,
        headers: { "Content-Type": "application/json" },
      });
      return withCors(notFoundResponse, env, request);

    } catch (err: any) {
      // 4. Global Error Trap (Safety Net)
      console.error(`Unhandled Worker Error: ${err.message}`, err.stack);
      const errorResponse = new Response(JSON.stringify({ error: "Internal Server Error" }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
      return withCors(errorResponse, env, request);
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
    "access-control-allow-methods": "GET, POST, OPTIONS",
    "access-control-allow-headers": "Content-Type, X-FBC, X-FBP",
    "access-control-max-age": "86400",
  };

  const origin = request.headers.get("origin");
  const allowList = (env.ALLOWED_ORIGINS || env.ALLOWED_ORIGIN || "")
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
  if (origin && allowList.includes(origin)) {
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
  console.log("PDP POST request", { url: url.toString(), slug });
  const body = await parseJsonBody(request);
  if (!body) {
    const invalidResponse = new Response(JSON.stringify({ error: "Invalid JSON body" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
    return withCors(invalidResponse, env, request);
  }
  const requestBodyHash = await hashRequestBody(body);

  const region = (String(body.region || url.searchParams.get("region") || "sa")).toLowerCase();
  const language = (String(body.language || url.searchParams.get("language") || "en")).toLowerCase();

  const cacheKeyUrl = `${url.origin}/products/${slug}?region=${region}&language=${language}`;
  ctx.waitUntil(incrementVisit(env, "pdp", cacheKeyUrl));
  console.log("PDP POST cache key", { cacheKeyUrl });

  const totalStart = Date.now();
  const edgeStart = Date.now();
  const edgeCached = await getEdgeCache(cacheKeyUrl);
  if (edgeCached) {
    const timings = { edge: Date.now() - edgeStart, d1: 0, total: Date.now() - totalStart };
    console.log("PDP POST edge cache HIT", { cacheKeyUrl });
    const res = new Response(edgeCached.body, edgeCached);
    res.headers.set("x-edge-cache", "HIT");
    addCacheDebugHeaders(res, "edge", timings);
    addCorsHeaders(res, env, request);
    ctx.waitUntil(ensureCacheMetaExists(env, "pdp", cacheKeyUrl, { synced: false, source: "post", requestBodyHash }));
    return res;
  }

  const d1Start = Date.now();
  const cachedResponse = await getSharedCache(env, cacheKeyUrl);
  if (cachedResponse) {
    const timings = { edge: Date.now() - edgeStart, d1: Date.now() - d1Start, total: Date.now() - totalStart };
    console.log("PDP POST cache HIT", { cacheKeyUrl });
    const cachedText = await cachedResponse.clone().text();
    ctx.waitUntil(putEdgeCacheFromText(cacheKeyUrl, cachedText, cachedResponse.status, Object.fromEntries(cachedResponse.headers)));
    const res = new Response(cachedResponse.body, cachedResponse);
    res.headers.set("x-edge-cache", "HIT");
    addCacheDebugHeaders(res, "d1", timings);
    addCorsHeaders(res, env, request);
    ctx.waitUntil(ensureCacheMetaExists(env, "pdp", cacheKeyUrl, { synced: false, source: "post", requestBodyHash }));
    return res;
  }
  const timingsMiss = { edge: Date.now() - edgeStart, d1: Date.now() - d1Start, total: Date.now() - totalStart };
  console.log("PDP POST cache MISS", { cacheKeyUrl });

  try {
    const backendResponse = await fetchPdpBackend(env, request, slug, region, language);
    if (!backendResponse.ok) {
      console.error(`Backend returned ${backendResponse.status} for slug: ${slug}`);
      const errorResponse = new Response(backendResponse.body, {
        status: backendResponse.status,
        headers: backendResponse.headers
      });
      errorResponse.headers.set("cache-control", "no-store");
      return withCors(errorResponse, env, request);
    }

    const textBody = await backendResponse.text();
    const edgeResponse = new Response(textBody, {
      status: backendResponse.status,
      headers: {
        "content-type": backendResponse.headers.get("content-type") || "application/json",
        "cache-control": `public, max-age=${CACHE_TTL_SECONDS}, stale-while-revalidate=${CACHE_TTL_SECONDS}`,
      },
    });

    edgeResponse.headers.set("x-edge-cache", "MISS");
    addCacheDebugHeaders(edgeResponse, "miss", timingsMiss);
    addCorsHeaders(edgeResponse, env, request);

    ctx.waitUntil(putSharedCacheFromText(env, cacheKeyUrl, textBody, edgeResponse.status, Object.fromEntries(edgeResponse.headers)));
    ctx.waitUntil(putEdgeCacheFromText(cacheKeyUrl, textBody, edgeResponse.status, Object.fromEntries(edgeResponse.headers)));
    ctx.waitUntil(recordCacheMeta(env, "pdp", cacheKeyUrl, {
      synced: false,
      source: "post",
      requestBodyHash,
    }));

    return edgeResponse;
  } catch (error) {
    console.error(`Fetch failed for PDP ${slug}:`, error);
    const errorResponse = new Response(JSON.stringify({ error: "Service Unavailable" }), {
      status: 502,
      headers: { "Content-Type": "application/json", "cache-control": "no-store" }
    });
    return withCors(errorResponse, env, request);
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
    const invalidResponse = new Response(JSON.stringify({ error: "Invalid JSON body" }), {
      status: 400,
      headers: { "Content-Type": "application/json" },
    });
    return withCors(invalidResponse, env, request);
  }
  const requestBodyHash = await hashRequestBody(body);

  const backendBody = normalizePlpBody(body);
  const isCollection404 = backendBody.collection === "404";
  if (isCollection404) {
    console.log("PLP POST request", { url: url.toString() });
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
  const cacheKeyUrl = buildPlpCacheKeyUrl(url.origin, backendBody);
  ctx.waitUntil(incrementVisit(env, "plp", cacheKeyUrl));
  if (isCollection404) {
    console.log("PLP POST cache key", { cacheKeyUrl });
    logCollection404("plp_post_request", request, url, {
      cacheKeyUrl,
      backendBody,
      requestBody: body,
      requestBodyHash,
    });
  }
  if (isCollection404) {
    try {
      const backendResponse = await fetchPlpBackend(env, request, backendBody, backendHeaders, {
        noCache: true,
      });
      const bodyText = await backendResponse.text();
      const response = new Response(bodyText, {
        status: backendResponse.status,
        headers: {
          "content-type": backendResponse.headers.get("content-type") || "application/json",
          "cache-control": "no-store",
        },
      });
      addCorsHeaders(response, env, request);
      logCollection404Response("plp_post_nocache", response, bodyText, {
        cacheKeyUrl,
        backendBody,
      });
      return response;
    } catch (error) {
      console.error(`Fetch failed for PLP:`, error);
      const errorResponse = new Response(JSON.stringify({ error: "Service Unavailable" }), {
        status: 502,
        headers: { "Content-Type": "application/json", "cache-control": "no-store" }
      });
      return withCors(errorResponse, env, request);
    }
  }

  const totalStart = Date.now();
  const edgeStart = Date.now();
  const edgeCached = await getEdgeCache(cacheKeyUrl);
  if (edgeCached) {
    const timings = { edge: Date.now() - edgeStart, d1: 0, total: Date.now() - totalStart };
    if (isCollection404) {
      console.log("PLP POST edge cache HIT", { cacheKeyUrl });
      const cachedText = await edgeCached.clone().text();
      logCollection404Response("plp_post_edge_hit", edgeCached, cachedText, {
        cacheKeyUrl,
        backendBody,
      });
    }
    const res = new Response(edgeCached.body, edgeCached);
    res.headers.set("x-edge-cache", "HIT");
    addCacheDebugHeaders(res, "edge", timings);
    addCorsHeaders(res, env, request);
    ctx.waitUntil(ensureCacheMetaExists(env, "plp", cacheKeyUrl, { synced: false, source: "post", requestBodyHash }));
    return res;
  }

  const d1Start = Date.now();
  const cachedResponse = await getSharedCache(env, cacheKeyUrl);
  if (cachedResponse) {
    const timings = { edge: Date.now() - edgeStart, d1: Date.now() - d1Start, total: Date.now() - totalStart };
    if (isCollection404) {
      console.log("PLP POST cache HIT", { cacheKeyUrl });
    }
    const cachedText = await cachedResponse.clone().text();
    const itemCount = extractPlpItemCount(cachedText);
    if (isCollection404) {
      logCollection404Response("plp_post_d1_hit", cachedResponse, cachedText, {
        cacheKeyUrl,
        backendBody,
      });
    }
    ctx.waitUntil(putEdgeCacheFromText(cacheKeyUrl, cachedText, cachedResponse.status, Object.fromEntries(cachedResponse.headers)));
    const res = new Response(cachedResponse.body, cachedResponse);
    res.headers.set("x-edge-cache", "HIT");
    addCacheDebugHeaders(res, "d1", timings);
    addCorsHeaders(res, env, request);
    ctx.waitUntil(ensureCacheMetaExists(env, "plp", cacheKeyUrl, { synced: false, source: "post", requestBodyHash, itemCount }));
    return res;
  }
  const timingsMiss = { edge: Date.now() - edgeStart, d1: Date.now() - d1Start, total: Date.now() - totalStart };
  if (isCollection404) {
    console.log("PLP POST cache MISS", { cacheKeyUrl });
  }

  try {
    const debug = request.headers.get("x-prewarm-debug") === "1";
    const backendResponse = await fetchPlpBackend(env, request, backendBody, backendHeaders, {
      debug,
    });
    if (!backendResponse.ok) {
      console.error(`Backend list returned ${backendResponse.status}`);
      if (isCollection404) {
        const errorText = await backendResponse.clone().text();
        logCollection404Response("plp_post_backend_error", backendResponse, errorText, {
          cacheKeyUrl,
          backendBody,
        });
      }
      const errorResponse = new Response(backendResponse.body, {
        status: backendResponse.status,
        headers: backendResponse.headers
      });
      errorResponse.headers.set("cache-control", "no-store");
      return withCors(errorResponse, env, request);
    }

    const textBody = await backendResponse.text();
    const edgeResponse = new Response(textBody, {
      status: backendResponse.status,
      headers: {
        "content-type": backendResponse.headers.get("content-type") || "application/json",
        "cache-control": `public, max-age=${CACHE_TTL_SECONDS}, stale-while-revalidate=${CACHE_TTL_SECONDS}`,
      },
    });

    edgeResponse.headers.set("x-edge-cache", "MISS");
    addCacheDebugHeaders(edgeResponse, "miss", timingsMiss);
    addCorsHeaders(edgeResponse, env, request);
    if (isCollection404) {
      logCollection404Response("plp_post_miss", edgeResponse, textBody, {
        cacheKeyUrl,
        backendBody,
      });
    }

    ctx.waitUntil(putSharedCacheFromText(env, cacheKeyUrl, textBody, edgeResponse.status, Object.fromEntries(edgeResponse.headers)));
    ctx.waitUntil(putEdgeCacheFromText(cacheKeyUrl, textBody, edgeResponse.status, Object.fromEntries(edgeResponse.headers)));
    ctx.waitUntil(recordCacheMeta(env, "plp", cacheKeyUrl, {
      synced: false,
      source: "post",
      requestBodyHash,
      itemCount,
    }));

    return edgeResponse;
  } catch (error) {
    console.error(`Fetch failed for PLP:`, error);
    const errorResponse = new Response(JSON.stringify({ error: "Service Unavailable" }), {
      status: 502,
      headers: { "Content-Type": "application/json", "cache-control": "no-store" }
    });
    return withCors(errorResponse, env, request);
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
  console.log("PDP GET request", { url: url.toString(), slug });

  const region = (url.searchParams.get("region") || "sa").toLowerCase();
  const language = (url.searchParams.get("language") || "en").toLowerCase();

  // Create a cache key specific to this request variation
  const cacheKeyUrl = `${url.origin}/products/${slug}?region=${region}&language=${language}`;
  ctx.waitUntil(incrementVisit(env, "pdp", cacheKeyUrl));
  console.log("PDP GET cache key", { cacheKeyUrl });

  // 1. Try Cache
  const totalStart = Date.now();
  const edgeStart = Date.now();
  const edgeCached = await getEdgeCache(cacheKeyUrl);
  if (edgeCached) {
    const timings = { edge: Date.now() - edgeStart, d1: 0, total: Date.now() - totalStart };
    console.log("PDP GET edge cache HIT", { cacheKeyUrl });
    const res = new Response(edgeCached.body, edgeCached);
    res.headers.set("x-edge-cache", "HIT");
    addCacheDebugHeaders(res, "edge", timings);
    addCorsHeaders(res, env, request);
    ctx.waitUntil(ensureCacheMetaExists(env, "pdp", cacheKeyUrl, { synced: false, source: "get" }));
    return res;
  }

  const d1Start = Date.now();
  const cachedResponse = await getSharedCache(env, cacheKeyUrl);
  if (cachedResponse) {
    const timings = { edge: Date.now() - edgeStart, d1: Date.now() - d1Start, total: Date.now() - totalStart };
    console.log("PDP GET cache HIT", { cacheKeyUrl });
    const cachedText = await cachedResponse.clone().text();
    ctx.waitUntil(putEdgeCacheFromText(cacheKeyUrl, cachedText, cachedResponse.status, Object.fromEntries(cachedResponse.headers)));
    const res = new Response(cachedResponse.body, cachedResponse);
    res.headers.set("x-edge-cache", "HIT");
    addCacheDebugHeaders(res, "d1", timings);
    addCorsHeaders(res, env, request);
    ctx.waitUntil(ensureCacheMetaExists(env, "pdp", cacheKeyUrl, { synced: false, source: "get" }));
    return res;
  }
  const timingsMiss = { edge: Date.now() - edgeStart, d1: Date.now() - d1Start, total: Date.now() - totalStart };
  console.log("PDP GET cache MISS", { cacheKeyUrl });

  // 2. Prepare Backend Request
  try {
    const backendResponse = await fetchPdpBackend(env, request, slug, region, language);

    // 3. Handle Backend Errors
    // If backend returns 4xx/5xx, we typically want to pass that through but NOT cache it aggressively
    // or strictly avoid caching errors depending on requirements.
    if (!backendResponse.ok) {
        console.error(`Backend returned ${backendResponse.status} for slug: ${slug}`);
        // We clone here because we might read the body or pass it through
        const errorResponse = new Response(backendResponse.body, {
            status: backendResponse.status,
            headers: backendResponse.headers
        });
        errorResponse.headers.set("cache-control", "no-store");
        return withCors(errorResponse, env, request);
    }

    const textBody = await backendResponse.text();

    const edgeResponse = new Response(textBody, {
      status: backendResponse.status,
      headers: {
        "content-type": backendResponse.headers.get("content-type") || "application/json",
        "cache-control": `public, max-age=${CACHE_TTL_SECONDS}, stale-while-revalidate=${CACHE_TTL_SECONDS}`,
      },
    });

    edgeResponse.headers.set("x-edge-cache", "MISS");
    addCacheDebugHeaders(edgeResponse, "miss", timingsMiss);
    addCorsHeaders(edgeResponse, env, request);

    // 4. Update Cache (Non-blocking)
    ctx.waitUntil(putSharedCacheFromText(env, cacheKeyUrl, textBody, edgeResponse.status, Object.fromEntries(edgeResponse.headers)));
    ctx.waitUntil(putEdgeCacheFromText(cacheKeyUrl, textBody, edgeResponse.status, Object.fromEntries(edgeResponse.headers)));
    ctx.waitUntil(recordCacheMeta(env, "pdp", cacheKeyUrl, { synced: false, source: "get" }));

    return edgeResponse;

  } catch (error) {
    console.error(`Fetch failed for PDP ${slug}:`, error);
    const errorResponse = new Response(JSON.stringify({ error: "Service Unavailable" }), {
        status: 502,
        headers: { "Content-Type": "application/json", "cache-control": "no-store" }
    });
    return withCors(errorResponse, env, request);
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

  // Normalize cache key so equivalent query params don't create duplicates.
  const backendBody = buildPlpBodyFromUrl(url);
  const isCollection404 = backendBody.collection === "404";
  if (isCollection404) {
    console.log("PLP GET request", { url: url.toString() });
  }
  const cacheKeyUrl = buildPlpCacheKeyUrl(url.origin, backendBody);
  ctx.waitUntil(incrementVisit(env, "plp", cacheKeyUrl));
  if (isCollection404) {
    console.log("PLP GET cache key", { cacheKeyUrl });
    logCollection404("plp_get_request", request, url, {
      cacheKeyUrl,
      backendBody,
    });
  }
  if (isCollection404) {
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
      const backendResponse = await fetchPlpBackend(env, request, backendBody, backendHeaders, {
        noCache: true,
      });
      const bodyText = await backendResponse.text();
      const response = new Response(bodyText, {
        status: backendResponse.status,
        headers: {
          "content-type": backendResponse.headers.get("content-type") || "application/json",
          "cache-control": "no-store",
        },
      });
      addCorsHeaders(response, env, request);
      logCollection404Response("plp_get_nocache", response, bodyText, {
        cacheKeyUrl,
        backendBody,
      });
      return response;
    } catch (error) {
      console.error(`Fetch failed for PLP:`, error);
      const errorResponse = new Response(JSON.stringify({ error: "Service Unavailable" }), {
        status: 502,
        headers: { "Content-Type": "application/json", "cache-control": "no-store" }
      });
      return withCors(errorResponse, env, request);
    }
  }

  // 1. Try Cache
  const totalStart = Date.now();
  const edgeStart = Date.now();
  const edgeCached = await getEdgeCache(cacheKeyUrl);
  if (edgeCached) {
    const timings = { edge: Date.now() - edgeStart, d1: 0, total: Date.now() - totalStart };
    if (isCollection404) {
      console.log("PLP GET edge cache HIT", { cacheKeyUrl });
      const cachedText = await edgeCached.clone().text();
      logCollection404Response("plp_get_edge_hit", edgeCached, cachedText, {
        cacheKeyUrl,
        backendBody,
      });
    }
    const res = new Response(edgeCached.body, edgeCached);
    res.headers.set("x-edge-cache", "HIT");
    addCacheDebugHeaders(res, "edge", timings);
    addCorsHeaders(res, env, request);
    ctx.waitUntil(ensureCacheMetaExists(env, "plp", cacheKeyUrl, { synced: false, source: "get" }));
    return res;
  }

  const d1Start = Date.now();
  const cachedResponse = await getSharedCache(env, cacheKeyUrl);
  if (cachedResponse) {
    const timings = { edge: Date.now() - edgeStart, d1: Date.now() - d1Start, total: Date.now() - totalStart };
    if (isCollection404) {
      console.log("PLP GET cache HIT", { cacheKeyUrl });
    }
    const cachedText = await cachedResponse.clone().text();
    const itemCount = extractPlpItemCount(cachedText);
    if (isCollection404) {
      logCollection404Response("plp_get_d1_hit", cachedResponse, cachedText, {
        cacheKeyUrl,
        backendBody,
      });
    }
    ctx.waitUntil(putEdgeCacheFromText(cacheKeyUrl, cachedText, cachedResponse.status, Object.fromEntries(cachedResponse.headers)));
    const res = new Response(cachedResponse.body, cachedResponse);
    res.headers.set("x-edge-cache", "HIT");
    addCacheDebugHeaders(res, "d1", timings);
    addCorsHeaders(res, env, request);
    ctx.waitUntil(ensureCacheMetaExists(env, "plp", cacheKeyUrl, { synced: false, source: "get", itemCount }));
    return res;
  }
  const timingsMiss = { edge: Date.now() - edgeStart, d1: Date.now() - d1Start, total: Date.now() - totalStart };
  if (isCollection404) {
    console.log("PLP GET cache MISS", { cacheKeyUrl });
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
      if (isCollection404) {
        const errorText = await backendResponse.clone().text();
        logCollection404Response("plp_get_backend_error", backendResponse, errorText, {
          cacheKeyUrl,
          backendBody,
        });
      }
      const errorResponse = new Response(backendResponse.body, {
        status: backendResponse.status,
        headers: backendResponse.headers
      });
      errorResponse.headers.set("cache-control", "no-store");
      return withCors(errorResponse, env, request);
    }

    const textBody = await backendResponse.text();

    const edgeResponse = new Response(textBody, {
      status: backendResponse.status,
      headers: {
        "content-type": backendResponse.headers.get("content-type") || "application/json",
        "cache-control": `public, max-age=${CACHE_TTL_SECONDS}, stale-while-revalidate=${CACHE_TTL_SECONDS}`,
      },
    });

    edgeResponse.headers.set("x-edge-cache", "MISS");
    addCacheDebugHeaders(edgeResponse, "miss", timingsMiss);
    addCorsHeaders(edgeResponse, env, request);
    if (isCollection404) {
      logCollection404Response("plp_get_miss", edgeResponse, textBody, {
        cacheKeyUrl,
        backendBody,
      });
    }

    ctx.waitUntil(putSharedCacheFromText(env, cacheKeyUrl, textBody, edgeResponse.status, Object.fromEntries(edgeResponse.headers)));
    ctx.waitUntil(putEdgeCacheFromText(cacheKeyUrl, textBody, edgeResponse.status, Object.fromEntries(edgeResponse.headers)));
    ctx.waitUntil(recordCacheMeta(env, "plp", cacheKeyUrl, { synced: false, source: "get", itemCount }));

    return edgeResponse;

  } catch (error) {
    console.error(`Fetch failed for PLP:`, error);
    const errorResponse = new Response(JSON.stringify({ error: "Service Unavailable" }), {
        status: 502,
        headers: { "Content-Type": "application/json", "cache-control": "no-store" }
    });
    return withCors(errorResponse, env, request);
  }
}

function addCorsHeaders(response: Response, env: Env, request: Request) {
  const origin = request.headers.get("origin");
  const allowList = (env.ALLOWED_ORIGINS || env.ALLOWED_ORIGIN || "")
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
  if (origin && allowList.includes(origin)) {
    response.headers.set("access-control-allow-origin", origin);
    response.headers.set("vary", "Origin");
  }
  response.headers.set("access-control-allow-credentials", "true");
}

function withCors(response: Response, env: Env, request: Request): Response {
  addCorsHeaders(response, env, request);
  return response;
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
  itemCount?: number | null;
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

interface CacheSyncJob {
  id: string;
  status: "running" | "completed" | "failed";
  startedAt: string;
  finishedAt?: string | null;
  type: CacheType | "all";
  prefixes: string[];
  prefixIndex: number;
  cursor: string | null;
  total: number;
  processed: number;
  synced: number;
  failed: number;
  batchLimit: number;
  concurrency: number;
  lastBatchMs?: number | null;
  totalBatchMs?: number;
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

type WebhookPdpItem = {
  slug: string;
  region?: string;
  regions?: string[];
  language?: string;
  languages?: string[];
  body?: unknown;
  status?: number;
  headers?: Record<string, string>;
};

type WebhookPlpItem = {
  category: string;
  collection?: string;
  region?: string;
  regions?: string[];
  language?: string;
  languages?: string[];
  page?: number;
  count?: number;
  order?: string;
  filters?: unknown[];
  calculateTotalPrice?: boolean;
  body?: unknown;
  status?: number;
  headers?: Record<string, string>;
};

function normalizeWebhookRegions(itemRegion?: string, itemRegions?: string[]): string[] {
  if (Array.isArray(itemRegions) && itemRegions.length > 0) {
    return itemRegions.map((value) => value.toLowerCase());
  }
  if (itemRegion) {
    return [itemRegion.toLowerCase()];
  }
  return ["sa"];
}

function normalizeWebhookLanguages(itemLanguage?: string, itemLanguages?: string[]): string[] {
  if (Array.isArray(itemLanguages) && itemLanguages.length > 0) {
    return itemLanguages.map((value) => value.toLowerCase());
  }
  if (itemLanguage) {
    return [itemLanguage.toLowerCase()];
  }
  return ["en"];
}

function buildWebhookCacheBody(body: unknown): string {
  if (typeof body === "string") {
    return body;
  }
  return JSON.stringify(body ?? null);
}

function buildWebhookCacheHeaders(headers?: Record<string, string>): Record<string, string> {
  const normalized: Record<string, string> = {
    "content-type": "application/json",
  };
  if (headers && typeof headers === "object") {
    for (const [key, value] of Object.entries(headers)) {
      normalized[key.toLowerCase()] = value;
    }
  }
  return normalized;
}

async function handleVtexWebhook(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
  const payload = await parseJsonBody(request);
  if (!payload) {
    return jsonResponse({ error: "Invalid JSON body" }, 400);
  }

  const pdpItems = Array.isArray(payload.pdp) ? payload.pdp as WebhookPdpItem[] : [];
  const plpItems = Array.isArray(payload.plp) ? payload.plp as WebhookPlpItem[] : [];
  const origin = new URL(request.url).origin;

  let processed = 0;
  let synced = 0;
  let failed = 0;
  const errors: { type: string; message: string; item: string }[] = [];

  for (const item of pdpItems) {
    processed += 1;
    if (!item || !item.slug) {
      failed += 1;
      errors.push({ type: "pdp", message: "Missing slug", item: "" });
      continue;
    }
    if (item.body === undefined) {
      failed += 1;
      errors.push({ type: "pdp", message: "Missing body", item: item.slug });
      continue;
    }
    const regions = normalizeWebhookRegions(item.region, item.regions);
    const languages = normalizeWebhookLanguages(item.language, item.languages);
    for (const region of regions) {
      for (const language of languages) {
        const cacheKeyUrl = `${origin}/products/${encodeURIComponent(item.slug)}?region=${region}&language=${language}`;
        const rawBody = buildWebhookCacheBody(item.body);
        const itemCount = extractPlpItemCount(rawBody);
        const headers = buildWebhookCacheHeaders(item.headers);
        const status = typeof item.status === "number" ? item.status : 200;
        await putSharedCacheFromText(env, cacheKeyUrl, rawBody, status, headers);
        await putEdgeCacheFromText(cacheKeyUrl, rawBody, status, headers);
        await recordCacheMeta(env, "pdp", cacheKeyUrl, {
          synced: true,
          source: "post",
          requestBodyHash: null,
        });
        synced += 1;
      }
    }
  }

  for (const item of plpItems) {
    processed += 1;
    if (!item || !item.category) {
      failed += 1;
      errors.push({ type: "plp", message: "Missing category", item: "" });
      continue;
    }
    if (item.body === undefined) {
      failed += 1;
      errors.push({ type: "plp", message: "Missing body", item: item.category });
      continue;
    }
    const regions = normalizeWebhookRegions(item.region, item.regions);
    const languages = normalizeWebhookLanguages(item.language, item.languages);
    for (const region of regions) {
      for (const language of languages) {
        const plpBody = normalizePlpBody({
          page: item.page ?? 1,
          count: item.count ?? 24,
          category: item.category,
          region,
          language,
          order: item.order ?? "OrderByScoreDESC",
          collection: item.collection,
          filters: item.filters ?? [],
          calculateTotalPrice: item.calculateTotalPrice === true,
        });
        const cacheKeyUrl = buildPlpCacheKeyUrl(origin, plpBody);
        const rawBody = buildWebhookCacheBody(item.body);
        const headers = buildWebhookCacheHeaders(item.headers);
        const status = typeof item.status === "number" ? item.status : 200;
        await putSharedCacheFromText(env, cacheKeyUrl, rawBody, status, headers);
        await putEdgeCacheFromText(cacheKeyUrl, rawBody, status, headers);
        await recordCacheMeta(env, "plp", cacheKeyUrl, {
          synced: true,
          source: "post",
          requestBodyHash: null,
          itemCount,
        });
        synced += 1;
      }
    }
  }

  return jsonResponse({ status: "ok", processed, synced, failed, errors }, 200);
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
    const limitParam = url.searchParams.get("limit");
    const cursorParam = url.searchParams.get("cursor");
    const limit = limitParam ? Math.max(1, Math.min(1000, parseInt(limitParam, 10))) : null;
    const cursor = cursorParam || null;
    const includeVisits = url.searchParams.get("visits") === "1";

    let items: CacheMeta[] = [];
    let nextCursor: string | null = null;
    if (limit || cursor) {
      const page = await listCacheMetaPaginated(
        env,
        type === "all" ? null : (type as CacheType),
        limit ?? 100,
        cursor
      );
      items = page.items;
      nextCursor = page.nextCursor;
    } else {
      items = await listCacheMeta(env, type === "all" ? null : (type as CacheType));
    }

    if (includeVisits) {
      items = await Promise.all(items.map(async (item) => {
        const visits = await getVisitCount(env, item.type, item.cacheKeyUrl);
        return { ...item, visits };
      }));
    }

    return jsonResponse({ items, nextCursor }, 200);
  }

  if (pathname === "/admin/cache/search" && request.method === "GET") {
    const type = (url.searchParams.get("type") || "all").toLowerCase();
    const limitParam = url.searchParams.get("limit");
    const limit = limitParam ? Math.max(1, Math.min(200, parseInt(limitParam, 10))) : 50;
    const cursorParam = url.searchParams.get("cursor");
    const cursor = cursorParam || null;
    const scanParam = url.searchParams.get("scan");
    const maxPages = scanParam ? Math.max(1, Math.min(20, parseInt(scanParam, 10))) : 2;
    const queryRaw = url.searchParams.get("query") || url.searchParams.get("q") || "";
    const query = queryRaw.trim().toLowerCase();
    const includeVisits = url.searchParams.get("visits") === "1";
    const sync = url.searchParams.get("sync") === "1";

    if (!query) {
      return jsonResponse({ error: "Missing query" }, 400);
    }

    const searchResult = await searchCacheMetaPaginated(
      env,
      type === "all" ? null : (type as CacheType),
      query,
      limit,
      cursor,
      maxPages
    );
    let items = searchResult.items;

    if (includeVisits) {
      items = await Promise.all(items.map(async (item) => {
        const visits = await getVisitCount(env, item.type, item.cacheKeyUrl);
        return { ...item, visits };
      }));
    }

    let synced = 0;
    let failed = 0;
    if (sync) {
      for (const item of items) {
        try {
          await syncCacheForMeta(env, ctx, item);
          synced += 1;
        } catch {
          failed += 1;
        }
      }
    }

    return jsonResponse({ items, synced, failed, nextCursor: searchResult.nextCursor }, 200);
  }

  if (pathname === "/admin/cache/stats" && request.method === "GET") {
    const type = (url.searchParams.get("type") || "all").toLowerCase();
    if (type === "all") {
      const plp = await countCacheKeysForPrefix(env, "plp:");
      const pdp = await countCacheKeysForPrefix(env, "pdp:");
      return jsonResponse({ total: plp + pdp, plp, pdp }, 200);
    }
    const prefix = type === "pdp" ? "pdp:" : "plp:";
    const total = await countCacheKeysForPrefix(env, prefix);
    return jsonResponse({ total }, 200);
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

    const cacheResult = await getCachedResponse(env, meta.cacheKeyUrl);
    const freshResult = await getFreshResponseForMeta(env, meta);

    return jsonResponse({
      meta,
      cache: cacheResult,
      fresh: freshResult,
    }, 200);
  }

  if (pathname === "/admin/cache/lookup" && request.method === "GET") {
    const inputUrl = url.searchParams.get("url");
    if (!inputUrl) {
      return jsonResponse({ error: "Missing url" }, 400);
    }
    const resolved = resolveCacheKeyFromUrl(inputUrl, url.origin);
    if (!resolved) {
      return jsonResponse({ error: "Invalid url" }, 400);
    }
    const { type, cacheKeyUrl } = resolved;
    const id = await hashCacheKey(type, cacheKeyUrl);
    const metaKey = `${type}:${id}`;
    const meta = await env.CACHE_META.get(metaKey, "json") as CacheMeta | null;
    const now = formatUaeNow();
    const fallbackMeta: CacheMeta = meta || {
      id,
      type,
      cacheKeyUrl,
      createdAt: now,
      updatedAt: now,
    };
    const cacheResult = await getCachedResponse(env, cacheKeyUrl);
    const freshResult = await getFreshResponseForMeta(env, fallbackMeta);
    return jsonResponse({
      meta: fallbackMeta,
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
    const limitParam = url.searchParams.get("limit");
    const batchLimit = limitParam ? Math.max(1, Math.min(200, parseInt(limitParam, 10))) : 10;
    const concurrencyParam = url.searchParams.get("concurrency");
    const concurrency = concurrencyParam ? Math.max(1, Math.min(20, parseInt(concurrencyParam, 10))) : 5;
    const job = await createCacheSyncJob(env, type === "all" ? "all" : (type as CacheType), batchLimit, concurrency);
    const runPromise = runCacheSyncBatch(env, ctx, job.id);
    ctx.waitUntil(runPromise);
    return jsonResponse({ status: "started", jobId: job.id }, 202);
  }

  if (pathname === "/admin/cache/sync/status" && request.method === "GET") {
    const id = url.searchParams.get("id");
    if (!id) {
      return jsonResponse({ error: "Missing id" }, 400);
    }
    const job = await getCacheSyncJob(env, id);
    if (!job) {
      return jsonResponse({ error: "Not found" }, 404);
    }
    return jsonResponse(job, 200);
  }

  if (pathname === "/admin/cache/sync/latest" && request.method === "GET") {
    const job = await getLatestCacheSyncJob(env);
    if (!job) {
      return jsonResponse({ error: "Not found" }, 404);
    }
    return jsonResponse(job, 200);
  }

  if (pathname === "/admin/cache/sync/continue" && request.method === "POST") {
    const id = url.searchParams.get("id");
    if (!id) {
      return jsonResponse({ error: "Missing id" }, 400);
    }
    const job = await runCacheSyncBatch(env, ctx, id);
    if (!job) {
      return jsonResponse({ error: "Not found" }, 404);
    }
    return jsonResponse(job, 200);
  }

  if (pathname === "/admin/cache/sync/stop" && request.method === "POST") {
    const id = url.searchParams.get("id");
    if (!id) {
      return jsonResponse({ error: "Missing id" }, 400);
    }
    const job = await getCacheSyncJob(env, id);
    if (!job) {
      return jsonResponse({ error: "Not found" }, 404);
    }
    if (job.status === "running") {
      job.status = "failed";
      job.finishedAt = formatUaeNow();
      job.message = "Stopped by admin";
      await updateCacheSyncJob(env, job);
    }
    return jsonResponse(job, 200);
  }

  if (pathname === "/admin/cache/delete" && request.method === "POST") {
    const id = url.searchParams.get("id");
    if (!id) {
      return jsonResponse({ error: "Missing id" }, 400);
    }
    const deleted = await deleteCacheById(env, id);
    if (!deleted) {
      return jsonResponse({ error: "Not found" }, 404);
    }
    return jsonResponse({ deleted: true }, 200);
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
  options: {
    synced: boolean;
    source?: "get" | "post";
    requestBodyHash?: string | null;
    itemCount?: number | null;
  }
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
    itemCount: options.itemCount ?? existing?.itemCount ?? null,
  };

  await kvPutWithRetry(env, key, JSON.stringify(meta));
}

async function ensureCacheMetaExists(
  env: Env,
  type: CacheType,
  cacheKeyUrl: string,
  options: {
    synced: boolean;
    source?: "get" | "post";
    requestBodyHash?: string | null;
    itemCount?: number | null;
  }
): Promise<void> {
  const id = await hashCacheKey(type, cacheKeyUrl);
  const key = `${type}:${id}`;
  const existing = await env.CACHE_META.get(key, "json") as CacheMeta | null;
  if (existing) {
    return;
  }
  await recordCacheMeta(env, type, cacheKeyUrl, { ...options });
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

async function getSharedCache(env: Env, cacheKeyUrl: string): Promise<Response | null> {
  const result = await env.CACHE_DB.prepare(
    "SELECT status, headers, body, expires_at FROM cache_entries WHERE cache_key = ?"
  ).bind(cacheKeyUrl).first<{
    status: number;
    headers: string;
    body: string;
    expires_at: number;
  }>();

  if (!result) {
    return null;
  }

  if (result.status !== 200) {
    await deleteSharedCache(env, cacheKeyUrl);
    return null;
  }

  const now = Date.now();
  if (result.expires_at && result.expires_at <= now) {
    await deleteSharedCache(env, cacheKeyUrl);
    return null;
  }

  const headersObj = safeJsonParse(result.headers) as Record<string, string> | null;
  const headers = new Headers(headersObj || {});
  headers.set("cache-control", `public, max-age=${CACHE_TTL_SECONDS}, stale-while-revalidate=${CACHE_TTL_SECONDS}`);

  return new Response(result.body, {
    status: result.status,
    headers,
  });
}

async function getEdgeCache(cacheKeyUrl: string): Promise<Response | null> {
  const cacheKey = new Request(cacheKeyUrl, { method: "GET" });
  const cachedResponse = await caches.default.match(cacheKey);
  if (!cachedResponse) {
    return null;
  }
  if (cachedResponse.status !== 200) {
    await caches.default.delete(cacheKey);
    return null;
  }
  return cachedResponse;
}

async function putEdgeCacheFromText(
  cacheKeyUrl: string,
  body: string,
  status: number,
  headers: Record<string, string>
): Promise<void> {
  if (status !== 200) {
    return;
  }
  const cacheKey = new Request(cacheKeyUrl, { method: "GET" });
  const cacheResponse = buildEdgeCacheResponse(body, status, headers);
  await caches.default.put(cacheKey, cacheResponse);
}

function addCacheDebugHeaders(response: Response, layer: "edge" | "d1" | "miss", timings: Record<string, number>) {
  response.headers.set("x-cache-layer", layer);
  const parts: string[] = [];
  if (Number.isFinite(timings.edge)) {
    parts.push(`edge;dur=${timings.edge.toFixed(1)}`);
  }
  if (Number.isFinite(timings.d1)) {
    parts.push(`d1;dur=${timings.d1.toFixed(1)}`);
  }
  if (Number.isFinite(timings.total)) {
    parts.push(`total;dur=${timings.total.toFixed(1)}`);
  }
  if (parts.length) {
    response.headers.set("server-timing", parts.join(", "));
  }
}

async function putSharedCacheFromText(
  env: Env,
  cacheKeyUrl: string,
  body: string,
  status: number,
  headers: Record<string, string>
): Promise<void> {
  if (status !== 200) {
    return;
  }
  const now = Date.now();
  const expiresAt = now + CACHE_TTL_SECONDS * 1000;
  await env.CACHE_DB.prepare(
    `INSERT INTO cache_entries (cache_key, status, headers, body, created_at, updated_at, expires_at)
     VALUES (?, ?, ?, ?, ?, ?, ?)
     ON CONFLICT(cache_key) DO UPDATE SET
       status = excluded.status,
       headers = excluded.headers,
       body = excluded.body,
       updated_at = excluded.updated_at,
       expires_at = excluded.expires_at`
  ).bind(
    cacheKeyUrl,
    status,
    JSON.stringify(headers),
    body,
    now,
    now,
    expiresAt
  ).run();
}

async function deleteSharedCache(env: Env, cacheKeyUrl: string): Promise<void> {
  await env.CACHE_DB.prepare("DELETE FROM cache_entries WHERE cache_key = ?")
    .bind(cacheKeyUrl)
    .run();
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
      try {
        const value = await env.CACHE_META.get(key.name, "json") as CacheMeta | null;
        if (value && value.cacheKeyUrl) {
          items.push(value);
        }
      } catch (error) {
        console.error("Invalid cache meta JSON", { key: key.name, error });
      }
    }
  }

  items.sort((a, b) => b.updatedAt.localeCompare(a.updatedAt));
  return items;
}

function buildSearchCursor(prefixIndex: number, cursor: string | null): string | null {
  const safeCursor = cursor || "";
  return `${prefixIndex}:${safeCursor}`;
}

function parseSearchCursor(cursor: string | null): { prefixIndex: number; cursor: string | null } {
  if (!cursor) {
    return { prefixIndex: 0, cursor: null };
  }
  const separatorIndex = cursor.indexOf(":");
  if (separatorIndex === -1) {
    return { prefixIndex: 0, cursor: cursor || null };
  }
  const prefixPart = cursor.slice(0, separatorIndex);
  const rest = cursor.slice(separatorIndex + 1);
  const parsedPrefix = parseInt(prefixPart, 10);
  return {
    prefixIndex: Number.isFinite(parsedPrefix) ? parsedPrefix : 0,
    cursor: rest || null,
  };
}

async function searchCacheMetaPaginated(
  env: Env,
  type: CacheType | null,
  query: string,
  limit: number,
  cursor: string | null,
  maxPages: number
): Promise<{ items: CacheMeta[]; nextCursor: string | null }> {
  const prefixes = type ? [`${type}:`] : ["plp:", "pdp:"];
  const normalizedQuery = query.trim().toLowerCase();
  const parsedCursor = parseSearchCursor(cursor);
  let prefixIndex = Math.max(0, Math.min(parsedCursor.prefixIndex, prefixes.length - 1));
  let kvCursor = parsedCursor.cursor || undefined;
  const items: CacheMeta[] = [];
  let pages = 0;
  let nextCursor: string | null = null;
  const listLimit = Math.max(50, Math.min(200, limit * 2));
  const maxConcurrency = 10;

  while (prefixIndex < prefixes.length && items.length < limit && pages < maxPages) {
    const listResult = await env.CACHE_META.list({
      prefix: prefixes[prefixIndex],
      limit: listLimit,
      cursor: kvCursor,
    });
    pages += 1;

    for (let index = 0; index < listResult.keys.length; index += maxConcurrency) {
      const slice = listResult.keys.slice(index, index + maxConcurrency);
      const values = await Promise.all(slice.map(async (key) => {
        try {
          return await env.CACHE_META.get(key.name, "json") as CacheMeta | null;
        } catch (error) {
          console.error("Invalid cache meta JSON", { key: key.name, error });
          return null;
        }
      }));
      for (const value of values) {
        if (value && value.cacheKeyUrl && value.cacheKeyUrl.toLowerCase().includes(normalizedQuery)) {
          items.push(value);
          if (items.length >= limit) {
            break;
          }
        }
      }
      if (items.length >= limit) {
        break;
      }
    }

    if (items.length >= limit) {
      if (listResult.list_complete) {
        const nextPrefixIndex = prefixIndex + 1;
        nextCursor = nextPrefixIndex < prefixes.length ? buildSearchCursor(nextPrefixIndex, null) : null;
      } else {
        nextCursor = buildSearchCursor(prefixIndex, listResult.cursor);
      }
      break;
    }

    if (listResult.list_complete) {
      prefixIndex += 1;
      kvCursor = undefined;
    } else {
      kvCursor = listResult.cursor;
    }
  }

  if (!nextCursor && prefixIndex < prefixes.length) {
    nextCursor = buildSearchCursor(prefixIndex, kvCursor || null);
  }

  return { items, nextCursor };
}

async function countCacheKeysForPrefix(env: Env, prefix: string): Promise<number> {
  let total = 0;
  let cursor: string | undefined = undefined;
  do {
    const listResult = await env.CACHE_META.list({ prefix, limit: 1000, cursor });
    total += listResult.keys.length;
    cursor = listResult.list_complete ? undefined : listResult.cursor;
  } while (cursor);
  return total;
}

async function listCacheMetaPaginated(
  env: Env,
  type: CacheType | null,
  limit: number,
  cursor: string | null
): Promise<{ items: CacheMeta[]; nextCursor: string | null }> {
  const prefix = type ? `${type}:` : undefined;
  const listResult = await env.CACHE_META.list({
    prefix,
    limit,
    cursor: cursor || undefined,
  });
  const items: CacheMeta[] = [];

  for (const key of listResult.keys) {
    try {
      const value = await env.CACHE_META.get(key.name, "json") as CacheMeta | null;
      if (value && value.cacheKeyUrl) {
        items.push(value);
      }
    } catch (error) {
      console.error("Invalid cache meta JSON", { key: key.name, error });
    }
  }

  return {
    items,
    nextCursor: listResult.list_complete ? null : listResult.cursor,
  };
}

async function clearCacheByType(
  env: Env,
  ctx: ExecutionContext,
  type: CacheType | null
): Promise<number> {
  const items = await listCacheMeta(env, type);
  let cleared = 0;
  for (const item of items) {
    await deleteCacheEntry(env, item.cacheKeyUrl);
    const metaKey = `${item.type}:${item.id}`;
    await env.CACHE_META.delete(metaKey);
    cleared += 1;
  }
  return cleared;
}

async function deleteCacheEntry(env: Env, cacheKeyUrl: string): Promise<void> {
  await deleteSharedCache(env, cacheKeyUrl);
  const cacheKey = new Request(cacheKeyUrl, { method: "GET" });
  await caches.default.delete(cacheKey);
}

async function deleteCacheById(env: Env, id: string): Promise<boolean> {
  const meta = await getCacheMeta(env, id);
  if (!meta) {
    return false;
  }
  await deleteCacheEntry(env, meta.cacheKeyUrl);
  const metaKey = `${meta.type}:${meta.id}`;
  await env.CACHE_META.delete(metaKey);
  return true;
}

function resolveCacheKeyFromUrl(
  inputUrl: string,
  originForKey: string
): { type: CacheType; cacheKeyUrl: string } | null {
  try {
    const parsed = new URL(inputUrl);
    const pathname = parsed.pathname;
    const slugMatch = pathname.match(/^\/products\/([^\/]+)\/?$/);
    if (slugMatch) {
      const slug = decodeURIComponent(slugMatch[1]);
      const region = (parsed.searchParams.get("region") || "sa").toLowerCase();
      const language = (parsed.searchParams.get("language") || "en").toLowerCase();
      const cacheKeyUrl = `${originForKey}/products/${encodeURIComponent(slug)}?region=${region}&language=${language}`;
      return { type: "pdp", cacheKeyUrl };
    }
    if (pathname === "/products" || pathname === "/products/") {
      const backendBody = buildPlpBodyFromUrl(parsed);
      const cacheKeyUrl = buildPlpCacheKeyUrl(originForKey, backendBody);
      return { type: "plp", cacheKeyUrl };
    }
    return null;
  } catch {
    return null;
  }
}

async function getCacheMeta(env: Env, id: string): Promise<CacheMeta | null> {
  const plp = await env.CACHE_META.get(`plp:${id}`, "json") as CacheMeta | null;
  if (plp) {
    return plp;
  }
  const pdp = await env.CACHE_META.get(`pdp:${id}`, "json") as CacheMeta | null;
  return pdp || null;
}

async function getCachedResponse(env: Env, cacheKeyUrl: string): Promise<unknown> {
  const edgeCached = await getEdgeCache(cacheKeyUrl);
  if (edgeCached) {
    const edgeText = await edgeCached.clone().text();
    return {
      hit: true,
      status: edgeCached.status,
      headers: Object.fromEntries(edgeCached.headers),
      body: safeJsonParse(edgeText),
      raw: edgeText,
      source: "edge",
    };
  }
  const cachedResponse = await getSharedCache(env, cacheKeyUrl);
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
    source: "d1",
  };
}

async function getFreshResponseForMeta(env: Env, meta: CacheMeta): Promise<unknown> {
  const requestUrl = new URL(meta.cacheKeyUrl);
  const fakeRequest = new Request(requestUrl.toString(), { method: "GET" });
  const start = Date.now();

  if (meta.type === "pdp") {
    const slugMatch = requestUrl.pathname.match(/^\/products\/([^\/]+)\/?$/);
    if (!slugMatch) {
      return { error: "Invalid PDP cache key" };
    }
    const slug = slugMatch[1];
    const region = (requestUrl.searchParams.get("region") || "sa").toLowerCase();
    const language = (requestUrl.searchParams.get("language") || "en").toLowerCase();
    const backendResponse = await fetchPdpBackend(env, fakeRequest, slug, region, language, { noCache: true });
    const described = await describeResponse(backendResponse) as Record<string, unknown>;
    described.durationMs = Date.now() - start;
    return described;
  }

  const backendBody = buildPlpBodyFromUrl(requestUrl);
  const backendHeaders = {
    "content-type": "application/json",
    "accept": "application/json",
  };
  const backendResponse = await fetchPlpBackend(env, fakeRequest, backendBody, backendHeaders, { noCache: true });
  const described = await describeResponse(backendResponse) as Record<string, unknown>;
  described.durationMs = Date.now() - start;
  return described;
}

async function syncCacheForMeta(env: Env, ctx: ExecutionContext, meta: CacheMeta): Promise<unknown> {
  const fresh = await getFreshResponseForMeta(env, meta);
  if ("status" in (fresh as Record<string, unknown>) && (fresh as any).raw) {
    const responseInfo = fresh as { status: number; headers: Record<string, string>; raw: string };
    const itemCount = meta.type === "plp" ? extractPlpItemCount(responseInfo.raw) : null;
    const cacheResponse = buildEdgeCacheResponse(responseInfo.raw, responseInfo.status, responseInfo.headers);
    await putSharedCacheFromText(env, meta.cacheKeyUrl, responseInfo.raw, cacheResponse.status, Object.fromEntries(cacheResponse.headers));
    await putEdgeCacheFromText(meta.cacheKeyUrl, responseInfo.raw, cacheResponse.status, Object.fromEntries(cacheResponse.headers));
    await recordCacheMeta(env, meta.type, meta.cacheKeyUrl, {
      synced: true,
      source: meta.source,
      requestBodyHash: meta.requestBodyHash,
      itemCount,
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

function getCacheSyncKey(id: string): string {
  return `cache-sync:${id}`;
}

function getCacheSyncLatestKey(): string {
  return "cache-sync:latest";
}

async function createCacheSyncJob(
  env: Env,
  type: CacheType | "all",
  batchLimit: number,
  concurrency: number
): Promise<CacheSyncJob> {
  const id = crypto.randomUUID();
  const prefixes = type === "all" ? ["plp:", "pdp:"] : [`${type}:`];
  let total = 0;
  for (const prefix of prefixes) {
    total += await countCacheKeysForPrefix(env, prefix);
  }
  const job: CacheSyncJob = {
    id,
    status: "running",
    startedAt: formatUaeNow(),
    finishedAt: null,
    type,
    prefixes,
    prefixIndex: 0,
    cursor: null,
    total,
    processed: 0,
    synced: 0,
    failed: 0,
    batchLimit,
    concurrency,
    lastBatchMs: null,
    totalBatchMs: 0,
  };
  await kvPutWithRetry(env, getCacheSyncKey(id), JSON.stringify(job));
  await kvPutWithRetry(env, getCacheSyncLatestKey(), id);
  return job;
}

async function getCacheSyncJob(env: Env, id: string): Promise<CacheSyncJob | null> {
  return await env.CACHE_META.get(getCacheSyncKey(id), "json") as CacheSyncJob | null;
}

async function getLatestCacheSyncJob(env: Env): Promise<CacheSyncJob | null> {
  const id = await env.CACHE_META.get(getCacheSyncLatestKey());
  if (!id) {
    return null;
  }
  return await getCacheSyncJob(env, id);
}

async function updateCacheSyncJob(env: Env, job: CacheSyncJob): Promise<void> {
  await kvPutWithRetry(env, getCacheSyncKey(job.id), JSON.stringify(job));
}

async function runCacheSyncBatch(
  env: Env,
  ctx: ExecutionContext,
  id: string
): Promise<CacheSyncJob | null> {
  const job = await getCacheSyncJob(env, id);
  if (!job) {
    return null;
  }
  if (job.status !== "running") {
    return job;
  }

  if (job.prefixIndex >= job.prefixes.length) {
    job.status = "completed";
    job.finishedAt = formatUaeNow();
    await updateCacheSyncJob(env, job);
    return job;
  }

  const batchStart = Date.now();
  const maxBatchMs = 8000;
  const concurrency = Math.max(1, Math.min(20, job.concurrency || 1));

  while (job.prefixIndex < job.prefixes.length && Date.now() - batchStart < maxBatchMs) {
    const prefix = job.prefixes[job.prefixIndex];
    const listResult = await env.CACHE_META.list({
      prefix,
      limit: job.batchLimit,
      cursor: job.cursor || undefined,
    });

    let index = 0;
    const runWorker = async () => {
      while (index < listResult.keys.length) {
        const current = listResult.keys[index];
        index += 1;
        try {
          const value = await env.CACHE_META.get(current.name, "json") as CacheMeta | null;
          if (value && value.cacheKeyUrl) {
            await syncCacheForMeta(env, ctx, value);
            job.synced += 1;
          } else {
            job.failed += 1;
          }
        } catch {
          job.failed += 1;
        } finally {
          job.processed += 1;
        }
      }
    };
    await Promise.all(Array.from({ length: concurrency }, runWorker));

    if (listResult.list_complete) {
      job.prefixIndex += 1;
      job.cursor = null;
    } else {
      job.cursor = listResult.cursor;
    }
  }

  if (job.prefixIndex >= job.prefixes.length && job.cursor === null) {
    job.status = "completed";
    job.finishedAt = formatUaeNow();
  }

  const batchMs = Date.now() - batchStart;
  job.lastBatchMs = batchMs;
  job.totalBatchMs = (job.totalBatchMs || 0) + batchMs;

  await updateCacheSyncJob(env, job);
  return job;
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
    url: response.url,
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
      const cachedFirstPage = await getEdgeCache(baseCacheKeyUrl) || await getSharedCache(env, baseCacheKeyUrl);
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
      const itemCount = getPlpItemCount(data);
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
      const cacheResponse = buildEdgeCacheResponse(firstBodyText, firstResponse.status, Object.fromEntries(firstResponse.headers));
      await putSharedCacheFromText(env, cacheKeyUrl, firstBodyText, cacheResponse.status, Object.fromEntries(cacheResponse.headers));
      await putEdgeCacheFromText(cacheKeyUrl, firstBodyText, cacheResponse.status, Object.fromEntries(cacheResponse.headers));
      await recordCacheMeta(env, "plp", cacheKeyUrl, { synced: false, source: "post", itemCount });

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
        const pageItemCount = extractPlpItemCount(pageBodyText);
        const pageCacheKeyUrl = buildPlpCacheKeyUrl(edgeOrigin, body);
        const pageCacheResponse = buildEdgeCacheResponse(pageBodyText, pageResponse.status, Object.fromEntries(pageResponse.headers));
        await putSharedCacheFromText(env, pageCacheKeyUrl, pageBodyText, pageCacheResponse.status, Object.fromEntries(pageCacheResponse.headers));
        await putEdgeCacheFromText(pageCacheKeyUrl, pageBodyText, pageCacheResponse.status, Object.fromEntries(pageCacheResponse.headers));
        await recordCacheMeta(env, "plp", pageCacheKeyUrl, { synced: false, source: "post", itemCount: pageItemCount });
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

function getPlpItemCount(data: unknown): number | null {
  if (!data) {
    return null;
  }
  if (Array.isArray(data)) {
    return data.length;
  }
  if (typeof data !== "object") {
    return null;
  }
  const record = data as Record<string, unknown>;
  const listCandidates = [record.products, record.items, record.data];
  for (const list of listCandidates) {
    if (Array.isArray(list)) {
      return list.length;
    }
  }
  const nested = record.data;
  if (nested && typeof nested === "object" && !Array.isArray(nested)) {
    const nestedObj = nested as Record<string, unknown>;
    const nestedLists = [nestedObj.products, nestedObj.items, nestedObj.data];
    for (const list of nestedLists) {
      if (Array.isArray(list)) {
        return list.length;
      }
    }
  }
  return null;
}

function extractPlpItemCount(rawBody: string): number | null {
  const parsed = safeJsonParse(rawBody) as Record<string, unknown> | null;
  return getPlpItemCount(parsed);
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
  responseHeaders.set("cache-control", `public, max-age=${CACHE_TTL_SECONDS}, stale-while-revalidate=${CACHE_TTL_SECONDS}`);

  return new Response(rawBody, {
    status,
    headers: responseHeaders,
  });
}

function logCollection404(
  stage: string,
  request: Request,
  url: URL,
  extra: Record<string, unknown> = {}
) {
  const headers = Object.fromEntries(request.headers);
  const searchParams = Object.fromEntries(url.searchParams);
  console.log("collection-404 trace", {
    stage,
    method: request.method,
    url: url.toString(),
    pathname: url.pathname,
    searchParams,
    headers,
    ...extra,
  });
}

function logCollection404Response(
  stage: string,
  response: Response,
  bodyText: string,
  extra: Record<string, unknown> = {}
) {
  console.log("collection-404 response", {
    stage,
    status: response.status,
    headers: Object.fromEntries(response.headers),
    body: bodyText,
    ...extra,
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
      .spinner {
        display: inline-block;
        width: 12px;
        height: 12px;
        border: 2px solid #cbd5f5;
        border-top-color: #111827;
        border-radius: 50%;
        animation: spin 0.8s linear infinite;
        margin-left: 6px;
        vertical-align: middle;
      }
      @keyframes spin {
        to { transform: rotate(360deg); }
      }
      .compare-grid {
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 12px;
        margin-top: 12px;
      }
      .diff-block {
        background: #f8fafc;
        padding: 12px;
        border-radius: 8px;
        border: 1px solid var(--border);
        max-height: 320px;
        overflow: auto;
        font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
        font-size: 12px;
        line-height: 1.4;
      }
      .diff-line { white-space: pre; }
      .diff-mismatch { background: #fee2e2; }
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
        .compare-grid { grid-template-columns: 1fr; }
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
        const [showPrewarm, setShowPrewarm] = React.useState(false);
        const [categories, setCategories] = React.useState([]);
        const [modalOpen, setModalOpen] = React.useState(false);
        const [autoContinue, setAutoContinue] = React.useState(true);
        const [activeJobId, setActiveJobId] = React.useState('');
        const [filterRegion, setFilterRegion] = React.useState('');
        const [filterLanguage, setFilterLanguage] = React.useState('');
        const [filterCategory, setFilterCategory] = React.useState('');
        const [filterCount, setFilterCount] = React.useState('');
        const [searchTerm, setSearchTerm] = React.useState('');
        const [searchUrl, setSearchUrl] = React.useState('');
        const [searchCursor, setSearchCursor] = React.useState('');
        const [searchMode, setSearchMode] = React.useState(false);
        const [cacheType, setCacheType] = React.useState('all');
        const [pageLimit, setPageLimit] = React.useState('100');
        const [nextCursor, setNextCursor] = React.useState('');
        const [cursorHistory, setCursorHistory] = React.useState(['']);
        const [pageIndex, setPageIndex] = React.useState(1);
        const [cacheStats, setCacheStats] = React.useState({ total: 0, plp: 0, pdp: 0 });
        const [cacheSyncJob, setCacheSyncJob] = React.useState(null);
        const [cacheSyncAuto, setCacheSyncAuto] = React.useState(true);
        const [cacheSyncLimit, setCacheSyncLimit] = React.useState('50');
        const [cacheSyncConcurrency, setCacheSyncConcurrency] = React.useState('10');
        const [cacheSyncTotalMs, setCacheSyncTotalMs] = React.useState(0);
        const [syncing, setSyncing] = React.useState(false);
        const [searching, setSearching] = React.useState(false);
        const [compareMode, setCompareMode] = React.useState(false);
        const [searchSyncing, setSearchSyncing] = React.useState(false);
        const [searchSyncStatus, setSearchSyncStatus] = React.useState('');
        const [searchSyncTotals, setSearchSyncTotals] = React.useState({ synced: 0, failed: 0, pages: 0 });

        const loadCache = async (type, cursor, options = { reset: false }) => {
          setLoading(true);
          setError('');
          setSearchMode(false);
          setSearchCursor('');
          try {
            const params = new URLSearchParams();
            const parsedLimit = parseInt(pageLimit, 10);
            const safeLimit = Number.isFinite(parsedLimit) ? String(parsedLimit) : '100';
            params.set('type', type);
            params.set('limit', safeLimit);
            if (cursor) params.set('cursor', cursor);
            const res = await fetch('/admin/cache?' + params.toString(), { credentials: 'same-origin' });
            const data = await res.json();
            setItems(data.items || []);
            setSelected(null);
            setNextCursor(data.nextCursor || '');
            if (options.reset) {
              setCursorHistory([cursor || '']);
              setPageIndex(1);
            }
          } catch (err) {
            setError('Failed to load cache list.');
          } finally {
            setLoading(false);
          }
        };
        const load = async (type) => {
          setCacheType(type);
          await loadCache(type, '', { reset: true });
        };
        const loadFirstPage = async () => {
          await loadCache(cacheType, '', { reset: true });
        };
        const loadNextPage = async () => {
          if (!nextCursor) return;
          const cursor = nextCursor;
          setCursorHistory((prev) => [...prev, cursor]);
          setPageIndex((prev) => prev + 1);
          await loadCache(cacheType, cursor);
        };
        const loadPrevPage = async () => {
          if (cursorHistory.length <= 1) return;
          const history = cursorHistory.slice(0, -1);
          const cursor = history[history.length - 1];
          setCursorHistory(history);
          setPageIndex((prev) => Math.max(1, prev - 1));
          await loadCache(cacheType, cursor);
        };
        const searchBackend = async (query, cursor = '') => {
          const trimmed = String(query || '').trim();
          if (!trimmed) return;
          setSearching(true);
          setLoading(true);
          setError('');
          try {
            const params = new URLSearchParams();
            params.set('type', cacheType);
            const parsedLimit = parseInt(pageLimit, 10);
            const safeLimit = Number.isFinite(parsedLimit) ? String(Math.min(parsedLimit, 200)) : '100';
            params.set('limit', safeLimit);
            params.set('query', trimmed);
            if (cursor) params.set('cursor', cursor);
            const res = await fetch('/admin/cache/search?' + params.toString(), { credentials: 'same-origin' });
            const data = await res.json();
            if (!res.ok) {
              setError(data && data.error ? data.error : 'Search failed.');
              return;
            }
            if (cursor) {
              setItems((prev) => prev.concat(data.items || []));
            } else {
              setItems(data.items || []);
            }
            setSelected(null);
            setNextCursor('');
            setCursorHistory(['']);
            setPageIndex(1);
            setSearchMode(true);
            setSearchCursor(data.nextCursor || '');
          } catch {
            setError('Search failed.');
          } finally {
            setLoading(false);
            setSearching(false);
          }
        };
        const startSearch = async () => {
          setSearchCursor('');
          await searchBackend(searchTerm, '');
        };
        const loadMoreSearch = async () => {
          if (!searchCursor) return;
          await searchBackend(searchTerm, searchCursor);
        };
        const syncSearchAll = async () => {
          const query = String(searchTerm || '').trim();
          if (!query) {
            setError('Enter a search term first.');
            return;
          }
          const maxPages = 50;
          setSearchSyncing(true);
          setSearchSyncStatus('Starting...');
          setSearchSyncTotals({ synced: 0, failed: 0, pages: 0 });
          setError('');
          let cursor = '';
          let totalSynced = 0;
          let totalFailed = 0;
          setSearchSyncStatus('');
          let pages = 0;
          try {
            while (pages < maxPages) {
              const params = new URLSearchParams();
              params.set('type', cacheType);
              params.set('limit', '200');
              params.set('query', query);
              params.set('sync', '1');
              params.set('scan', '2');
              if (cursor) params.set('cursor', cursor);
              const res = await fetch('/admin/cache/search?' + params.toString(), { credentials: 'same-origin' });
              const data = await res.json();
              if (!res.ok) {
                setError(data && data.error ? data.error : 'Sync search failed.');
                break;
              }
              totalSynced += data.synced || 0;
              totalFailed += data.failed || 0;
              pages += 1;
              setSearchSyncTotals({ synced: totalSynced, failed: totalFailed, pages });
              setSearchSyncStatus('Synced ' + totalSynced + ' | Failed ' + totalFailed + ' | Pages ' + pages + '/' + maxPages);
              if (!data.nextCursor) {
                break;
              }
              cursor = data.nextCursor;
            }
          } catch {
            setError('Sync search failed.');
          } finally {
            setSearchSyncing(false);
            await loadCacheStats();
            await loadFirstPage();
          }
        };

        const applySearchUrl = async () => {
          const raw = searchUrl.trim();
          if (!raw) return;
          try {
            const parsed = new URL(raw);
            const slugMatch = parsed.pathname.match(/\\\/products\\\/([^\\/]+)\\\/?$/);
            if (slugMatch) {
              const slug = decodeURIComponent(slugMatch[1] || '');
              setSearchTerm(slug);
              setSearchCursor('');
              await searchBackend(slug, '');
              return;
            }
            const category = parsed.searchParams.get('category') || '';
            const collection = parsed.searchParams.get('collection') || '';
            const region = parsed.searchParams.get('region') || '';
            if (region) {
              setFilterRegion(region);
            }
            if (category) {
              setFilterCategory(decodeURIComponent(category));
            }
            const searchValue = collection || category;
            if (searchValue) {
              const decoded = decodeURIComponent(searchValue);
              setSearchTerm(decoded);
              setSearchCursor('');
              await searchBackend(decoded, '');
            }
          } catch {
            setSearchTerm(raw);
            setSearchCursor('');
            await searchBackend(raw, '');
          }
        };
        const syncAllWithType = async (type) => {
          setLoading(true);
          setSyncing(true);
          setError('');
          try {
            const params = new URLSearchParams();
            params.set('type', type);
            const parsedLimit = parseInt(cacheSyncLimit, 10);
            const safeLimit = Number.isFinite(parsedLimit) ? String(parsedLimit) : '10';
            params.set('limit', safeLimit);
            const parsedConcurrency = parseInt(cacheSyncConcurrency, 10);
            const safeConcurrency = Number.isFinite(parsedConcurrency) ? String(Math.min(parsedConcurrency, 20)) : '5';
            params.set('concurrency', safeConcurrency);
            const res = await fetch('/admin/cache/sync-all?' + params.toString(), { method: 'POST', credentials: 'same-origin' });
            const data = await res.json();
            if (data && data.jobId) {
              setCacheSyncJob({ id: data.jobId });
              setCacheSyncAuto(true);
            } else if (!res.ok) {
              setError('Failed to start sync job.');
            }
          } catch {
            setError('Failed to start sync job.');
          } finally {
            setLoading(false);
            setSyncing(false);
          }
        };
        const syncAll = async () => {
          await syncAllWithType(cacheType);
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

        const loadCacheStats = async () => {
          try {
            const res = await fetch('/admin/cache/stats?type=all', { credentials: 'same-origin' });
            const data = await res.json();
            setCacheStats({
              total: data.total || 0,
              plp: data.plp || 0,
              pdp: data.pdp || 0
            });
          } catch {
            setCacheStats({ total: 0, plp: 0, pdp: 0 });
          }
        };

        const loadCacheSyncStatus = async (jobId) => {
          if (!jobId) return null;
          const res = await fetch('/admin/cache/sync/status?id=' + jobId, { credentials: 'same-origin' });
          if (!res.ok) return null;
          return await res.json();
        };

        const loadLatestCacheSync = async () => {
          const res = await fetch('/admin/cache/sync/latest', { credentials: 'same-origin' });
          if (!res.ok) return null;
          return await res.json();
        };

        const continueCacheSync = async (jobId) => {
          if (!jobId) return null;
          const res = await fetch('/admin/cache/sync/continue?id=' + jobId, { method: 'POST', credentials: 'same-origin' });
          if (!res.ok) return null;
          return await res.json();
        };

        const stopCacheSync = async (jobId) => {
          if (!jobId) return null;
          const res = await fetch('/admin/cache/sync/stop?id=' + jobId, { method: 'POST', credentials: 'same-origin' });
          if (!res.ok) return null;
          return await res.json();
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
          await loadFirstPage();
        };

        const view = async (id) => {
          const res = await fetch('/admin/cache/item?id=' + id, { credentials: 'same-origin' });
          const data = await res.json();
          setSelected(data);
          setModalOpen(true);
          setCompareMode(false);
        };

        const compare = async (id) => {
          const res = await fetch('/admin/cache/item?id=' + id, { credentials: 'same-origin' });
          const data = await res.json();
          setSelected(data);
          setModalOpen(true);
          setCompareMode(true);
        };

        const lookupUrl = async () => {
          const raw = searchUrl.trim();
          if (!raw) return;
          setLoading(true);
          setError('');
          try {
            const res = await fetch('/admin/cache/lookup?url=' + encodeURIComponent(raw), { credentials: 'same-origin' });
            const data = await res.json();
            if (!res.ok) {
              setError(data && data.error ? data.error : 'Lookup failed.');
              return;
            }
            setSelected(data);
            setModalOpen(true);
            setCompareMode(true);
          } catch {
            setError('Lookup failed.');
          } finally {
            setLoading(false);
          }
        };

        const sync = async (id) => {
          const res = await fetch('/admin/cache/sync?id=' + id, { method: 'POST', credentials: 'same-origin' });
          const data = await res.json();
          setSelected((prev) => ({ ...prev, fresh: data }));
          await loadFirstPage();
        };

        const remove = async (id) => {
          if (!id) return;
          if (!confirm('Delete this cache entry?')) return;
          setLoading(true);
          setError('');
          try {
            const res = await fetch('/admin/cache/delete?id=' + id, { method: 'POST', credentials: 'same-origin' });
            const data = await res.json();
            if (!res.ok) {
              setError(data && data.error ? data.error : 'Failed to delete cache entry.');
              return;
            }
            if (selected && selected.meta && selected.meta.id === id) {
              setSelected(null);
              setModalOpen(false);
            }
            await loadCacheStats();
            await loadFirstPage();
          } catch {
            setError('Failed to delete cache entry.');
          } finally {
            setLoading(false);
          }
        };

        React.useEffect(() => {
          loadFirstPage();
          loadPrewarm();
          loadCategories();
          loadCacheStats();
          (async () => {
            const latest = await loadLatestCacheSync();
            if (latest) {
              setCacheSyncJob(latest);
              if (latest.status === 'running') {
                setCacheSyncAuto(true);
              }
              if (typeof latest.totalBatchMs === 'number') {
                setCacheSyncTotalMs(latest.totalBatchMs);
              }
            }
          })();
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

        React.useEffect(() => {
          if (!cacheSyncAuto || !cacheSyncJob || !cacheSyncJob.id) return;
          const interval = setInterval(async () => {
            const progressed = await continueCacheSync(cacheSyncJob.id);
            if (progressed) {
              setCacheSyncJob(progressed);
              if (typeof progressed.totalBatchMs === 'number') {
                setCacheSyncTotalMs(progressed.totalBatchMs);
              }
            }
            const current = progressed || (await loadCacheSyncStatus(cacheSyncJob.id));
            if (!current || current.status !== 'running') {
              setCacheSyncAuto(false);
              await loadCacheStats();
              await loadFirstPage();
            }
          }, 5000);
          return () => clearInterval(interval);
        }, [cacheSyncAuto, cacheSyncJob && cacheSyncJob.id]);

        const ttlSeconds = 604800;
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
        const getItemCount = (item) => {
          return typeof item.itemCount === 'number' ? item.itemCount : null;
        };
        const tryFormatJson = (raw) => {
          if (typeof raw !== 'string') {
            return '';
          }
          const trimmed = raw.trim();
          if (!trimmed) return '';
          try {
            const parsed = JSON.parse(trimmed);
            return JSON.stringify(parsed, null, 2);
          } catch {
            return raw;
          }
        };
        const getResponseRaw = (payload) => {
          if (!payload) return '';
          if (typeof payload.raw === 'string') {
            return payload.raw;
          }
          return JSON.stringify(payload, null, 2);
        };
        const buildLineDiff = (leftText, rightText) => {
          const left = tryFormatJson(leftText);
          const right = tryFormatJson(rightText);
          const newline = String.fromCharCode(10);
          const leftLines = (left || '').split(newline);
          const rightLines = (right || '').split(newline);
          const max = Math.max(leftLines.length, rightLines.length);
          const leftOut = [];
          const rightOut = [];
          let mismatches = 0;
          for (let i = 0; i < max; i += 1) {
            const leftLine = leftLines[i] ?? '';
            const rightLine = rightLines[i] ?? '';
            const mismatch = leftLine !== rightLine;
            if (mismatch) mismatches += 1;
            leftOut.push({ text: leftLine, mismatch });
            rightOut.push({ text: rightLine, mismatch });
          }
          return { leftLines: leftOut, rightLines: rightOut, mismatches, left, right };
        };

        const filteredItems = items.filter((item) => {
          const region = getRegion(item);
          const language = getLanguage(item);
          const category = getCategoryName(item);
          const regionFilter = filterRegion.trim().toLowerCase();
          const languageFilter = filterLanguage.trim().toLowerCase();
          const regionOk = !regionFilter || region === regionFilter;
          const languageOk = !languageFilter || language === languageFilter;
          const categoryOk = !filterCategory || category === filterCategory;
          const countFilter = filterCount.trim();
          const parsedCount = countFilter ? parseInt(countFilter, 10) : NaN;
          const countValue = Number.isFinite(parsedCount) ? parsedCount : null;
          const itemCount = getItemCount(item);
          const countOk = countValue === null || (item.type === 'plp' && itemCount === countValue);
          const search = searchTerm.trim().toLowerCase();
          if (!search) {
            return regionOk && languageOk && categoryOk && countOk;
          }
          const collection = getCollection(item);
          const slug = getSlug(item);
          const match =
            (category || '').toLowerCase().includes(search) ||
            (collection || '').toLowerCase().includes(search) ||
            (slug || '').toLowerCase().includes(search);
          return regionOk && languageOk && categoryOk && countOk && match;
        });

        return e('div', null,
          e('h1', null, 'Edge Cache Admin'),
          e('div', { className: 'controls' },
            e('select', {
              value: cacheType,
              onChange: (e) => setCacheType(e.target.value),
              style: { padding: '8px', borderRadius: '8px', border: '1px solid #e5e7eb' }
            },
              e('option', { value: 'all' }, 'All types'),
              e('option', { value: 'pdp' }, 'PDP'),
              e('option', { value: 'plp' }, 'PLP')
            ),
            e('input', {
              value: pageLimit,
              onChange: (e) => setPageLimit(e.target.value),
              placeholder: 'page size',
              style: { padding: '8px', borderRadius: '8px', border: '1px solid #e5e7eb', width: '110px' }
            }),
            e('button', { onClick: () => loadFirstPage() }, 'Load'),
            e('input', {
              value: cacheSyncLimit,
              onChange: (e) => setCacheSyncLimit(e.target.value),
              placeholder: 'sync batch',
              style: { padding: '8px', borderRadius: '8px', border: '1px solid #e5e7eb', width: '110px' }
            }),
            e('input', {
              value: cacheSyncConcurrency,
              onChange: (e) => setCacheSyncConcurrency(e.target.value),
              placeholder: 'sync concurrency',
              style: { padding: '8px', borderRadius: '8px', border: '1px solid #e5e7eb', width: '130px' }
            }),
            e('button', { className: 'secondary', onClick: () => syncAll(), disabled: syncing },
              syncing ? e('span', null, 'Syncing', e('span', { className: 'spinner' })) : 'Sync All'
            ),
            e('button', { className: 'danger', onClick: () => clear('all') }, 'Clear All'),
            e('button', { className: 'secondary', onClick: () => setShowPrewarm(!showPrewarm) },
              showPrewarm ? 'Hide Prewarm' : 'Show Prewarm'
            ),
            loading ? e('span', { style: { marginLeft: '8px' } }, 'Loading...') : null,
            error ? e('span', { style: { marginLeft: '8px', color: '#b91c1c' } }, error) : null
          ),
          e('div', { className: 'meta', style: { marginBottom: '12px' } },
            'Total cache entries: ', cacheStats.total,
            ' | PDP: ', cacheStats.pdp,
            ' | PLP: ', cacheStats.plp
          ),
          cacheSyncJob ? e('div', { className: 'panel', style: { marginBottom: '16px' } },
            e('div', { className: 'item-title' }, 'Cache Sync Job'),
            e('div', { className: 'meta' },
              'Status: ', cacheSyncJob.status || '-',
              ' | Processed: ', cacheSyncJob.processed || 0, '/', cacheSyncJob.total || 0,
              ' | Synced: ', cacheSyncJob.synced || 0,
              ' | Failed: ', cacheSyncJob.failed || 0,
              ' | Left: ', Math.max(0, (cacheSyncJob.total || 0) - (cacheSyncJob.processed || 0)),
              ' | Last batch: ', cacheSyncJob.lastBatchMs ? cacheSyncJob.lastBatchMs + 'ms' : '-',
              ' | Total: ', cacheSyncTotalMs ? cacheSyncTotalMs + 'ms' : '-'
            ),
            cacheSyncJob.status === 'running'
              ? e('div', { className: 'controls' },
                e('button', {
                  className: 'danger',
                  onClick: async () => {
                    const stopped = await stopCacheSync(cacheSyncJob.id);
                    if (stopped) {
                      setCacheSyncJob(stopped);
                      setCacheSyncAuto(false);
                    }
                  }
                }, 'Stop Sync')
              )
              : null
          ) : null,
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
                value: filterLanguage,
                onChange: (e) => setFilterLanguage(e.target.value),
                placeholder: 'filter language',
                style: { padding: '8px', borderRadius: '8px', border: '1px solid #e5e7eb', width: '140px' }
              }),
              e('input', {
                value: searchTerm,
                onChange: (e) => setSearchTerm(e.target.value),
                placeholder: 'search slug/category/collection',
                style: { padding: '8px', borderRadius: '8px', border: '1px solid #e5e7eb', minWidth: '240px' }
              }),
              e('button', { className: 'secondary', onClick: () => startSearch(), disabled: searching },
                searching ? e('span', null, 'Searching', e('span', { className: 'spinner' })) : 'Search'
              ),
              e('button', { className: 'secondary', onClick: () => syncSearchAll(), disabled: searchSyncing },
                searchSyncing ? e('span', null, 'Syncing', e('span', { className: 'spinner' })) : 'Sync Search'
              ),
              e('input', {
                value: filterCount,
                onChange: (e) => setFilterCount(e.target.value),
                placeholder: 'filter count',
                style: { padding: '8px', borderRadius: '8px', border: '1px solid #e5e7eb', width: '120px' }
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
            e('div', { className: 'controls' },
              e('input', {
                value: searchUrl,
                onChange: (e) => setSearchUrl(e.target.value),
                placeholder: 'paste category or product URL',
                style: { padding: '8px', borderRadius: '8px', border: '1px solid #e5e7eb', minWidth: '360px' }
              }),
              e('button', { className: 'secondary', onClick: () => applySearchUrl() }, 'Apply URL'),
              e('button', { className: 'secondary', onClick: () => lookupUrl() }, 'Lookup URL')
            ),
            searchSyncStatus
              ? e('div', { className: 'panel', style: { marginBottom: '12px' } },
                e('div', { className: 'item-title' }, 'Search Sync'),
                e('div', { className: 'meta' },
                  'Status: ', searchSyncing ? 'running' : 'done',
                  ' | Synced: ', searchSyncTotals.synced,
                  ' | Failed: ', searchSyncTotals.failed,
                  ' | Pages: ', searchSyncTotals.pages, '/50'
                )
              )
              : null,
            e('div', { className: 'controls' },
              e('button', { className: 'secondary', onClick: () => loadPrevPage(), disabled: cursorHistory.length <= 1 || searchMode }, 'Prev'),
              e('button', { className: 'secondary', onClick: () => loadNextPage(), disabled: !nextCursor || searchMode }, 'Next'),
              e('button', { className: 'secondary', onClick: () => loadMoreSearch(), disabled: !searchMode || !searchCursor }, 'More Results'),
              e('span', { className: 'meta' }, 'Page ', pageIndex)
            ),
            e('div', { className: 'table-wrap' },
              e('table', null,
                e('thead', null,
                    e('tr', null,
                      e('th', null, 'Type'),
                      e('th', null, 'Category/Slug/Collection'),
                      e('th', null, 'Page'),
                      e('th', null, 'Count'),
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
                        e('td', null, item.type === 'plp' ? (getItemCount(item) ?? '-') : '-'),
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
                          e('button', { className: 'secondary', onClick: () => compare(item.id) }, 'Compare'),
                          ' ',
                          e('button', { className: 'secondary', onClick: () => sync(item.id) }, 'Sync'),
                          ' ',
                          e('button', { className: 'danger', onClick: () => remove(item.id) }, 'Delete')
                        )
                    )
                  )
                )
              )
            ),
            modalOpen && selected ? e('div', { className: 'modal-backdrop', onClick: () => setModalOpen(false) },
              e('div', { className: 'modal', onClick: (event) => event.stopPropagation() },
                e('div', { className: 'modal-header' },
                  e('h3', null, compareMode ? 'Compare Responses' : 'Cached Response'),
                  e('div', null,
                    e('button', { className: 'secondary', onClick: () => setModalOpen(false) }, 'Close'),
                    ' ',
                    e('button', { className: 'secondary', onClick: () => setCompareMode(!compareMode) },
                      compareMode ? 'View Cache' : 'Compare'
                    ),
                    ' ',
                    e('button', { className: 'danger', onClick: () => remove(selected.meta && selected.meta.id) }, 'Delete')
                  )
                ),
                e('div', { className: 'meta' },
                  'Cache ID: ', selected.meta && selected.meta.id ? selected.meta.id : '-',
                  ' | Cache Key: ', selected.meta && selected.meta.cacheKeyUrl ? selected.meta.cacheKeyUrl : '-'
                ),
                compareMode
                  ? (() => {
                      const cachedRaw = getResponseRaw(selected.cache);
                      const freshRaw = getResponseRaw(selected.fresh);
                      const diff = buildLineDiff(cachedRaw, freshRaw);
                      const statusMatch = (selected.cache && selected.fresh) ? selected.cache.status === selected.fresh.status : false;
                      const headerMatch = (selected.cache && selected.fresh)
                        ? JSON.stringify(selected.cache.headers || {}) === JSON.stringify(selected.fresh.headers || {})
                        : false;
                      const rawMatch = diff.mismatches === 0;
                      const matchLabel = statusMatch && headerMatch && rawMatch ? 'Match' : 'Mismatch';
                      const freshUrl = selected.fresh && selected.fresh.url ? selected.fresh.url : '-';
                      const duration = selected.fresh && typeof selected.fresh.durationMs === 'number'
                        ? selected.fresh.durationMs + 'ms'
                        : '-';
                      return e('div', null,
                        e('div', { className: 'meta' },
                          'Status: ', statusMatch ? 'match' : 'mismatch',
                          ' | Headers: ', headerMatch ? 'match' : 'mismatch',
                          ' | Body: ', rawMatch ? 'match' : 'mismatch',
                          ' | Result: ', matchLabel
                        ),
                        e('div', { className: 'meta' },
                          'Fresh URL: ', freshUrl,
                          ' | Duration: ', duration
                        ),
                        e('div', { className: 'compare-grid' },
                          e('div', null,
                            e('div', { className: 'meta' }, 'Cached'),
                            e('div', { className: 'diff-block' },
                              diff.leftLines.map((line, idx) =>
                                e('div', { key: 'l' + idx, className: 'diff-line' + (line.mismatch ? ' diff-mismatch' : '') }, line.text || ' ')
                              )
                            )
                          ),
                          e('div', null,
                            e('div', { className: 'meta' }, 'Fresh (api.auraliving.com)'),
                            e('div', { className: 'diff-block' },
                              diff.rightLines.map((line, idx) =>
                                e('div', { key: 'r' + idx, className: 'diff-line' + (line.mismatch ? ' diff-mismatch' : '') }, line.text || ' ')
                              )
                            )
                          )
                        )
                      );
                    })()
                  : e('pre', null, JSON.stringify(selected.cache, null, 2))
              )
            ) : null
          )
        );
      }

      const root = document.getElementById('root');
      if (root) {
        root.textContent = 'Loading...';
        try {
          ReactDOM.createRoot(root).render(React.createElement(App));
        } catch (error) {
          const message = error && error.message ? error.message : String(error);
          root.textContent = 'Admin failed to load: ' + message;
          console.error(error);
        }
      }
    </script>
  </body>
</html>`;
}
