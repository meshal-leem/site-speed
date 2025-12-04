/**
 * Interface for Environment Variables
 */
interface Env {
  BACKEND_PRODUCT_URL: string;
  BACKEND_PRODUCT_LIST_URL: string;
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
      if (request.method === "GET") {
        
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
    "access-control-allow-methods": "GET, OPTIONS",
    "access-control-allow-headers": "Content-Type, X-FBC, X-FBP",
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

  const cacheKeyUrl = `${url.origin}/products/${slug}?region=${region}&language=${language}`;
  const cacheKey = new Request(cacheKeyUrl, { method: "GET" });
  const cache = caches.default;

  // 1. Try Cache
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

  // 2. CACHE MISS
  return await fetchAndCacheProduct(request, env, ctx, cacheKey, slug, region, language);
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
  const cacheKey = new Request(url.toString(), { method: "GET" });

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
    if (filtersParam) filters = JSON.parse(filtersParam);
  } catch (e) {
    console.warn("Invalid filters param received");
    filters = [];
  }

  const backendBody: any = {
    page, count, category, region, language, order, filters, calculateTotalPrice,
  };
  if (collection) backendBody.collection = collection;

  // 1. Try Cache
  let cachedResponse = await cache.match(cacheKey);

  if (cachedResponse) {
    // CACHE HIT: Fire background update
    ctx.waitUntil(
        fetchAndCacheProductList(request, env, ctx, cacheKey, backendBody)
    );

    const res = new Response(cachedResponse.body, cachedResponse);
    res.headers.set("x-edge-cache", "HIT-STALE");
    addCorsHeaders(res, env, request);
    return res;
  }

  // 2. CACHE MISS
  return await fetchAndCacheProductList(request, env, ctx, cacheKey, backendBody);
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
  language: string
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
        "cache-control": "public, max-age=31536000", 
      },
    });

    edgeResponse.headers.set("x-edge-cache", "MISS");
    addCorsHeaders(edgeResponse, env, request);

    ctx.waitUntil(cache.put(cacheKey, edgeResponse.clone()));

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
    backendBody: any
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
      const backendResponse = await fetch(env.BACKEND_PRODUCT_LIST_URL, {
        method: "POST",
        headers: backendHeaders,
        body: JSON.stringify(backendBody),
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
          "cache-control": "public, max-age=31536000",
        },
      });
  
      edgeResponse.headers.set("x-edge-cache", "MISS");
      addCorsHeaders(edgeResponse, env, request);
  
      ctx.waitUntil(cache.put(cacheKey, edgeResponse.clone()));
  
      return edgeResponse;
  
    } catch (error) {
      console.error(`Fetch failed for PLP:`, error);
      return new Response(JSON.stringify({ error: "Service Unavailable" }), {
          status: 502,
          headers: { "Content-Type": "application/json" }
      });
    }
  }