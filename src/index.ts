/**
 * Interface for Environment Variables
 * Ensures type safety when accessing variables defined in wrangler.toml
 */
interface Env {
  BACKEND_PRODUCT_URL: string;
  BACKEND_PRODUCT_LIST_URL: string;
  ALLOWED_ORIGIN: string;
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
        // This captures the slug and ensures we don't match deep paths like /products/123/extra
        const slugMatch = url.pathname.match(/^\/products\/([^\/]+)\/?$/);
        if (slugMatch) {
          const slug = slugMatch[1]; // The captured slug
          return await handleProductRequest(request, env, ctx, slug);
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

  // 1. Try Cache
  let cachedResponse = await cache.match(cacheKey);
  if (cachedResponse) {
    const res = new Response(cachedResponse.body, cachedResponse);
    res.headers.set("x-edge-cache", "HIT");
    addCorsHeaders(res, env, request);
    return res;
  }

  // 2. Prepare Backend Request
  const backendBody = { slug, region, language };
  const incomingHeaders = request.headers;

  const backendHeaders: Record<string, string> = {
    "content-type": "application/json",
    "accept": "application/json",
  };
  
  // Forward tracking headers if present
  const xFbc = incomingHeaders.get("x-fbc");
  const xFbp = incomingHeaders.get("x-fbp");
  if (xFbc) backendHeaders["x-fbc"] = xFbc;
  if (xFbp) backendHeaders["x-fbp"] = xFbp;

  try {
    const backendResponse = await fetch(env.BACKEND_PRODUCT_URL, {
      method: "POST",
      headers: backendHeaders,
      body: JSON.stringify(backendBody),
    });

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
        "cache-control": "public, max-age=60, stale-while-revalidate=120",
      },
    });

    edgeResponse.headers.set("x-edge-cache", "MISS");
    addCorsHeaders(edgeResponse, env, request);

    // 4. Update Cache (Non-blocking)
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

  // 1. Try Cache
  let cachedResponse = await cache.match(cacheKey);
  if (cachedResponse) {
    const res = new Response(cachedResponse.body, cachedResponse);
    res.headers.set("x-edge-cache", "HIT");
    addCorsHeaders(res, env, request);
    return res;
  }

  // 2. Parse Query Params safely
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
  } catch (e) {
    // Malformed JSON in filters shouldn't crash the worker, just default to empty
    console.warn("Invalid filters param received");
    filters = [];
  }

  const backendBody: any = {
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
        "cache-control": "public, max-age=60, stale-while-revalidate=120",
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

function addCorsHeaders(response: Response, env: Env, request: Request) {
  const origin = request.headers.get("origin");
  if (origin && origin === env.ALLOWED_ORIGIN) {
    response.headers.set("access-control-allow-origin", origin);
    response.headers.set("vary", "Origin");
  }
  response.headers.set("access-control-allow-credentials", "true");
}