/**
 * coi-serviceworker.js
 *
 * WHY THIS EXISTS
 * ───────────────
 * DuckDB-WASM uses SharedArrayBuffer internally, which browsers only allow
 * when the page is "cross-origin isolated". That requires two HTTP headers:
 *
 *   Cross-Origin-Embedder-Policy: require-corp   (or credentialless)
 *   Cross-Origin-Opener-Policy:   same-origin
 *
 * GitHub Pages does not let you set custom response headers, so we use a
 * Service Worker to inject them on every response the browser receives.
 *
 * SETUP
 * ─────
 * index.html registers this worker before loading DuckDB:
 *
 *   if ("serviceWorker" in navigator) {
 *     await navigator.serviceWorker.register("./coi-serviceworker.js");
 *     // If the SW was just installed, reload so the new headers take effect.
 *     if (!crossOriginIsolated) location.reload();
 *   }
 *
 * REFERENCE
 * ─────────
 * Adapted from https://github.com/nicolo-ribaudo/tc39-proposal-shadowrealm
 * and the pattern used in djouallah/analytics-as-code.
 */

self.addEventListener("install", () => self.skipWaiting());
self.addEventListener("activate", (e) => e.waitUntil(self.clients.claim()));

self.addEventListener("fetch", (event) => {
  event.respondWith(
    fetch(event.request).then((response) => {
      // Opaque (no-cors) responses can't have headers modified; they are
      // already allowed under COEP credentialless so return them as-is.
      if (response.type === "opaque") return response;

      // Already has the isolation headers we need — pass through.
      if (
        response.headers.get("Cross-Origin-Embedder-Policy") &&
        response.headers.get("Cross-Origin-Opener-Policy")
      ) {
        return response;
      }

      const isSameOrigin = event.request.url.startsWith(self.location.origin);
      const headers = new Headers(response.headers);

      if (isSameOrigin) {
        // Inject COEP + COOP on same-origin responses (the page itself).
        headers.set("Cross-Origin-Embedder-Policy", "credentialless");
        headers.set("Cross-Origin-Opener-Policy",   "same-origin");
      }

      // Add CORP to every response (same-origin and CDN alike) so that
      // cross-origin dynamic import() calls — including internal webpack
      // chunks inside duckdb-mvp.js — are embeddable under COEP.
      headers.set("Cross-Origin-Resource-Policy", "cross-origin");

      return new Response(response.body, {
        status:     response.status,
        statusText: response.statusText,
        headers,
      });
    })
  );
});
