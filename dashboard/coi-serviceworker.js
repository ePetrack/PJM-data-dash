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
  // Only intercept same-origin requests (leave CDN calls unmodified)
  if (event.request.url.startsWith(self.location.origin)) {
    event.respondWith(
      fetch(event.request).then((response) => {
        // If the response already has the required headers, pass it through.
        if (
          response.headers.get("Cross-Origin-Embedder-Policy") &&
          response.headers.get("Cross-Origin-Opener-Policy")
        ) {
          return response;
        }

        // Clone the headers and add the cross-origin isolation headers.
        const headers = new Headers(response.headers);
        headers.set("Cross-Origin-Embedder-Policy", "credentialless");
        headers.set("Cross-Origin-Opener-Policy",   "same-origin");
        headers.set("Cross-Origin-Resource-Policy", "cross-origin");

        return new Response(response.body, {
          status:     response.status,
          statusText: response.statusText,
          headers,
        });
      })
    );
  }
});
