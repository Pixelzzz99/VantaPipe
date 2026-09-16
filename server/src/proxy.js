'use strict';

const { createProxyMiddleware } = require('http-proxy-middleware');

// Everything under /api and the /ws/logs WebSocket are proxied straight
// through to the Rust engine, byte for byte — the gateway does not
// reinterpret any of this traffic, it only gates it behind auth.
function createApiProxy(target) {
  return createProxyMiddleware({
    target,
    changeOrigin: true,
    logger: console,
  });
}

function createWsProxy(target) {
  return createProxyMiddleware({
    target,
    changeOrigin: true,
    ws: true,
    logger: console,
  });
}

module.exports = { createApiProxy, createWsProxy };
