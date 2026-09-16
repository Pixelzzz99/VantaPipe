'use strict';

const path = require('path');
const express = require('express');
const { loadAuthConfig, basicAuthMiddleware, verifyBasicAuth } = require('./auth');
const { createApiProxy, createWsProxy } = require('./proxy');

const PORT = parseInt(process.env.PORT || '3000', 10);
const ETL_ENGINE_URL = process.env.ETL_ENGINE_URL || 'http://localhost:4000';

let authConfig;
try {
  authConfig = loadAuthConfig(process.env);
} catch (e) {
  console.error(e.message);
  process.exit(1);
}
if (authConfig) {
  console.log(`Gateway auth enabled for user '${authConfig.username}'`);
} else {
  console.warn(
    'Gateway auth is disabled — set ETL_AUTH_USER and ETL_AUTH_PASS to protect the dashboard/API before exposing this port publicly.'
  );
}

const app = express();
const requireAuth = basicAuthMiddleware(authConfig);

app.use(requireAuth);
app.use(express.static(path.join(__dirname, '..', 'public'), { index: 'dashboard.html' }));
app.use('/api', createApiProxy(ETL_ENGINE_URL));

const wsProxy = createWsProxy(ETL_ENGINE_URL);
app.use('/ws', wsProxy);

const server = app.listen(PORT, () => {
  console.log(`Gateway listening on http://localhost:${PORT}`);
  console.log(`Proxying /api and /ws to engine at ${ETL_ENGINE_URL}`);
});

// WebSocket upgrades bypass Express's normal middleware chain (they fire
// their own 'upgrade' event on the raw HTTP server), so Basic Auth has to
// be checked here explicitly before handing off to the proxy.
server.on('upgrade', (req, socket, head) => {
  if (authConfig && !verifyBasicAuth(req.headers.authorization, authConfig)) {
    socket.write('HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm="etl-engine"\r\n\r\n');
    socket.destroy();
    return;
  }
  wsProxy.upgrade(req, socket, head);
});
