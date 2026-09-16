'use strict';

const crypto = require('crypto');

// Mirrors the config semantics of the old src/auth.rs: both env vars unset
// -> auth disabled; exactly one set -> fail fast at startup; both set ->
// require Basic Auth on every request.
function loadAuthConfig(env) {
  const username = env.ETL_AUTH_USER;
  const password = env.ETL_AUTH_PASS;

  if (!username && !password) {
    return null;
  }
  if (!username || !password) {
    throw new Error(
      'Invalid auth configuration: set both ETL_AUTH_USER and ETL_AUTH_PASS, or neither'
    );
  }
  return { username, password };
}

function constantTimeEqual(a, b) {
  const bufA = Buffer.from(a, 'utf8');
  const bufB = Buffer.from(b, 'utf8');
  if (bufA.length !== bufB.length) {
    // Still run a comparison so the timing doesn't leak the length
    // difference for free; compare against itself.
    crypto.timingSafeEqual(bufA, bufA);
    return false;
  }
  return crypto.timingSafeEqual(bufA, bufB);
}

function verifyBasicAuth(headerValue, config) {
  if (!headerValue || !headerValue.startsWith('Basic ')) {
    return false;
  }
  const encoded = headerValue.slice('Basic '.length).trim();
  let decoded;
  try {
    decoded = Buffer.from(encoded, 'base64').toString('utf8');
  } catch {
    return false;
  }
  const sep = decoded.indexOf(':');
  if (sep === -1) {
    return false;
  }
  const user = decoded.slice(0, sep);
  const pass = decoded.slice(sep + 1);
  return constantTimeEqual(user, config.username) && constantTimeEqual(pass, config.password);
}

// `config` is null when auth is disabled — the returned middleware then
// passes every request through unchanged (same behavior as the Rust side
// when ETL_AUTH_USER/PASS are both unset).
function basicAuthMiddleware(config) {
  if (!config) {
    return (_req, _res, next) => next();
  }
  return (req, res, next) => {
    if (verifyBasicAuth(req.headers.authorization, config)) {
      return next();
    }
    res.set('WWW-Authenticate', 'Basic realm="etl-engine"');
    res.status(401).json({ error: 'Unauthorized' });
  };
}

module.exports = { loadAuthConfig, basicAuthMiddleware, verifyBasicAuth, constantTimeEqual };
