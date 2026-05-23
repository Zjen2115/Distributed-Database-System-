/**
 * rp.js
 * Reverse Proxy for beeDB project.
 * Handles:
 * - Deterministic sharding
 * - Request forwarding to DN masters
 * - Receives /set_master notifications
 */

const express = require('express');
const proxy = require('express-http-proxy');
const bodyParser = require('body-parser');
const md5 = require('md5');
const url = require('url');
const axios = require('axios');
const logger = require('../logWrapper/logger');

const config = require('../configure.json');
const app = express();
const { exec } = require('child_process');
app.use(bodyParser.json());

const PORT = config.rp_port || 8000;
const NUM_DNS = config.dns.length;

logger.init({ level: 'info' });
logger.info(`RP starting on port ${PORT}`, 20001);

const dnMasters = {}; // { dnId: masterUrl }
const rpStats = { create: 0, read: 0, update: 0, delete: 0 }; // RP forwarding stats

logger.info(`>>> Runtime FOREVER_UID: ${process.env.FOREVER_UID}`, 20002);

// Helper function for consistent responses (matching DN format)
function wrapResp(data, error = 0) {
    return { resp: { error, data } };
}

// Utility: Parse key according to type hint for consistent routing
function parseKey(key, typeHint) {
    if (typeHint === 'json') {
        try {
            return JSON.parse(key);
        } catch {
            throw new Error('Invalid JSON key');
        }
    }

    if (typeHint === 'number') {
        const num = Number(key);
        if (!/^-?\d+(\.\d+)?$/.test(key) || isNaN(num)) throw new Error('Invalid number key');
        return num;
    }

    // Fallback: auto-detect type (used when no typeHint provided)
    try {
        const parsed = JSON.parse(key);
        if (typeof parsed === 'object' || typeof parsed === 'number') return parsed;
    } catch (_) {}

    return key.replace(/^"(.*)"$/, '$1'); // Clean quotes
}

// Utility: Deterministic sharding using md5(key) with proper key type handling
function keyToString(key) {
      if (key === null) return 'null:null';
    const type = typeof key;

    if (type === 'object') {
        return 'object:' + JSON.stringify(key); 
    }
    return type + ':' + String(key);
}

function determineDnId(key) {
    const keyString = keyToString(key);
    const hashHex = md5(keyString).slice(0, 8);
    const hashInt = parseInt(hashHex, 16);
    return hashInt % NUM_DNS;
}

function enforcePrv(req, res, next) {
    const originIp = req.headers['x-forwarded-for'] || req.connection.remoteAddress || '';
    if (originIp.includes('127.0.0.1') || originIp.includes('::1')) {
        next();
    } else {
        logger.warn(`prv restriction failed for /admin/loglevel from IP: ${originIp}`, 20003);
        res.status(403).json(wrapResp(0, { code: 'ePrvRestrict', errno: 403, message: 'Access restricted to localhost (prv)' }));
    }
}

function enforceRPt(req, res, next) {
    const originIp = req.headers['x-forwarded-for'] || req.connection.remoteAddress || '';
    const testClientIp = config.test_client_ip || '';
    if (
        originIp.includes('127.0.0.1') ||
        originIp.includes('::1') ||
        (testClientIp && originIp.includes(testClientIp))
    ) {
        next();
    } else {
        logger.warn(`RPt restriction failed for /stop from IP: ${originIp}`, 20004);
        res.status(403).json(wrapResp(0, { code: 'eRPtRestrict', errno: 403, message: 'Access restricted to RP or test client (RPt)' }));
    }
}

function enforceDNp(req, res, next) {
    const originIp = req.headers['x-forwarded-for'] || req.connection.remoteAddress || '';
    if (originIp.includes('127.0.0.1') || originIp.includes('::1')) {
        next();
    } else {
        logger.warn(`DNp restriction failed for /set_master from IP: ${originIp}`, 20005);
        res.status(403).json(wrapResp(0, { code: 'eDNpRestrict', errno: 403, message: 'Access restricted to DN nodes (DNp)' }));
    }
}

app.get('/set_master', enforceDNp, (req, res) => {
    const { dnId, masterId, masterUrl, term } = req.query;
     if (!dnId || !masterId || !masterUrl || !term) {
        return res.status(400).json(wrapResp(0, { code: 'eMissingParams', errno: 400, message: 'Missing dnId, masterId, masterUrl, or term' }));
    }
    dnMasters[dnId] = {
        masterId,
        masterUrl: masterUrl.startsWith('http') ? masterUrl : `http://${masterUrl}`
    };
    logger.info(`Master set for DN${dnId} -> ${dnMasters[dnId].masterUrl} (masterId ${masterId}, term ${term})`, 20006);
    res.json(wrapResp({ message: `Master for DN${dnId} updated.` }));
});

// ---------------------- Sharded CRUD Routes ----------------------

// Middleware to inject target DN URL dynamically
function dynamicProxyPath(forwardPath) {
  return async (req, res, next) => {
    const rawKey = (req.body && req.body.key) || req.query.key;
    if (!rawKey) return res.status(400).json(wrapResp(0, { code: 'eKeyRequired', errno: 400, message: 'Key is required' }));

    try {
        // Parse key according to type hint if provided (for GET requests with query params)
        const typeHint = req.query.type;
        const parsedKey = typeHint ? parseKey(rawKey, typeHint) : rawKey;
        
        logger.info(`Routing ${req.method} ${forwardPath} with raw key: ${rawKey}, type: ${typeHint}, parsed key: ${JSON.stringify(parsedKey)}`, 20007);

        const dnId = determineDnId(parsedKey);
        const dnMaster = dnMasters[dnId];
        if (!dnMaster) return res.status(502).json(wrapResp(0, { code: 'eNoMaster', errno: 502, message: `No master found for DN ${dnId}` }));

        req.dnTarget = dnMaster.masterUrl;
        logger.info(`Forwarding to DN${dnId}: ${dnMaster.masterUrl}`, 20008);
        next();
    } catch (err) {
        logger.error(`Key parsing error: ${err.message}`, 20011);
        return res.status(400).json(wrapResp(0, { code: 'eKeyParsing', errno: 400, message: err.message }));
    }
  };
}

// Unified proxy creator
function createProxy(forwardPath) {
  return proxy((req) => req.dnTarget, {
    proxyReqPathResolver: (req) => {
      const url = require('url');
      const parsedUrl = url.parse(req.originalUrl);
      const finalPath = forwardPath + (parsedUrl.search || '');
      logger.info(`Proxying to: ${req.dnTarget}${finalPath}`, 20009);
      
      // Track RP forwarding stats
      if (forwardPath === '/db/c') rpStats.create++;
      else if (forwardPath === '/db/r') rpStats.read++;
      else if (forwardPath === '/db/u') rpStats.update++;
      else if (forwardPath === '/db/d') rpStats.delete++;
      
      return finalPath;
    },
    proxyReqBodyDecorator: (body, src) => {
      logger.info(`Forwarding body: ${JSON.stringify(body)}`, 20010);
      return body;
    },
    proxyReqOptDecorator: (proxyReqOpts, src) => {
      // Ensure proper content type for JSON
      proxyReqOpts.headers['Content-Type'] = 'application/json';
      return proxyReqOpts;
    },
    proxyErrorHandler: (err, res, next) => {
      logger.error('Proxy Error Occurred', 20011);
      logger.error(`Message: ${err.message}`, 20012);
      logger.error(`Code: ${err.code}`, 20013);
      logger.error(`Stack: ${err.stack}`, 20014);

      res.status(502).json(wrapResp(0, {
        code: 'eProxyFailed',
        errno: 502,
        message: 'Proxy communication failed',
        details: {
          reason: err.code || 'UNKNOWN',
          error: err.message
        }
      }));
    },
  });
}

app.get('/db', (req, res) => {
    res.json(wrapResp({
        message: '/db root active on RP',
        note: 'Available operations: POST /db/c (Create), GET /db/r (Read), POST /db/u (Update), GET /db/d (Delete)'
    }));
    logger.info('/db accessed on RP', 20015);
});

// Dynamic proxy routes
app.post('/db/c', dynamicProxyPath('/db/c'), createProxy('/db/c'));
app.get('/db/r', dynamicProxyPath('/db/r'), createProxy('/db/r'));
app.post('/db/u', dynamicProxyPath('/db/u'), createProxy('/db/u')); // Update uses POST only per spec
app.get('/db/d', dynamicProxyPath('/db/d'), createProxy('/db/d')); // Delete uses GET with query param per spec

// ---------------------- Admin and System Routes ----------------------

app.get('/status', (req, res) => {
    res.json(wrapResp({
        status: 'RP running',
        port: PORT,
        dnMasters
    }));
});

app.get('/stats', (req, res) => {
    try {
        const total = rpStats.create + rpStats.read + rpStats.update + rpStats.delete;
        const data = {
            ...rpStats,
            total,
            message: 'RP CRUD forwarding statistics',
            port: PORT,
            activeDNs: Object.keys(dnMasters).length,
            timestamp: new Date().toISOString()
        };
        
        logger.info(`/stats accessed - RP forwarded: ${JSON.stringify(rpStats)}`, 20015);
        res.json(wrapResp(data));
    } catch (err) {
        logger.error(`Stats retrieval failed: ${err.message}`, 20016);
        res.status(500).json(wrapResp(0, { 
            code: 'eStats', 
            errno: 500, 
            message: 'Failed to retrieve RP stats'
        }));
    }
});

app.get('/admin', (req, res) => {
    res.json(wrapResp({
        message: 'Admin root on RP active',
        endpoints: ['/status', '/stats', '/admin', '/admin/loglevel', '/db', '/db/c', '/db/r', '/db/u', '/db/d', '/stop', '/set_master']
    }));
});

app.get('/admin/loglevel', enforcePrv, (req, res) => {
    const { level } = req.query;
    if (level) {
        logger.setLogLevel(level);
        res.json(wrapResp({ message: `Log level set to ${level}` }));
    } else {
        res.json(wrapResp({ message: `Current log level: ${logger.level || 'info'}` }));
    }
});

// Stop RP itself
app.get('/stop', enforceRPt, (req, res) => {
  const uid = process.env.FOREVER_UID || "rp";

  logger.info(`/stop endpoint triggered for UID: ${uid}`, 20016);
  res.json(wrapResp({ message: `Shutting down server with UID: ${uid}` }));

  if (!process.env.FOREVER_UID) {
    logger.error('FOREVER_UID not set. Cannot stop using forever. Exiting forcefully...', 20017);
    setTimeout(() => {
      logger.end();
      process.exit(1);
    }, 300);
    return;
  }

  // Delay to ensure response is flushed and logs are printed
  setTimeout(() => {
    exec(`forever stop ${uid}`, (err, stdout, stderr) => {
      if (err) {
        logger.error(`Error stopping forever UID ${uid}: ${err.message}`, 20018);
      } else {
        logger.info(`Successfully stopped forever UID ${uid}`, 20019);
        logger.info(`stdout: ${stdout}`, 20020);
        if (stderr) logger.warn(`stderr: ${stderr}`, 20021);
      }

      // Ensure logs are flushed before exiting
      setTimeout(() => {
        logger.end();
        process.exit(0);
      }, 300);
    });
  }, 500); // Wait a bit before calling forever stop
});

// Stop DN servers by server ID (e.g., /stop/dn/nodeA/serverA1)
app.get('/stop/dn/:nodeId/:serverId', enforceRPt, async (req, res) => {
  const { nodeId, serverId } = req.params;
  const serverLabel = `${nodeId}/${serverId}`;
  
  logger.info(`/stop/dn endpoint triggered for DN server: ${serverLabel}`, 20023);
  
  try {
    // Find the DN server URL from config
    const dnConfig = config.dns.find(dn => dn.servers.some(server => server.id === serverId));
    if (!dnConfig) {
      return res.status(404).json(wrapResp(0, { code: 'eServerNotFound', errno: 404, message: `Server ${serverLabel} not found in config` }));
    }
    
    const serverConfig = dnConfig.servers.find(server => server.id === serverId);
    const serverUrl = `http://${serverConfig.host}:${serverConfig.port}`;
    
    logger.info(`Forwarding stop request to DN server: ${serverUrl}`, 20024);
    
    // Forward the stop request to the specific DN server
    const response = await axios.get(`${serverUrl}/stop`, { timeout: 5000 });
    
    logger.info(`DN server ${serverLabel} stop response: ${JSON.stringify(response.data)}`, 20025);
    res.json(wrapResp({ message: `Stop request sent to ${serverLabel}`, response: response.data }));
    
  } catch (err) {
    logger.error(`Failed to stop DN server ${serverLabel}: ${err.message}`, 20026);
    res.status(500).json(wrapResp(0, { code: 'eStopDNFailed', errno: 500, message: `Failed to stop ${serverLabel}: ${err.message}` }));
  }
});

// ---------------------- Start Server ----------------------

app.listen(PORT, () => {
    logger.info(`RP is running on port ${PORT}`, 20022);
});
