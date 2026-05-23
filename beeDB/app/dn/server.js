/**
 * server.js
 * Clean, submission-ready DN server for beeDB with twoPhaseCommit explicitly integrated
 */

const express = require('express');
const bodyParser = require('body-parser');
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const md5 = require('md5');
const { exec } = require('child_process');

const logger = require('../logWrapper/logger');
const RaftNode = require('../utils/raft');
const twoPhase = require('../utils/twophase');
const store = require('../myFS/fileStore');

const configPath = process.argv[2];
if (!configPath) {
    console.error('Config path required: node server.js dn/configs/nodeX/serverY.json');
    process.exit(1);
}

const config = JSON.parse(fs.readFileSync(path.resolve(configPath), 'utf-8'));
const PORT = config.port;
const host = config.host;
const id = config.id;
const peers = config.peers || [];
const dataDir = config.dataDir;
const rp_ip = config.rp_ip || '127.0.0.1';
const rp_port = config.rp_port || 8000;
const rpUrl = `http://${rp_ip}:${rp_port}`;
const nodeUrl = `http://${host}:${PORT}`;
const test_ip = config.test_client_ip || '127.0.0.1';
const dnId = config.dnId || 0;

const raft = new RaftNode(id, peers, dnId, rpUrl,nodeUrl);

logger.init({ level: 'info' });
logger.info(`Starting ${id} on port ${PORT}`);

const app = express();
app.use(bodyParser.json());

const stats = { create: 0, read: 0, update: 0, delete: 0 };

function wrapResp(data, error = 0) {
    return { resp: { error, data } };
}

function enforceRPt(req, res, next) {
    const originIp = req.headers['x-forwarded-for'] || req.connection.remoteAddress || '';
    logger.info(`enforceRPt check from IP: ${originIp}`);
    console.log('enforceRPt: originIp =', originIp);
    if (originIp.includes(rp_ip) || (test_ip && originIp.includes(test_ip)) || originIp.includes('::1') || originIp.includes('127.0.0.1')) {
        next();
    } else {
        logger.warn(`RPt restriction failed from IP: ${originIp}`);
        res.status(403).json(wrapResp(0, { code: 'eRPtRestrict', errno: 403, message: 'Restricted to RP' }));
    }
}

function enforceDNp(req, res, next) {
    const originIp = req.headers['x-forwarded-for'] || req.connection.remoteAddress || '';
    if (originIp.includes('127.0.0.1') || originIp.includes('::1')) {
        next();
    } else {
        logger.warn(`DNp restriction failed from IP: ${originIp}`);
        res.status(403).json(wrapResp(0, { code: 'eDNpRestrict', errno: 403, message: 'Restricted to DNs' }));
    }
}

// ------------------ CRUD Routes with 2PC -------------------

app.get('/db', enforceRPt, (req, res) => {
    const data = {
        id,
        port: PORT,
        message: '/db root active on DN',
        endpoints: [
            '/db/c (POST) - Create key-value pair',
            '/db/r (GET) - Read with any key type (string, number, object)', 
            '/db/u (POST) - Update key-value pair',
            '/db/d (GET) - Delete with any key type (string, number, object)'
        ]
    };
    logger.info('/db accessed');
    res.json(wrapResp(data));
});

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

app.post('/db/c', enforceRPt, async (req, res) => {
    const { key, value } = req.body;
    try {
        if (key === undefined || value === undefined) throw new Error('Key and value required');
        const parsedKey = key;
        const keyType = typeof parsedKey === 'object' ? 'object' : typeof parsedKey;

        logger.info(`DN /db/c hit with ${keyType} key: ${JSON.stringify(parsedKey)}, value: ${JSON.stringify(value)}`);

        if (req.query.demo === 'true') {
            await store.saveKeyValue(dataDir, parsedKey, value);
            stats.create++;
            logger.info(`New ${keyType} key created successfully`, 10001);
            return res.json(wrapResp({ key: parsedKey, value }));
        }

        logger.info(`Starting 2PC for CREATE on ${keyType} key: ${JSON.stringify(parsedKey)}, peers: ${JSON.stringify(peers)}`);
        const success = await twoPhase.prepareCommit(peers, parsedKey, value, 'create');
        if (!success) throw new Error('Prepare phase failed during CREATE');

        await store.saveKeyValue(dataDir, parsedKey, value);
        stats.create++;
        logger.info(`New ${keyType} key created successfully`, 10001);
        res.json(wrapResp({ key: parsedKey, value }));
    } catch (err) {
        logger.error(`Database connection failed: ${err.message}`, 13005);
        res.status(500).json(wrapResp(0, { code: 'eDBCreate', errno: 500, message: err.message }));
    }
});


app.get('/db/r', enforceRPt, async (req, res) => {
    const { key, type } = req.query;

    try {
        if (!key) throw new Error('Key required');
        const parsedKey = parseKey(key, type);
        const keyType = typeof parsedKey;

        const data = await store.readKeyValue(dataDir, parsedKey);

        // Track read stats regardless of role (leader or follower)
        stats.read++;
        logger.info(`${keyType} key read successfully on ${raft.getRole()}: ${JSON.stringify(parsedKey)}`, 10002);
        res.json(wrapResp(data));
    } catch (err) {
        logger.error(`Key read failed: ${err.message}`, 13001);
        res.status(404).json(wrapResp(0, { code: 'eDBRead', errno: 404, message: err.message }));
    }
});



// Only GET method for read operations

app.post('/db/u', enforceRPt, async (req, res) => {
    const { key, value } = req.body;
    try {
        if (key === undefined || value === undefined) throw new Error('Key and value required');
        const parsedKey = key;
        const keyType = typeof parsedKey;

        const existing = await store.readKeyValue(dataDir, parsedKey);

        let updated;
        if (
            typeof value === 'object' && value !== null &&
            typeof existing.value === 'object' && existing.value !== null &&
            !Array.isArray(value) && !Array.isArray(existing.value)
        ) {
            updated = { ...existing.value, ...value };
            logger.info(`Merging object values for ${keyType} key: ${JSON.stringify(parsedKey)}`);
        } else {
            updated = value;
            logger.info(`Replacing entire value for ${keyType} key: ${JSON.stringify(parsedKey)}`);
        }

        logger.info(`Starting 2PC for UPDATE on ${keyType} key: ${JSON.stringify(parsedKey)}`);
        const success = await twoPhase.prepareCommit(peers, parsedKey, updated, 'update');
        if (!success) throw new Error('Prepare phase failed during UPDATE');

        await store.saveKeyValue(dataDir, parsedKey, updated);
        stats.update++;
        logger.info(`${keyType} key updated successfully`, 10003);
        res.json(wrapResp({ key: parsedKey, value: updated }));
    } catch (err) {
        logger.error(`Update operation failed: ${err.message}`, 13002);
        res.status(500).json(wrapResp(0, { code: 'eDBUpdate', errno: 500, message: err.message }));
    }
});


app.get('/db/d', enforceRPt, async (req, res) => {
    const { key, type } = req.query;

    try {
        if (!key) throw new Error('Key required');
        const parsedKey = parseKey(key, type);
        const keyType = typeof parsedKey;

        await store.readKeyValue(dataDir, parsedKey); // Ensure key exists

        logger.info(`Starting 2PC for DELETE on ${keyType} key: ${JSON.stringify(parsedKey)}`);
        const success = await twoPhase.prepareCommit(peers, parsedKey, null, 'delete');
        if (!success) throw new Error('Prepare phase failed during DELETE');

        await store.deleteKeyValue(dataDir, parsedKey);
        stats.delete++;
        logger.info(`${keyType} key deleted successfully`, 10004);
        res.json(wrapResp({ key: parsedKey, deleted: true }));
    } catch (err) {
        logger.error(`Delete operation failed: ${err.message}`, 13003);
        res.status(500).json(wrapResp(0, { code: 'eDBDelete', errno: 500, message: err.message }));
    }
});


// Only GET method for delete operations


// ------------------ System Routes -------------------

app.get('/status', (req, res) => {
    try {
        res.json(wrapResp({
            id,
            port: PORT,
            role: raft.getRole(),
            term: raft.getTerm(),
            startTime: new Date().toISOString()
        }));
    } catch (err) {
        logger.error(`Status check failed: ${err.message}`);
        res.status(500).json(wrapResp(0, { code: 'eStatus', errno: 500, message: err.message }));
    }
});

app.get('/stats', (req, res) => {
    try {
        const data = {
            ...stats,
            role: raft.getRole(),
            term: raft.getTerm(),
            leader: raft.getLeader(),
            id,
            port: PORT,
            message: `CRUD stats for ${raft.getRole()} node (distributed system reflects all operations)`
        };
        res.json(wrapResp(data));
        logger.info(`/stats accessed - CRUD counts: ${JSON.stringify(stats)}, Role: ${raft.getRole()}`, 10006);
    } catch (err) {
        logger.error(`Stats retrieval failed: ${err.message}`);
        res.status(500).json(wrapResp(0, { code: 'eStats', errno: 500, message: err.message }));
    }
});

app.get('/admin', (req, res) => {
    try {
        const uptimeSeconds = Math.floor(process.uptime());
        const data = {
            id,
            port: PORT,
            role: raft.getRole(),
            term: raft.getTerm(),
            logLevel: logger.level || 'info',
            startTime: new Date(Date.now() - uptimeSeconds * 1000).toISOString(),
            uptimeSeconds
        };
        logger.info('/admin accessed');
        res.json(wrapResp(data));
    } catch (err) {
        logger.error(`Admin info retrieval failed: ${err.message}`);
        res.status(500).json(wrapResp(0, { code: 'eAdmin', errno: 500, message: err.message }));
    }
});

app.get('/admin/loglevel', (req, res) => {
    const { level } = req.query;
    try {
        if (level) {
            // Validate log level
            const validLevels = ['error', 'warn', 'info', 'debug', 'trace'];
            if (!validLevels.includes(level)) {
                logger.warn('Invalid log level requested', 20001);
                res.status(400).json(wrapResp(0, { code: 'eInvalidLogLevel', errno: 400, message: 'Invalid log level' }));
                return;
            }
            
            logger.setLogLevel(level);
            logger.info(`Log level changed to ${level} on ${id}`, 10005);
            res.json(wrapResp({ message: `Log level set to ${level}` }));
        } else {
            res.json(wrapResp({ message: `Current log level: ${logger.level || 'info'}` }));
        }
    } catch (err) {
        logger.error('Failed to change log level', 13004);
        res.status(500).json(wrapResp(0, { code: 'eLogLevel', errno: 500, message: err.message }));
    }
});

app.get('/stop', enforceRPt, (req, res) => {
    const uid = process.env.FOREVER_UID || id;
    
    logger.info(`>>> Runtime FOREVER_UID: ${process.env.FOREVER_UID}`, 10018);
    logger.info(`/stop endpoint triggered for server ${id}`);
    logger.info(`FOREVER_UID environment variable: ${process.env.FOREVER_UID}`);
    logger.info(`Using UID: ${uid} for forever stop`);
    
    res.json(wrapResp({ message: `Shutting down server ${id} with UID: ${uid}` }));

    if (!process.env.FOREVER_UID) {
        logger.error('FOREVER_UID not set. Cannot stop using forever. Exiting forcefully...');
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
                logger.error(`Error stopping forever UID ${uid}: ${err.message}`);
            } else {
                logger.info(`Successfully stopped forever UID ${uid}`);
                logger.info(`stdout: ${stdout}`);
                if (stderr) logger.warn(`stderr: ${stderr}`);
            }

            // Ensure logs are flushed before exiting
            setTimeout(() => {
                logger.end();
                process.exit(0);
            }, 300);
        });
    }, 500); // Wait a bit before calling forever stop
});

// ------------------ Raft Routes -------------------

app.post('/raft/vote', bodyParser.json(), (req, res) => raft.handleVoteRequest(req, res));
app.post('/raft/heartbeat', bodyParser.json(), (req, res) => raft.handleHeartbeat(req, res));
app.get('/raft/status', (req, res) => {
    try {
        res.json({
            resp: {
                error: 0,
                data: {
                    id,
                    role: raft.getRole(),
                    term: raft.getTerm(),
                    leader: raft.getLeader()
                }
            }
        });
        logger.info('/raft/status accessed');
    } catch (err) {
        logger.error(`Raft status retrieval failed: ${err.message}`);
        res.status(500).json({
            resp: {
                error: { code: 'eRaftStatus', errno: 500, message: err.message },
                data: 0
            }
        });
    }
});

app.get('/election', enforceDNp, (req, res) => {
    try {
        const data = {
            id,
            term: raft.getTerm(),
            role: raft.getRole(),
            leader: raft.getLeader(),
            message: 'Election info provided for DN master establishment.'
        };
        logger.info('/election accessed for DN master info');
        res.json(wrapResp(data));
    } catch (err) {
        logger.error(`Election info retrieval failed: ${err.message}`);
        res.status(500).json(wrapResp(0, { code: 'eElection', errno: 500, message: err.message }));
    }
});

app.get('/maintenance', enforceDNp, async (req, res) => {
    try {
        const keys = await store.listKeyHashes(dataDir);
        const data = {
            id,
            storedKeys: keys,
            message: 'Maintenance sync data provided.'
        };
        logger.info('/maintenance accessed for DN sync');
        res.json(wrapResp(data));
    } catch (err) {
        logger.error(`/maintenance error: ${err.message}`);
        res.status(500).json(wrapResp(0, { code: 'eMaintenance', errno: 500, message: err.message }));
    }
});

// ------------------ Start Server -------------------

// 2PC Internal Endpoints for twophase.js coordination
app.post('/internal/prepare', enforceDNp, (req, res) => {
    const { key, value, operation } = req.body;
    logger.info(`[2PC-Internal] Received prepare for key "${key}", operation "${operation}"`);
    // For now, always accept for testing:
    res.json({ status: 'ready' });
});

app.post('/internal/abort', enforceDNp, (req, res) => {
    const { key, operation } = req.body;
    logger.info(`[2PC-Internal] Received abort for key "${key}", operation "${operation}"`);
    res.json({ status: 'aborted' });
});

app.post('/internal/commit', enforceDNp, async (req, res) => {
    const { key, value, operation } = req.body;
    logger.info(`[2PC-Internal] Received commit for key "${key}", operation "${operation}"`);

    try {
        if (operation === 'create' || operation === 'update') {
            await store.saveKeyValue(dataDir, key, value);
            logger.info(`Internal ${operation} committed: ${key}`);
            // Update stats for follower operations
            if (operation === 'create') {
                stats.create++;
            } else if (operation === 'update') {
                stats.update++;
            }
        } else if (operation === 'delete') {
            await store.deleteKeyValue(dataDir, key);
            logger.info(`Internal delete committed: ${key}`);
            // Update stats for follower operations
            stats.delete++;
        }

        res.json({ status: 'committed' });
    } catch (err) {
        logger.error(`Internal commit failed: ${err.message}`);
        res.status(500).json({ status: 'error', message: err.message });
    }
});

app.use((err, req, res, next) => {
    logger.info(`Incoming request: ${JSON.stringify(req.headers)} ${req.url}`);
    logger.error(`Uncaught error: ${err.message}`);
    res.status(500).json(wrapResp(0, {
        code: 'eInternalServer',
        errno: 500,
        message: err.message
    }));
});

app.listen(PORT, async () => {
    try {
        logger.info(`Server ${id} running on port ${PORT}`);
        logger.info(`Server accessible at http://${host}:${PORT}`);
    } catch (err) {
        logger.error(`Server startup error: ${err.message}`);
        process.exit(1);
    }
});
