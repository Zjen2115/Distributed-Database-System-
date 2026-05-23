/**
 * beeDB.js with logger: All servers run under forever
 * Usage: node beeDB.js [start|stop|restart|status|stats]
 */

const { spawn, execSync } = require('child_process');
const path = require('path');
const fs = require('fs');
const axios = require('axios');
const logger = require('./logWrapper/logger');
logger.init({ level: 'info' });

const config = JSON.parse(fs.readFileSync(path.join(__dirname, 'configure.json'), 'utf-8'));
const action = process.argv[2];
if (!action) {
  logger.error("Usage: node beeDB.js [start|stop|restart|status|stats]", 40001);
  process.exit(1);
}

// Build DN and RP process commands
const servers = [];
for (const dn of config.dns) {
  for (const srv of dn.servers) {
    servers.push({
      id: srv.id,
      name: srv.id,
      cmd: `dn/server.js dn/configs/${srv.id}.json`,
      port: srv.port,
    });
  }
}
const rp = { name: 'rp', cmd: 'rp/rp.js' };

// --- Forever abstraction utility for all servers --- //
function foreverStart(label, script) {
  return new Promise((resolve) => {
    const baseForeverDir = path.join(process.env.USERPROFILE, '.forever');
    const pidDir = path.join(baseForeverDir, 'pids', label);
    const labelDir = path.join(baseForeverDir, label); // <-- ensure this exists

    try {
      fs.mkdirSync(pidDir, { recursive: true });
      fs.mkdirSync(labelDir, { recursive: true }); // <-- add this
    } catch (err) {
      logger.error(`Directory creation failed: ${err.message}`, 40002);
    }

    const cmd = `forever start -a --uid "${label}" -o nul -e nul ${script}`;
    const p = spawn(cmd, { shell: true, stdio: 'inherit', env: { ...process.env, FOREVER_UID: label } });

    p.on('exit', code => {
      if (code === 0)
        logger.info(`Started ${label} with forever`, 40003);
      else
        logger.error(`Failed to start ${label} with forever, exit code ${code}`, 40004);
      resolve();
    });
  });
}

function foreverStop(label) {
  return new Promise((resolve) => {
    const p = spawn(`forever stop "${label}"`, { shell: true, stdio: 'inherit' });
    p.on('exit', code => {
      if (code === 0) logger.info(`Stopped ${label}`, 40005);
      else            logger.warn(`Could not stop ${label}: exit code ${code}`, 40006);
      resolve();
    });
  });
}
function foreverListRaw() {
  try {
    return execSync('forever list', { encoding: 'utf-8' });
  } catch {
    return '';
  }
}

// --- Process control --- //
async function startServers() {
  for (const srv of servers) await foreverStart(srv.name, srv.cmd);
  await foreverStart(rp.name, rp.cmd);
}
async function stopServers() {
  for (const srv of servers) await foreverStop(srv.name);
  await foreverStop(rp.name);
}
async function restartServers() {
  await stopServers();
  setTimeout(startServers, 3000);
}
function statusServers() {
  const foreverList = foreverListRaw();
  servers.forEach(srv => {
    if (foreverList.includes(srv.name)) {
      logger.info(`${srv.name} running under forever`, 40007);
    } else {
      logger.info(`${srv.name} not running under forever`, 40008);
    }
  });
  if (foreverList.includes(rp.name)) {
    logger.info(`RP running under forever`, 40009);
  } else {
    logger.info(`RP not running under forever`, 40010);
  }
}
async function statsServers() {
  logger.info('Collecting stats from all DN servers...', 40011);
  const stats = { create: 0, read: 0, update: 0, delete: 0 };
  for (const srv of servers) {
    try {
      const res = await axios.get(`http://127.0.0.1:${srv.port}/stats`, { timeout: 1000 });
      if (res.data && res.data.resp && res.data.resp.data) {
        const s = res.data.resp.data;
        stats.create += s.create || 0;
        stats.read   += s.read || 0;
        stats.update += s.update || 0;
        stats.delete += s.delete || 0;
        logger.info(`Stats from ${srv.name}: ${JSON.stringify(s)}`, 40012);
      } else {
        logger.warn(`Invalid stats format from ${srv.name}`, 40013);
      }
    } catch {
      logger.error(`Could not fetch stats from ${srv.name}`, 40014);
    }
  }
  logger.info(`Aggregated Stats: ${JSON.stringify(stats)}`, 40015);
}

// --- Main control --- //
(async () => {
  if      (action === 'start')     await startServers();
  else if (action === 'stop')      await stopServers();
  else if (action === 'restart')   await restartServers();
  else if (action === 'status')    statusServers();
  else if (action === 'stats')     await statsServers();
  else   logger.error("Unknown action. Use start, stop, restart, status, or stats.", 40016);
})();
