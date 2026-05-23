// utils/twophase.js placeholder for Two-Phase Commit implementation
/**
 * twophase.js
 * Two-Phase Commit implementation for beeDB project
 */

const axios = require('axios');
const logger = require('../logWrapper/logger');

async function prepareCommit(peers, key, value, operation) {
    logger.info(`[2PC] Starting prepare phase for "${operation}" on key "${key}"`, 50001);

    // Prepare Phase: Ask all peers if ready
    const preparePromises = peers.map(url =>
        axios.post(`${url}/internal/prepare`, { key, value, operation }, { timeout: 1500 })
            .then(res => res.data.status === 'ready')
            .catch(err => {
                logger.warn(`[2PC] Prepare failed for peer ${url}: ${err.message}`, 50002);
                return false;
            })
    );

    const prepareResults = await Promise.all(preparePromises);

    const allReady = prepareResults.every(r => r === true);

    if (!allReady) {
        logger.warn(`[2PC] Not all peers ready, initiating abort phase for "${operation}" on key "${key}"`, 50003);

        // Abort Phase
        const abortPromises = peers.map(url =>
            axios.post(`${url}/internal/abort`, { key, value, operation }, { timeout: 1000 })
                .catch(err => {
                    logger.warn(`[2PC] Abort failed for peer ${url}: ${err.message}`, 50004);
                })
        );

        await Promise.all(abortPromises);

        return false;
    }

    logger.info(`[2PC] All peers ready, starting commit phase for "${operation}" on key "${key}"`, 50005);

    // Commit Phase
    const commitPromises = peers.map(url =>
        axios.post(`${url}/internal/commit`, { key, value, operation }, { timeout: 1500 })
            .catch(err => {
                logger.warn(`[2PC] Commit failed for peer ${url}: ${err.message}`, 50006);
            })
    );

    await Promise.all(commitPromises);

    logger.info(`[2PC] Commit phase completed for "${operation}" on key "${key}"`, 50007);
    return true;
}

module.exports = { prepareCommit };
