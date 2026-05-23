const fs = require('fs-extra');
const path = require('path');
const md5 = require('md5');
const logger = require('../logWrapper/logger');

/**
 * Generates a stable string representation for any key type
 */
function keyToString(key) {
    if (typeof key === 'object' && key !== null) {
        return JSON.stringify(key);
    }
    return String(key);
}

/**
 * Generates the file path for a given key using MD5 for stable naming.
 */
function getFilePath(dataDir, key) {
    const keyString = keyToString(key);
    const hash = md5(keyString);
    return path.join(dataDir, `${hash}.json`);
}

/**
 * Saves a key-value pair in the specified data directory.
 */
const saveKeyValue = async (storePath, key, value) => {
    try {
        const keyString = keyToString(key);
        const hashedKey = md5(keyString);
        const filePath = path.join(storePath, `${hashedKey}.json`);
        
        logger.debug(`[FileStore] Saving key: ${keyString} (hash: ${hashedKey})`, 60001);
        
        const data = { key, value, timestamp: Date.now() };
        await fs.outputJson(filePath, data, { spaces: 2 });
        
        logger.info(`[FileStore] Successfully saved key: ${keyString}`, 60002);
        return true;
    } catch (error) {
        logger.error(`[FileStore] Failed to save key: ${keyString} - ${error.message}`, 60003);
        return false;
    }
};

/**
 * Reads the value for a given key.
 */
async function readKeyValue(dataDir, key) {
    try {
        const filePath = getFilePath(dataDir, key);
        const keyString = keyToString(key);
        const hash = md5(keyString);
        
        logger.debug(`[FileStore] Reading key: ${keyString} (hash: ${hash})`, 60004);
        
        if (await fs.pathExists(filePath)) {
            const data = await fs.readJson(filePath);
            logger.info(`[FileStore] Successfully read key: ${keyString}`, 60005);
            return data;
        } else {
            logger.warn(`[FileStore] Key not found: ${keyString}`, 60006);
            return null;
        }
    } catch (error) {
        logger.error(`[FileStore] Failed to read key: ${keyString} - ${error.message}`, 60007);
        throw error;
    }
}

/**
 * Deletes the key-value pair for a given key.
 */
async function deleteKeyValue(dataDir, key) {
    try {
        const filePath = getFilePath(dataDir, key);
        const keyString = keyToString(key);
        const hash = md5(keyString);
        
        logger.debug(`[FileStore] Deleting key: ${keyString} (hash: ${hash})`, 60008);
        
        if (await fs.pathExists(filePath)) {
            await fs.remove(filePath);
            logger.info(`[FileStore] Successfully deleted key: ${keyString}`, 60009);
        } else {
            logger.warn(`[FileStore] Attempted to delete non-existent key: ${keyString}`, 60010);
        }
    } catch (error) {
        logger.error(`[FileStore] Failed to delete key: ${keyString} - ${error.message}`, 60011);
        throw error;
    }
}

/**
 * Checks if the key exists.
 */
async function keyExists(dataDir, key) {
    try {
        const filePath = getFilePath(dataDir, key);
        const hash = md5(key);
        
        logger.debug(`[FileStore] Checking existence of key: ${key} (hash: ${hash})`, 60012);
        
        const exists = await fs.pathExists(filePath);
        
        if (exists) {
            logger.debug(`[FileStore] Key exists: ${key}`, 60013);
        } else {
            logger.debug(`[FileStore] Key does not exist: ${key}`, 60014);
        }
        
        return exists;
    } catch (error) {
        logger.error(`[FileStore] Failed to check key existence: ${key} - ${error.message}`, 60015);
        throw error;
    }
}

async function listKeyHashes(dataDir) {
    try {
        logger.debug(`[FileStore] Listing key hashes in directory: ${dataDir}`, 60016);
        
        const files = await fs.readdir(dataDir);
        const keyHashes = files
            .filter(file => file.endsWith('.json'))
            .map(file => path.basename(file, '.json'));
            
        logger.info(`[FileStore] Found ${keyHashes.length} key hashes in directory`, 60017);
        return keyHashes;
    } catch (error) {
        logger.error(`[FileStore] Failed to list key hashes in directory: ${dataDir} - ${error.message}`, 60018);
        throw error;
    }
}

module.exports = {
    saveKeyValue,
    readKeyValue,
    deleteKeyValue,
    keyExists,
    listKeyHashes
};
