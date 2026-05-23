/**
 * logger.js
 * Winston logger wrapper for beeDB
 * Implements: init, end, log, setLogLevel, trace support
 */

const winston = require('winston');
const path = require('path');
const fs = require('fs-extra');

let logger;

function init(options = {}) {
    const logDir = options.logDir || path.join(__dirname, '../../logs');
    fs.ensureDirSync(logDir);

    // Add trace level
    const customLevels = {
        levels: {
            error: 0,
            warn: 1,
            info: 2,
            http: 3,
            verbose: 4,
            debug: 5,
            trace: 6,
        },
        colors: {
            error: 'red',
            warn: 'yellow',
            info: 'green',
            http: 'magenta',
            verbose: 'cyan',
            debug: 'blue',
            trace: 'gray',
        },
    };

    winston.addColors(customLevels.colors);

    logger = winston.createLogger({
        levels: customLevels.levels,
        level: options.level || 'info',
        transports: [
            // Console transport with colors
            new winston.transports.Console({
                format: winston.format.combine(
                    winston.format.colorize(),
                    winston.format.timestamp(),
                    winston.format.printf(({ timestamp, level, message }) => {
                        return `[${timestamp}] [${level}]: ${message}`;
                    })
                )
            }),
            // File transport without colors
            new winston.transports.File({ 
                filename: path.join(logDir, 'beeDB.log'),
                format: winston.format.combine(
                    winston.format.timestamp(),
                    winston.format.printf(({ timestamp, level, message }) => {
                        return `[${timestamp}] [${level.toUpperCase()}]: ${message}`;
                    })
                )
            }),
        ],
    });

    logger.info('Logger initialized.');
    return logger;
}

function end() {
    logger.info('Logger shutting down.');
    for (const transport of logger.transports) {
        transport.close();
    }
}

function log(level, message, errorCode = null) {
    if (!logger) {
        init();
    }
    
    // Format message with error code if provided
    const formattedMessage = errorCode ? `(${errorCode}): ${message}` : message;
    logger.log(level, formattedMessage);
}

function setLogLevel(level) {
    if (!logger) {
        init();
    }
    if (!Object.keys(logger.levels).includes(level)) {
        logger.warn(`Unknown log level: ${level}`);
    } else {
        logger.level = level;
        logger.info(`Log level changed to ${level}`);
    }
}

// Direct logger method wrappers for convenience
function error(message, errorCode = null) {
    if (!logger) init();
    const formattedMessage = errorCode ? `(${errorCode}): ${message}` : message;
    logger.error(formattedMessage);
}

function warn(message, errorCode = null) {
    if (!logger) init();
    const formattedMessage = errorCode ? `(${errorCode}): ${message}` : message;
    logger.warn(formattedMessage);
}

function info(message, errorCode = null) {
    if (!logger) init();
    const formattedMessage = errorCode ? `(${errorCode}): ${message}` : message;
    logger.info(formattedMessage);
}

function debug(message, errorCode = null) {
    if (!logger) init();
    const formattedMessage = errorCode ? `(${errorCode}): ${message}` : message;
    logger.debug(formattedMessage);
}

function trace(message, errorCode = null) {
    if (!logger) init();
    const formattedMessage = errorCode ? `(${errorCode}): ${message}` : message;
    logger.trace(formattedMessage);
}

module.exports = {
    init,
    end,
    log,
    setLogLevel,
    error,
    warn,
    info,
    debug,
    trace,
};
