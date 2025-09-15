const WebSocket = require('ws');
const readline = require('readline');
const os = require('os');
const fs = require('fs').promises;
const path = require('path');
const EventEmitter = require('events');

const SUPPORTED_PROTOCOL_VERSION = 1;

const r = Math.random().toString(36).substring(2, 15); // Random suffix for log file
function log(...args) {
    const timestamp = new Date().toISOString();
    const logMessage = `[${timestamp}] [Agent] ${args.join(' ')}`;
    console.error(logMessage); // Log to stderr for agent's own logs
    fs.appendFile(path.join(os.tmpdir(), 'turbolfs-agent-' + r + '.log'), logMessage + '\n').catch(err => console.error("Failed to write to agent log file:", err));
}

function debugLog(...args) {
    log(...args);
}

const MessageType = {
    // Agent -> Aggregator
    OBJECT_DOWNLOAD_REQUEST: 0x01,

    // Aggregator -> Agent
    SERVER_PROTOCOL_INFO: 0xA0,
    OBJECT_DOWNLOAD_START_RESPONSE: 0xA1,
    OBJECT_DATA_CHUNK: 0xA2,
    OBJECT_TRANSFER_ERROR_NOTIFICATION: 0xA3,
};

const StatusCode = {
    OK: 0,
    ERROR_GENERAL: 1,
    ERROR_NOT_FOUND: 2,
    ERROR_TOO_LARGE_FOR_PROTOCOL: 3, // If server has internal limits that are hit before streaming
};


function makeObjectDownloadRequestMessage({ oid, size }) {
    const msgTypeByte = Buffer.from([MessageType.OBJECT_DOWNLOAD_REQUEST]);
    const oidBin = Buffer.from(oid, 'hex');
    const sizeBin = Buffer.alloc(8);
    sizeBin.writeBigUInt64BE(BigInt(size), 0);

    if (oidBin.length !== 32) {
        throw new Error(`Invalid object ID length: expected 32 bytes, got ${oidBin.length} bytes`);
    }
    return Buffer.concat([msgTypeByte, oidBin, sizeBin]);
}

const state = {
    lfs: null,
    wsClient: null,
    tempDirectory: null,
    downloadQueue: [],
    isProcessingQueue: false,
    currentDownload: null, // { oid, size, path, fileHandle, bytesWritten }
    serverProtocolVersion: null,
    agentReadyForLFS: false,
    incomingMessagesQueue: [], // Queue them to process sequentially, avoiding concurrency issues in writing to a file
    incomingMessagesQueueAddEvent: new EventEmitter(),

    // --- properties for reconnection ---
    wsUrl: null,
    reconnectDelay: 1000,
    maxReconnectDelay: 60000,
    reconnectTimerId: null,
};

async function main(backendUrlArg) {
    state.tempDirectory = await createTempDirectory();
    state.lfs = initLfsProtocolHandler();

    let wsUrl;
    try {
        const url = new URL(backendUrlArg);
        if (url.protocol !== 'ws:' && url.protocol !== 'wss:') {
            log(`Error: Backend URL must use ws: or wss: protocol. Got: ${url.protocol}`);
            process.exit(1);
        }
        wsUrl = backendUrlArg;
    } catch (e) {
        log(`Error: Invalid backend URL "${backendUrlArg}". ${e.message}`);
        process.exit(1);
    }

    initNetworking(wsUrl);
    initIncomingMessagesQueueLoop();
}

function scheduleReconnect() {
    // If a reconnect is already scheduled, do nothing.
    if (state.reconnectTimerId) {
        return;
    }

    log(`Connection lost. Will attempt to reconnect in ${state.reconnectDelay / 1000} seconds.`);

    state.reconnectTimerId = setTimeout(() => {
        log(`Attempting to reconnect to ${state.wsUrl}...`);
        initNetworking(state.wsUrl); // Call the main connection function again
        state.reconnectTimerId = null; // Clear the timer ID after it has run
    }, state.reconnectDelay);

    // Apply exponential backoff for the next potential failure
    state.reconnectDelay = Math.min(state.reconnectDelay * 2, state.maxReconnectDelay);
}

async function createTempDirectory() {
    // We use a temporary directory inside the .git directory, not the system temp directory,
    // to prevent cross-device issues when Git LFS will move files.
    try {
        const gitDir = path.join(process.cwd(), '.git');
        const gitExists = await fs.access(gitDir).then(() => true).catch(() => false);
        if (!gitExists) {
            throw new Error(`.git directory not found in current working directory: ${process.cwd()}`);
        }

        const parentTmpDir = path.join(gitDir, 'turbolfs');
        await fs.mkdir(parentTmpDir, { recursive: true }); // recursive to prevent throw if directory exists
        const res = await fs.mkdtemp(path.join(parentTmpDir, `tmp-`));
        log(`Created temporary directory: ${res}`);
        return res;
    } catch (err) {
        log(`FATAL: Could not create temporary directory: ${err.message}`);
        process.exit(1);
    }
}

async function cleanupTempDirectory() {
    if (state.tempDirectory) {
        try {
            await fs.rm(state.tempDirectory, { recursive: true, force: true });
            log(`Cleaned up temporary directory: ${state.tempDirectory}`);
        } catch (err) {
            log(`Error cleaning up temporary directory ${state.tempDirectory}: ${err.message}`);
        }
    }
}

async function processDownloadQueue() {
    if (state.isProcessingQueue || state.downloadQueue.length === 0 || !state.wsClient || state.wsClient.readyState !== WebSocket.OPEN || !state.agentReadyForLFS) {
        return;
    }
    state.isProcessingQueue = true;

    const item = state.downloadQueue.shift();
    log(`Processing OID from queue: ${item.oid}, Expected Size: ${item.size}`);

    const tempFilePath = path.join(state.tempDirectory, item.oid);

    try {
        const fileHandle = await fs.open(tempFilePath, 'w');
        state.currentDownload = {
            oid: item.oid,
            expectedSize: BigInt(item.size),
            path: tempFilePath,
            fileHandle,
            bytesWritten: BigInt(0),
        };

        const requestMessage = makeObjectDownloadRequestMessage({ oid: item.oid, size: item.size });
        state.wsClient.send(requestMessage);
        log(`Sent OBJECT_DOWNLOAD_REQUEST for OID: ${item.oid}`);
    } catch (err) {
        log(`Error starting download for ${item.oid}: ${err.message}`);
        state.lfs.sendToLfs({ event: "complete", oid: item.oid, error: { code: 1, message: `Agent failed to initiate download: ${err.message}` } });
        if (state.currentDownload && state.currentDownload.fileHandle) {
            await state.currentDownload.fileHandle.close().catch(e => log(`Error closing file for ${item.oid} after init error: ${e.message}`));
        }
        state.currentDownload = null;
        state.isProcessingQueue = false;
        processDownloadQueue(); // Try next item
    }
}


function handleServerProtocolInfo(message) {
    if (message.length < 4) {
        log("Error: SERVER_PROTOCOL_INFO message too short.");
        state.wsClient.close();
        return;
    }
    state.serverProtocolVersion = message.readUInt32BE(0);
    log(`Received server protocol version: ${state.serverProtocolVersion}`);

    if (state.serverProtocolVersion !== SUPPORTED_PROTOCOL_VERSION) {
        log(`Unsupported server protocol version: ${state.serverProtocolVersion}. Agent supports ${SUPPORTED_PROTOCOL_VERSION}. Closing connection.`);
        state.wsClient.close();
        // No process.exit(1) here, allow LFS to terminate if it wants, or connection retry logic if implemented
    } else {
        log("Server protocol version compatible.");
        state.agentReadyForLFS = true; // Agent is ready after protocol handshake
        // If LFS 'init' has already been received and we were waiting for this.
        if (state.lfs && typeof state.lfs.sendToLfs === 'function' && !state.lfs.initSent) {
            // This was moved to LFS init handler
        }
        processDownloadQueue(); // Start processing if anything is queued
    }
}

async function handleObjectDownloadStartResponse(message) {
    if (!state.currentDownload) {
        log("Error: Received OBJECT_DOWNLOAD_START_RESPONSE but no download is active.");
        return;
    }
    const oidHex = message.slice(0, 32).toString('hex');
    if (oidHex !== state.currentDownload.oid) {
        log(`Error: Received OBJECT_DOWNLOAD_START_RESPONSE for OID ${oidHex}, but current download is for ${state.currentDownload.oid}.`);
        // This case should ideally not happen if requests are serialized.
        return;
    }

    const totalObjectSize = message.readBigUInt64BE(32);
    const statusCode = message.readUInt8(32 + 8);

    log(`Received OBJECT_DOWNLOAD_START_RESPONSE for OID: ${oidHex}, Server Declared Size: ${totalObjectSize}, Status: ${statusCode}`);

    if (statusCode !== StatusCode.OK) {
        let errorMessage = "Unknown error from server.";
        if (message.length > (32 + 8 + 1)) {
            const errorMsgLen = message.readUInt32BE(32 + 8 + 1);
            if (message.length >= (32 + 8 + 1 + 4 + errorMsgLen)) {
                errorMessage = message.slice(32 + 8 + 1 + 4, 32 + 8 + 1 + 4 + errorMsgLen).toString('utf-8');
            } else {
                log("Error message in response is truncated or length is incorrect.");
            }
        }
        log(`Download failed for OID ${oidHex}. Server Error: ${errorMessage}`);
        state.lfs.sendToLfs({ event: "complete", oid: oidHex, error: { code: statusCode, message: `Server error: ${errorMessage}` } });
        await state.currentDownload.fileHandle.close().catch(e => log(`Error closing file for ${oidHex} after server error: ${e.message}`));
        try { await fs.unlink(state.currentDownload.path); } catch (e) { /* ignore */ }
        state.currentDownload = null;
        state.isProcessingQueue = false;
        processDownloadQueue();
    } else {
        // Store server-declared size, might be different from LFS size (LFS size is advisory for agent)
        state.currentDownload.serverDeclaredSize = totalObjectSize;
        if (totalObjectSize === BigInt(0)) { // Empty file
            log(`OID ${oidHex} is an empty file (0 bytes). Download complete.`);
            await state.currentDownload.fileHandle.close();
            state.lfs.sendToLfs({ event: "complete", oid: oidHex, path: state.currentDownload.path });
            state.currentDownload = null;
            state.isProcessingQueue = false;
            processDownloadQueue();
        }
        // Else, wait for OBJECT_DATA_CHUNK messages
    }
}

async function handleObjectDataChunk(message) {
    if (!state.currentDownload) {
        log("Error: Received OBJECT_DATA_CHUNK but no download is active.");
        return;
    }
    const oidHex = message.slice(0, 32).toString('hex');
    if (oidHex !== state.currentDownload.oid) {
        log(`Error: Received OBJECT_DATA_CHUNK for OID ${oidHex}, but current download is for ${state.currentDownload.oid}. Ignoring chunk.`);
        return;
    }

    const chunkData = message.slice(32);

    try {
        await state.currentDownload.fileHandle.write(chunkData);
        state.currentDownload.bytesWritten += BigInt(chunkData.length);

        // TODO: Consider sending progress to LFS
        // state.lfs.sendToLfs({ event: "progress", oid: oidHex, bytesSoFar: Number(state.currentDownload.bytesWritten), bytesSinceLast: chunkData.length });

        if (state.currentDownload.bytesWritten > state.currentDownload.serverDeclaredSize) {
            log(`Error: Received more data for OID ${oidHex} (${state.currentDownload.bytesWritten}) than expected server size (${state.currentDownload.serverDeclaredSize}). Aborting.`);
            state.lfs.sendToLfs({ event: "complete", oid: oidHex, error: { code: 1, message: "Received excess data from server." } });
            await state.currentDownload.fileHandle.close().catch(e => log(`Error closing file for ${oidHex} after excess data: ${e.message}`));
            try { await fs.unlink(state.currentDownload.path); } catch (e) { /* ignore */ }
            state.currentDownload = null;
            state.isProcessingQueue = false;
            processDownloadQueue();
            return;
        }

        if (state.currentDownload.bytesWritten === state.currentDownload.serverDeclaredSize) {
            log(`Successfully downloaded OID ${oidHex} (${state.currentDownload.bytesWritten} bytes) to ${state.currentDownload.path}`);
            await state.currentDownload.fileHandle.close();

            if (state.currentDownload.bytesWritten !== state.currentDownload.expectedSize) {
                log(`Warning: LFS expected size ${state.currentDownload.expectedSize} but received ${state.currentDownload.bytesWritten} for OID ${oidHex}. Proceeding with received size.`);
            }

            // close file
            await state.currentDownload.fileHandle.close();

            state.lfs.sendToLfs({ event: "complete", oid: oidHex, path: state.currentDownload.path });
            state.currentDownload = null;
            state.isProcessingQueue = false;
            processDownloadQueue();
        }
    } catch (err) {
        log(`Error writing chunk to file for OID ${oidHex}: ${err.message}`);
        state.lfs.sendToLfs({ event: "complete", oid: oidHex, error: { code: 1, message: `File I/O error: ${err.message}` } });
        if (state.currentDownload.fileHandle) {
            await state.currentDownload.fileHandle.close().catch(e => log(`Error closing file for ${oidHex} after write error: ${e.message}`));
        }
        try { await fs.unlink(state.currentDownload.path); } catch (e) { /* ignore */ }
        state.currentDownload = null;
        state.isProcessingQueue = false;
        processDownloadQueue();
    }
}

async function handleObjectTransferErrorNotification(message) {
    if (!state.currentDownload) {
        log("Error: Received OBJECT_TRANSFER_ERROR_NOTIFICATION but no download is active.");
        return;
    }
    const oidHex = message.slice(0, 32).toString('hex');
    if (oidHex !== state.currentDownload.oid) {
        log(`Error: Received OBJECT_TRANSFER_ERROR_NOTIFICATION for OID ${oidHex}, but current download is for ${state.currentDownload.oid}.`);
        return;
    }

    let errorMessage = "Unknown transfer error from server.";
    if (message.length > 32) {
        const errorMsgLen = message.readUInt32BE(32);
        if (message.length >= (32 + 4 + errorMsgLen)) {
            errorMessage = message.slice(32 + 4, 32 + 4 + errorMsgLen).toString('utf-8');
        } else {
            log("Error message in transfer error notification is truncated or length is incorrect.");
        }
    }

    log(`Mid-transfer error for OID ${oidHex}. Server Error: ${errorMessage}`);
    state.lfs.sendToLfs({ event: "complete", oid: oidHex, error: { code: 1, message: `Server mid-transfer error: ${errorMessage}` } });
    await state.currentDownload.fileHandle.close().catch(e => log(`Error closing file for ${oidHex} after server mid-transfer error: ${e.message}`));
    try { await fs.unlink(state.currentDownload.path); } catch (e) { /* ignore */ }
    state.currentDownload = null;
    state.isProcessingQueue = false;
    processDownloadQueue();
}


function initNetworking(wsUrl) {
    // Store the URL in the state so we can use it for reconnecting
    state.wsUrl = wsUrl;

    log(`Attempting to connect to WebSocket server: ${wsUrl}`);
    const client = new WebSocket(wsUrl, {
        binaryType: 'nodebuffer',
    });
    state.wsClient = client;

    let heartbeatInterval;
    let isAlive = true;

    function heartbeat() {
        // 1. If the server didn't respond to the last ping, the connection is dead.
        if (isAlive === false) {
            log("No pong received from server since last ping. Terminating connection.");
            clearInterval(heartbeatInterval); // Clean up immediately
            return client.terminate();
        }

        // 2. Assume the connection is dead until a pong proves otherwise.
        isAlive = false;

        // 3. Send the ping. The pong handler will set isAlive back to true.
        client.ping();
        debugLog("Sent ping to server.");
    }

    client.on('pong', () => {
        // The server is alive.
        isAlive = true;
        debugLog("Received pong from server.");
    });

    client.on('open', () => {
        log(`Connected to WebSocket server: ${wsUrl}`);
        state.reconnectDelay = 1000;
        if (state.reconnectTimerId) {
            clearTimeout(state.reconnectTimerId);
            state.reconnectTimerId = null;
        }

        // Start the heartbeat interval. Run it every 15 seconds.
        // It will terminate if a pong isn't received within that 15-second window.
        isAlive = true; // Start with a clean slate
        heartbeatInterval = setInterval(heartbeat, 15000);
    });

    client.on('message', async (data) => {
        if (!Buffer.isBuffer(data)) {
            log(`Received non-buffer message from server: ${typeof data}. Expected Buffer.`);
            process.exit(1);
        }
        const message = data;

        if (message.length === 0) {
            log("Received empty message from server, ignoring.");
            return;
        }
        const messageType = message.readUInt8(0);
        const payload = message.slice(1);

        switch (messageType) {
            case MessageType.SERVER_PROTOCOL_INFO:
                handleServerProtocolInfo(payload);
                break;
            case MessageType.OBJECT_DOWNLOAD_START_RESPONSE:
                state.incomingMessagesQueue.push(() => handleObjectDownloadStartResponse(payload));
                state.incomingMessagesQueueAddEvent.emit('messageAdded');
                break;
            case MessageType.OBJECT_DATA_CHUNK:
                state.incomingMessagesQueue.push(() => handleObjectDataChunk(payload));
                state.incomingMessagesQueueAddEvent.emit('messageAdded');
                break;
            case MessageType.OBJECT_TRANSFER_ERROR_NOTIFICATION:
                state.incomingMessagesQueue.push(() => handleObjectTransferErrorNotification(payload));
                state.incomingMessagesQueueAddEvent.emit('messageAdded');
                break;
            default:
                log(`Unknown message type 0x${messageType.toString(16)} received from server. Ignoring.`);
        }
    });

    client.on('error', (err) => {
        log(`WebSocket Error: ${err.message}`);
        // The 'close' event will usually fire immediately after 'error',
        // so we'll let the 'close' handler manage the reconnection logic.
        if (state.currentDownload) {
            state.lfs.sendToLfs({ event: "complete", oid: state.currentDownload.oid, error: { code: 1, message: `WebSocket connection error: ${err.message}` } });
            if (state.currentDownload.fileHandle) {
                state.currentDownload.fileHandle.close().catch(e => log(`Error closing file: ${e.message}`));
            }
            state.currentDownload = null;
        }
        state.isProcessingQueue = false;
        state.agentReadyForLFS = false; // No longer ready if connection drops
    });

    client.on('close', (code, reason) => {
        clearInterval(heartbeatInterval);

        const reasonStr = reason ? reason.toString() : 'No reason provided';
        log(`Disconnected from WebSocket server. Code: ${code}, Reason: ${reasonStr}`);
        state.agentReadyForLFS = false;

        if (state.currentDownload) {
            log(`Connection closed during active download of ${state.currentDownload.oid}. Reporting error to LFS.`);
            state.lfs.sendToLfs({ event: "complete", oid: state.currentDownload.oid, error: { code: 1, message: `Connection closed during transfer. Code: ${code}` } });
            if (state.currentDownload.fileHandle) {
                state.currentDownload.fileHandle.close().catch(e => log(`Error closing file: ${e.message}`));
            }
            state.currentDownload = null;
        }
        state.isProcessingQueue = false;

        scheduleReconnect();
    });
}

function initIncomingMessagesQueueLoop() {
    let locked = false;

    state.incomingMessagesQueueAddEvent.on('messageAdded', async () => {
        if (locked) return;
        locked = true;

        while (state.incomingMessagesQueue.length > 0) {
            const messageHandler = state.incomingMessagesQueue.shift();
            try {
                await messageHandler();
            } catch (err) {
                log(`Error processing queued message: ${err.message} \nStack: ${err.stack}`);
            }
        }

        locked = false;
    });
}

function initLfsProtocolHandler() {
    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout, // Git LFS expects JSON responses on stdout
        terminal: false
    });

    let operation = "";

    rl.on('line', async (line) => {
        debugLog(`Received from LFS: ${line}`);
        try {
            const request = JSON.parse(line);

            if (request.event === "init") {
                operation = request.operation;

                if (operation !== "download") {
                    sendToLfs({ error: { code: 1, message: `Turbolfs agent only supports 'download' operation.` } });
                    log("Error: Agent initialized for non-download operation. Exiting.");
                    await cleanupTempDirectory();
                    process.exit(1); // Hard exit as per original logic
                    return;
                }
                // Important: Only send init success AFTER WebSocket connection is established AND protocol version is confirmed.
                // For now, we'll send it, and if WebSocket isn't ready, downloads will queue.
                // A better approach would be to wait for `state.agentReadyForLFS`.
                if (state.agentReadyForLFS) {
                    sendToLfs({}); // Empty success object for init
                    log("Agent initialized for download operation and WebSocket ready.");
                    state.lfs.initSent = true;
                } else {
                    log("Agent initialized by LFS, but WebSocket not ready yet. Will send LFS init success upon WebSocket connection and protocol handshake.");
                    sendToLfs({});
                    log("Agent initialized for download operation (LFS confirmation sent). WebSocket connection pending/in progress.");
                    state.lfs.initSent = true; // Mark that LFS init response was sent
                }


            } else if (request.event === "download") {
                if (operation !== "download") {
                    sendToLfs({ event: "complete", oid: request.oid, error: { code: 99, message: "Agent not initialized for download." } });
                    return;
                }
                log(`Queueing download request from LFS for OID: ${request.oid}, Size: ${request.size}`);
                state.downloadQueue.push({ oid: request.oid, size: request.size });
                processDownloadQueue(); // Attempt to process immediately

            } else if (request.event === "terminate") {
                log("Terminate event received from Git LFS. Cleaning up and exiting.");
                if (state.wsClient && state.wsClient.readyState === WebSocket.OPEN) {
                    state.wsClient.close();
                }
                await cleanupTempDirectory();
                process.exit(0);
            } else {
                log(`Unknown event received from LFS: ${request.event}. Ignoring.`);
            }
        } catch (e) {
            log(`Error processing LFS input line: "${line}". Error: ${e.message} \nStack: ${e.stack}`);
        }
    });

    const sendToLfs = (data) => {
        const jsonResponse = JSON.stringify(data);
        process.stdout.write(jsonResponse + '\n');
        debugLog(`Sent to LFS: ${jsonResponse}`);
    };

    state.lfs = { sendToLfs, initSent: false }; // Store sendToLfs for later use
    return state.lfs;
}

module.exports = { main };
