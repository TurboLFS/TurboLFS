const WebSocket = require('ws');
const fsPromises = require('fs/promises');
const fs = require('fs');
const path = require('path');
const { Readable } = require('stream');

let CACHE_SOURCES;
const SUPPORTED_PROTOCOL_VERSION_BY_SERVER = 1;

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
    ERROR_TOO_LARGE_FOR_PROTOCOL: 3,
};

function log(...args) {
    const timestamp = new Date().toISOString();
    console.error(`[${timestamp}] [Aggregator]`, ...args);
}

const secureResolvePath = (base, oid, structured) => {
    const unsafePath = structured
        ? path.join(base, oid.slice(0, 2), oid.slice(2, 4), oid)
        : path.join(base, oid);

    const normalized = path.normalize(unsafePath);
    const resolvedBase = path.resolve(base);

    if (!normalized.startsWith(resolvedBase + path.sep) && normalized !== resolvedBase) {
        if (path.dirname(normalized) !== path.dirname(resolvedBase) && !normalized.startsWith(resolvedBase + path.sep)) {
            throw new Error(`Path traversal attempt detected: OID ${oid} resolved to ${normalized} outside of base ${resolvedBase}`);
        } else if (!normalized.startsWith(resolvedBase)) {
            throw new Error(`Path traversal attempt detected: OID ${oid} resolved to ${normalized} outside of base ${resolvedBase}`);
        }
    }
    return normalized;
};
/**
 * Retrieves a Git LFS file's content as a Stream from a GitHub repository
 * using its OID (SHA256 hash) and size.
 *
 * @param {string} repoFullName - The full name of the repository (e.g., "organisation/repository").
 * @param {string} lfsOid - The SHA256 OID of the LFS object (the 64-character hex string).
 * @param {number} lfsSize - The size of the LFS object in bytes. This is a required parameter for the LFS batch API.
 * @param {string} token - A GitHub Personal Access Token (PAT) with 'repo' scope for private repos.
 * @returns {Promise<NodeJS.ReadableStream>} A Promise that resolves to a ReadableStream of the LFS file's content.
 * @throws {Error} If any step of the retrieval process fails or if the LFS object is not found.
 */
async function getLfsFileStreamByOid(repoFullName, lfsOid, lfsSize, token) {
    const [owner, repoName] = repoFullName.split('/');
    if (!owner || !repoName) {
        throw new Error('Invalid repository format. Expected "owner/repoName".');
    }

    // Validate LFS OID format (should be a 64-character hex string)
    if (!/^[a-f0-9]{64}$/i.test(lfsOid)) { // Case-insensitive match for hex
        throw new Error('Invalid LFS OID format. Expected a 64-character SHA256 hex string. ' +
            ` Received: ${lfsOid} (length: ${lfsOid.length})`);
    }
    // Validate LFS size
    if (typeof lfsSize !== 'number' || lfsSize < 0 || !Number.isInteger(lfsSize)) {
        throw new Error('Invalid LFS size. Expected a non-negative integer.');
    }

    // --- Step 1: Call LFS Batch API to get download information ---
    const lfsBatchUrl = `https://github.com/${owner}/${repoName}.git/info/lfs/objects/batch`;
    const lfsPayload = {
        operation: 'download',
        transfer: ['basic'],
        objects: [
            { oid: lfsOid, size: lfsSize },
        ],
    };

    const lfsApiHeaders = {
        'Authorization': `token ${token}`, // LFS API uses the PAT for private repos
        'Accept': 'application/vnd.git-lfs+json',
        'Content-Type': 'application/json',
    };

    if (token === "PUBLIC") {
        delete lfsApiHeaders.Authorization; // If token is "PUBLIC", do not send Authorization header
    }

    let response = await fetch(lfsBatchUrl, {
        method: 'POST',
        headers: lfsApiHeaders,
        body: JSON.stringify(lfsPayload),
    });

    if (!response.ok) {
        const errorBody = await response.text();
        // Specific check for common "object does not exist" scenarios, often a 404 or 422 from LFS.
        if ((response.status === 404 || response.status === 422) &&
            (errorBody.includes("Object does not exist") || errorBody.toLowerCase().includes("not found"))) {
            throw new Error(`Git LFS object with OID ${lfsOid} and size ${lfsSize} not found in repository ${repoFullName} or access denied. LFS Batch API response: ${response.status} ${errorBody}`);
        }
        throw new Error(`Git LFS Batch API error for OID ${lfsOid} (repo: ${repoFullName}): ${response.status} ${errorBody}`);
    }
    const lfsBatchResponse = await response.json();

    // --- Step 2: Extract download URL and download the LFS file ---
    const objectResponseData = lfsBatchResponse.objects?.[0];
    if (!objectResponseData) {
        throw new Error(`No object data returned in LFS Batch API response for OID ${lfsOid}. Response: ${JSON.stringify(lfsBatchResponse)}`);
    }

    // Check for errors specific to this object within the batch response
    if (objectResponseData.error) {
        throw new Error(`LFS object error for OID ${lfsOid} (size ${lfsSize}) in repo ${repoFullName}: Code ${objectResponseData.error.code}, Message: ${objectResponseData.error.message}`);
    }

    const downloadAction = objectResponseData.actions?.download;
    if (!downloadAction || !downloadAction.href) {
        throw new Error(`LFS download URL ('href') not found in LFS Batch API response for OID ${lfsOid}. Response: ${JSON.stringify(lfsBatchResponse)}`);
    }
    const lfsDownloadUrl = downloadAction.href;

    // This request goes to the actual storage (e.g., S3) and typically does not need the GitHub token
    response = await fetch(lfsDownloadUrl);

    if (!response.ok) {
        throw new Error(`Error downloading LFS file (OID ${lfsOid}) from storage (${lfsDownloadUrl}): ${response.status} ${await response.text()}`);
    }

    // --- Step 3: Return the stream ---
    return response.body; // This is a NodeJS.ReadableStream from node-fetch
}

async function fetchStreamAndSizeFromSources(filenameOid, sizeBigint) {
    for (const source of CACHE_SOURCES) {
        const structured = source.structuredPath;
        const baseUrl = source.url;

        try {
            if (baseUrl.startsWith('file://')) {
                const basePath = baseUrl.slice(7);
                const localPath = secureResolvePath(basePath, filenameOid, structured);

                log(`Attempting to access local file: ${localPath} for OID ${filenameOid}`);
                try {
                    await fsPromises.access(localPath, fs.constants.R_OK);
                } catch (e) {
                    log(`Local file not found or not accessible for OID ${filenameOid} at ${localPath}: ${e.message}`);
                    continue;
                }

                const stats = await fsPromises.stat(localPath);
                if (!stats.isFile()) {
                    log(`Local path is not a file for OID ${filenameOid}: ${localPath}`);
                    continue;
                }
                const stream = fs.createReadStream(localPath);
                log(`Successfully opened local file stream for OID ${filenameOid}: ${localPath}, Size: ${stats.size}`);
                return { stream, size: BigInt(stats.size), filename: filenameOid, sourceType: 'file' };

            } else if (baseUrl.startsWith('https://') || baseUrl.startsWith('http://')) {

                log(`!!! Attempting to fetch LFS object ${filenameOid} from HTTP source: ${baseUrl}`);
                const sizeNumber = Number(sizeBigint);
                const readableStream = await getLfsFileStreamByOid(source.objectsRepoArg, filenameOid, sizeNumber, source.authorization);
                return {
                    stream: readableStream,
                    size: BigInt(sizeNumber),
                    filename: filenameOid,
                    sourceType: 'http'
                };
            }
        } catch (err) {
            log(`Source failed for OID ${filenameOid} (source URL: ${baseUrl}): ${err.message} ${err.stack}`);
            continue;
        }
    }
    log(`OID ${filenameOid} not found in any source or all sources failed.`);
    return null;
}

async function sendErrorResponse(ws, oidBin, statusCode, errorMessageStr) {
    const errorMsgBuf = Buffer.from(errorMessageStr, 'utf-8');
    const response = Buffer.alloc(1 + 32 + 8 + 1 + 4 + errorMsgBuf.length);
    let offset = 0;
    response.writeUInt8(MessageType.OBJECT_DOWNLOAD_START_RESPONSE, offset); offset += 1;
    oidBin.copy(response, offset); offset += 32;
    response.writeBigUInt64BE(BigInt(0), offset); offset += 8; // Size 0 for error
    response.writeUInt8(statusCode, offset); offset += 1;
    response.writeUInt32BE(errorMsgBuf.length, offset); offset += 4;
    errorMsgBuf.copy(response, offset);

    if (ws.readyState === WebSocket.OPEN) {
        ws.send(response);
    }
    log(`Sent error response for OID ${oidBin.toString('hex')}: ${errorMessageStr}`);
}

async function sendTransferErrorNotification(ws, oidBin, errorMessageStr) {
    const errorMsgBuf = Buffer.from(errorMessageStr, 'utf-8');
    const notification = Buffer.alloc(1 + 32 + 4 + errorMsgBuf.length);
    let offset = 0;
    notification.writeUInt8(MessageType.OBJECT_TRANSFER_ERROR_NOTIFICATION, offset); offset += 1;
    oidBin.copy(notification, offset); offset += 32;
    notification.writeUInt32BE(errorMsgBuf.length, offset); offset += 4;
    errorMsgBuf.copy(notification, offset);

    if (ws.readyState === WebSocket.OPEN) {
        ws.send(notification);
    }
    log(`Sent transfer error notification for OID ${oidBin.toString('hex')}: ${errorMessageStr}`);
}


async function handleClientConnection(ws, req) {
    const clientAddress = req.socket.remoteAddress + ":" + req.socket.remotePort;
    log(`Client connected: ${clientAddress}`);

    // 1. Send Server Protocol Info
    const protocolInfoMsg = Buffer.alloc(1 + 4);
    protocolInfoMsg.writeUInt8(MessageType.SERVER_PROTOCOL_INFO, 0);
    protocolInfoMsg.writeUInt32BE(SUPPORTED_PROTOCOL_VERSION_BY_SERVER, 1);
    ws.send(protocolInfoMsg);
    log(`Sent SERVER_PROTOCOL_INFO (version ${SUPPORTED_PROTOCOL_VERSION_BY_SERVER}) to ${clientAddress}`);

    ws.on('message', async (message) => {
        const incomingBuffer = Buffer.isBuffer(message) ? message : Buffer.from(message);

        if (incomingBuffer.length === 0) {
            log(`Received empty message from ${clientAddress}, ignoring.`);
            return;
        }
        const messageType = incomingBuffer.readUInt8(0);
        const payload = incomingBuffer.slice(1);

        if (messageType === MessageType.OBJECT_DOWNLOAD_REQUEST) {
            if (payload.length !== 40) {
                log(`Error: OBJECT_DOWNLOAD_REQUEST from ${clientAddress} has invalid payload length ${payload.length}. Closing connection.`);
                sendErrorResponse(ws, Buffer.alloc(32), StatusCode.ERROR_GENERAL, "Invalid payload length in request.");
                ws.terminate();
                return;
            }
            const oidBin = payload.slice(0, 32);
            const oidHex = oidBin.toString('hex');
            const sizeBigint = payload.readBigUInt64BE(32);
            log(`Received OBJECT_DOWNLOAD_REQUEST for OID: ${oidHex}, size: ${sizeBigint} from ${clientAddress}`);

            const streamDetails = await fetchStreamAndSizeFromSources(oidHex, sizeBigint);

            if (!streamDetails || !streamDetails.stream || streamDetails.size === undefined) {
                await sendErrorResponse(ws, oidBin, StatusCode.ERROR_NOT_FOUND, `OID ${oidHex} not found or could not be accessed.`);
                return;
            }

            // Send OBJECT_DOWNLOAD_START_RESPONSE (Success)
            const startResponse = Buffer.alloc(1 + 32 + 8 + 1);
            let offset = 0;
            startResponse.writeUInt8(MessageType.OBJECT_DOWNLOAD_START_RESPONSE, offset); offset += 1;
            oidBin.copy(startResponse, offset); offset += 32;
            startResponse.writeBigUInt64BE(streamDetails.size, offset); offset += 8;
            startResponse.writeUInt8(StatusCode.OK, offset); offset += 1;

            if (ws.readyState === WebSocket.OPEN) ws.send(startResponse);
            log(`Sent OBJECT_DOWNLOAD_START_RESPONSE for OID: ${oidHex}, Size: ${streamDetails.size} to ${clientAddress}`);

            if (streamDetails.size === BigInt(0)) {
                log(`OID ${oidHex} is an empty file. No data chunks to send.`);
                return;
            }

            const sourceStream = streamDetails.stream;
            // Ensure it's a Node.js Readable stream
            const nodeStream = (typeof sourceStream.pipe === 'function' && typeof sourceStream.on === 'function')
                ? sourceStream
                : Readable.fromWeb(sourceStream); // Convert WHATWG ReadableStream (from fetch) to Node.js Readable

            let totalBytesStreamed = BigInt(0);
            try {
                for await (const chunk of nodeStream) {
                    if (ws.readyState !== WebSocket.OPEN) {
                        log(`WebSocket connection for ${clientAddress} closed while streaming OID ${oidHex}. Aborting.`);
                        if (typeof nodeStream.destroy === 'function') nodeStream.destroy();
                        break;
                    }

                    const dataChunkMessage = Buffer.alloc(1 + 32 + chunk.length);
                    dataChunkMessage.writeUInt8(MessageType.OBJECT_DATA_CHUNK, 0);
                    oidBin.copy(dataChunkMessage, 1);
                    chunk.copy(dataChunkMessage, 1 + 32);
                    if (ws.readyState === WebSocket.OPEN) {
                        ws.send(dataChunkMessage);
                        totalBytesStreamed += BigInt(chunk.length);
                    }
                    else {
                        log(`WebSocket not open during chunk send for OID ${oidHex}. Aborting stream.`);
                        if (typeof nodeStream.destroy === 'function') nodeStream.destroy();
                        return;
                    }
                }

                if (totalBytesStreamed !== streamDetails.size) {
                    log(`Warning: Streamed ${totalBytesStreamed} bytes for OID ${oidHex} but expected ${streamDetails.size}. Client might detect error.`);
                    if (ws.readyState === WebSocket.OPEN) {
                        await sendTransferErrorNotification(ws, oidBin, `Stream ended prematurely. Sent ${totalBytesStreamed}/${streamDetails.size} bytes.`);
                    }
                } else {
                    log(`Finished streaming OID ${oidHex} (${totalBytesStreamed} bytes) to ${clientAddress}`);
                }

            } catch (streamErr) {
                log(`Error during data stream for OID ${oidHex} to ${clientAddress}: ${streamErr.message}`);
                if (ws.readyState === WebSocket.OPEN) {
                    await sendTransferErrorNotification(ws, oidBin, `Server stream error: ${streamErr.message}`);
                }
            } finally {
                if (typeof nodeStream.destroy === 'function') {
                    nodeStream.destroy();
                }
                log(`Cleaned up stream for OID ${oidHex}`);
            }

        } else {
            log(`Unknown message type 0x${messageType.toString(16)} received from ${clientAddress}. Length: ${incomingBuffer.length}. Closing connection.`);
            ws.terminate();
        }
    });

    ws.on('error', (err) => {
        log(`WebSocket error for client ${clientAddress}: ${err.message}`);
        // TODO: Consider ensuring socket is cleaned up
        // ws.terminate();
    });

    ws.on('close', (code, reason) => {
        const reasonStr = reason ? reason.toString() : 'No reason provided';
        log(`Client disconnected: ${clientAddress}. Code: ${code}, Reason: ${reasonStr}`);
    });
}


function mainServer(portArg, objectsDirArg, objectsRepoArg, objectRepoTokenArg) {
    CACHE_SOURCES = [];

    if (objectsDirArg) {
        const resolvedObjectsDir = path.resolve(objectsDirArg);
        log(`Adding local LFS cache source: file://${resolvedObjectsDir}/ (structured)`);
        CACHE_SOURCES.push({
            url: `file://${resolvedObjectsDir}/`,
            structuredPath: true
        });
    }

    if (objectsRepoArg) {
        let lfsBaseUrl = objectsRepoArg;
        if (!objectsRepoArg.startsWith('http://') && !objectsRepoArg.startsWith('https://')) {
            const [org, repo] = objectsRepoArg.split('/');
            if (!org || !repo) {
                console.error('Invalid objects-repo format. Expected ORG/REPO or a full URL.');
                return false;
            }
            lfsBaseUrl = `https://api.github.com/repos/${org}/${repo}/lfs/objects/`;
            log(`Constructed GitHub LFS Batch API URL pattern: ${lfsBaseUrl}{oid} from org/repo input.`);
        } else {
            log(`Using provided LFS URL: ${lfsBaseUrl}`);
        }

        if (!objectRepoTokenArg && lfsBaseUrl.includes('github.com')) {
            log(`Warning: object-repo-token not provided for GitHub LFS source ${lfsBaseUrl}. Access may fail for private repositories.`);
        }

        CACHE_SOURCES.push({
            objectsRepoArg: objectsRepoArg,
            url: lfsBaseUrl,
            structuredPath: false,
            authorization: objectRepoTokenArg ? `${objectRepoTokenArg}` : undefined
        });
        log(`Adding HTTP LFS cache source: ${lfsBaseUrl} (structured: false, auth: ${!!objectRepoTokenArg})`);
    }


    if (CACHE_SOURCES.length === 0) {
        console.error('No cache sources configured. Please provide --objects-dir and/or --objects-repo.');
        return false;
    }

    const wss = new WebSocket.Server({ port: parseInt(portArg) });

    wss.on('connection', (ws, req) => {
        handleClientConnection(ws, req).catch(err => {
            const clientAddressInfo = req ? `${req.socket.remoteAddress}:${req.socket.remotePort}` : 'unknown client';
            log(`Error in handleClientConnection for ${clientAddressInfo}: ${err.message} \nStack: ${err.stack}`);
            if (ws.readyState === WebSocket.OPEN) {
                ws.terminate();
            }
        });
    });

    wss.on('error', (err) => { // Errors on the server itself (e.g., EADDRINUSE)
        log(`WebSocket Server error: ${err.message}`);
        if (err.code === 'EADDRINUSE') {
            log(`Port ${portArg} is already in use. Cannot start server.`);
        }
        process.exit(1); // Critical server error
    });

    log(`WebSocket Aggregator server running on port ${portArg}`);
    log(`Configured CACHE_SOURCES:`);
    CACHE_SOURCES.forEach(source => log(`- URL: ${source.url}, Structured: ${source.structuredPath}, Auth: ${!!source.authorization}`));
    log("---");
    log("Awaiting connections from TurboLFS agents...");

    return true;
}

module.exports = { main: mainServer };
