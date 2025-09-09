const WebSocket = require('ws');
const fsPromises = require('fs/promises');
const fs = require('fs');
const path = require('path');
const { Readable, PassThrough } = require('stream');
const { S3Client, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');
const { Upload } = require('@aws-sdk/lib-storage');
const limit = require('p-limit').default;

let CACHE_SOURCES;
const SUPPORTED_PROTOCOL_VERSION_BY_SERVER = 1;

let s3global = {
    queue: [],
    maxConcurrent: 20,
    oidHexBannedFromPromote: new Set()
};

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

const unsafeResolvePath = (base, oid, structured) => {
    const unsafePath = structured
        ? path.join(base, oid.slice(0, 2), oid.slice(2, 4), oid)
        : path.join(base, oid);

    const normalized = path.normalize(unsafePath);
    return normalized;
};

const secureResolvePath = (base, oid, structured) => {
    const normalized = unsafeResolvePath(base, oid, structured);
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

// Temporary file naming helper to ensure uniqueness across processes and functions
function makeTempCachePath(finalCachePath, func) {
    return `${finalCachePath}.${process.pid}.${func.name}.tmp`;
}

/**
 * Lists objects from an S3 bucket, sorts them by size, and pre-downloads the smallest
 * files up to a specified total size limit into a local filesystem cache.
 *
 * @param {object} s3Source - The S3 source configuration object from CACHE_SOURCES.
 * @param {object} fsCacheSource - The filesystem cache destination object from CACHE_SOURCES.
 * @param {number} [maxTotalSizeBytes=1073741824] - The maximum total size of files to download (defaults to 1 GB).
 * @param {number} [concurrency=10] - The number of files to download in parallel.
 */
async function preDownloadSmallFilesFromS3(s3Source, fsCacheSource, maxTotalSizeBytes = Math.floor(1.5 * 1024 * 1024 * 1024), concurrency = 10) {
    if (!s3Source || !s3Source.s3 || !s3Source.s3.client) {
        log('[Pre-Download] Invalid S3 source configuration provided.');
        return;
    }
    if (!fsCacheSource || !fsCacheSource.url.startsWith('file://')) {
        log('[Pre-Download] Invalid or non-filesystem cache destination provided.');
        return;
    }

    const s3Client = s3Source.s3.client;
    const bucketName = s3Source.s3.bucket;
    const s3Prefix = s3Source.s3.prefix || '';
    const localBasePath = fsCacheSource.url.slice(7);
    const useStructuredPath = fsCacheSource.structuredPath;

    log(`[Pre-Download] Starting pre-download from S3 bucket '${bucketName}' to '${localBasePath}'.`);
    log(`[Pre-Download] Max cache size: ${(maxTotalSizeBytes / (1024 * 1024)).toFixed(2)} MB, Concurrency: ${concurrency}.`);

    // Step 1: List all objects from S3, handling pagination
    let allObjects = [];
    try {
        let isTruncated = true;
        let continuationToken = undefined;
        log(`[Pre-Download] Listing objects from s3://${bucketName}/${s3Prefix}...`);

        while (isTruncated) {
            const command = new ListObjectsV2Command({
                Bucket: bucketName,
                Prefix: s3Prefix,
                ContinuationToken: continuationToken,
            });
            const response = await s3Client.send(command);
            if (response.Contents) {
                // Filter out any "directory" objects by ensuring they have a size > 0
                const files = response.Contents.filter(obj => obj.Size > 0);
                allObjects.push(...files);
            }
            isTruncated = response.IsTruncated;
            continuationToken = response.NextContinuationToken;
        }
        log(`[Pre-Download] Found ${allObjects.length} total objects in S3.`);
    } catch (err) {
        log(`[Pre-Download] ERROR: Failed to list objects from S3: ${err.message}`);
        return; // Cannot proceed
    }

    // Step 2: Sort objects by size (smallest first)
    allObjects.sort((a, b) => a.Size - b.Size);

    // Step 3: Select objects to download up to the max size limit
    const objectsToDownload = [];
    let currentTotalSize = 0;
    for (const obj of allObjects) {
        if (currentTotalSize + obj.Size <= maxTotalSizeBytes) {
            objectsToDownload.push(obj);
            currentTotalSize += obj.Size;
        } else {
            break; // Stop once the next file would exceed the limit
        }
    }

    if (objectsToDownload.length === 0) {
        log('[Pre-Download] No objects to download.');
        return;
    }

    log(`[Pre-Download] Selected ${objectsToDownload.length} smallest objects, totaling ${(currentTotalSize / (1024 * 1024)).toFixed(2)} MB, for download.`);

    // Step 4: Download selected objects concurrently
    const concurrencyLimiter = limit(concurrency);
    let downloadedCount = 0;
    let failedCount = 0;
    let skippedCount = 0;

    const downloadPromises = objectsToDownload.map(s3Object => {
        return concurrencyLimiter(async () => {
            // Assuming the OID is the file name (basename of the S3 key)
            const oidHex = path.basename(s3Object.Key);

            let finalCachePath;
            try {
                finalCachePath = secureResolvePath(localBasePath, oidHex, useStructuredPath);
            } catch (pathErr) {
                log(`[Pre-Download] ERROR: Skipping OID ${oidHex} due to invalid path: ${pathErr.message}`);
                failedCount++;
                return;
            }

            // Check if file already exists in the cache
            try {
                await fsPromises.access(finalCachePath, fs.constants.F_OK);
                skippedCount++;
                return; // Already cached
            } catch (e) {
                // File doesn't exist, so proceed to download
            }

            // Use an atomic write: download to a temp file, then rename
            const tempCachePath = makeTempCachePath(finalCachePath, preDownloadSmallFilesFromS3);
            let fileWriteStream;

            try {
                await fsPromises.mkdir(path.dirname(finalCachePath), { recursive: true });

                const getObjectCommand = new GetObjectCommand({ Bucket: bucketName, Key: s3Object.Key });
                const response = await s3Client.send(getObjectCommand);

                fileWriteStream = fs.createWriteStream(tempCachePath);

                // Pipe the S3 object stream to the local file stream
                await new Promise((resolve, reject) => {
                    response.Body.on('error', reject); // S3 read error
                    fileWriteStream.on('error', reject); // Filesystem write error
                    fileWriteStream.on('finish', resolve); // Write success
                    response.Body.pipe(fileWriteStream);
                });

                // Atomically move the temp file to its final destination
                await fsPromises.rename(tempCachePath, finalCachePath);
                downloadedCount++;
                s3global.oidHexBannedFromPromote.add(oidHex); // Prevent immediate re-promotion
                if (downloadedCount > 0 && downloadedCount % 100 === 0) {
                    log(`[Pre-Download] Progress: Downloaded ${downloadedCount} / ${objectsToDownload.length - skippedCount} files.`);
                }
            } catch (downloadErr) {
                log(`[Pre-Download] ERROR: Failed to download OID ${oidHex}: ${downloadErr.message}`);
                failedCount++;
                // Cleanup the failed temporary file
                if (fileWriteStream) fileWriteStream.destroy();
                try {
                    await fsPromises.unlink(tempCachePath);
                } catch (cleanupErr) {
                    if (cleanupErr.code !== 'ENOENT') {
                        log(`[Pre-Download] ERROR: Failed to clean up temp file ${tempCachePath}: ${cleanupErr.message}`);
                    }
                }
            }
        });
    });

    await Promise.all(downloadPromises);

    log('[Pre-Download] Finished.');
    log(`[Pre-Download] Summary: Successfully downloaded ${downloadedCount} new files. Failed: ${failedCount}. Skipped (already cached): ${skippedCount}.`);
}

async function setupFsCachePromotion(streamDetails, oidHex) {
    let fileWriteStream = null;
    let tempCachePath = null;
    let finalCachePath = null;
    let isCachingAttempted = false;

    // Condition: We are streaming from HTTP and a local file cache exists.
    const localCacheSource = CACHE_SOURCES.find(s => s.url.startsWith('file://'));
    if (streamDetails.sourceType === 'http' && localCacheSource) {
        isCachingAttempted = true;
        const localBasePath = localCacheSource.url.slice(7);
        try {
            finalCachePath = secureResolvePath(localBasePath, oidHex, localCacheSource.structuredPath);
            // Use a temporary file to ensure atomic write. Suffix with PID for multi-process safety.
            tempCachePath = makeTempCachePath(finalCachePath, setupFsCachePromotion);

            // Ensure the target directory exists (e.g., /path/to/cache/aa/bb/)
            await fsPromises.mkdir(path.dirname(finalCachePath), { recursive: true });

            fileWriteStream = fs.createWriteStream(tempCachePath);
            log(`[Cache] Staging OID ${oidHex} to temporary file: ${tempCachePath}`);

            // Handle write errors gracefully without stopping the client stream
            fileWriteStream.on('error', (err) => {
                log(`[Cache] ERROR writing OID ${oidHex} to ${tempCachePath}: ${err.message}. Aborting cache write, but continuing client stream.`);
                if (fileWriteStream) {
                    fileWriteStream.destroy();
                    fileWriteStream = null; // Prevent further writes
                    // Clean up the failed temporary file
                    fs.unlink(tempCachePath, (unlinkErr) => {
                        if (unlinkErr) log(`[Cache] ERROR cleaning up failed temp file ${tempCachePath}: ${unlinkErr.message}`);
                    });
                }
            });

        } catch (cacheSetupErr) {
            log(`[Cache] ERROR setting up cache for OID ${oidHex}: ${cacheSetupErr.message}. Client stream will continue without caching.`);
            fileWriteStream = null; // Ensure we don't try to use it
        }
    }

    return {
        fileWriteStream,
        tempCachePath,
        finalCachePath,
        isCachingAttempted
    };
}

async function promoteFsCacheIfNeeded(fsCacheState, chunk) {
    if (fsCacheState.fileWriteStream) {
        if (!fsCacheState.fileWriteStream.write(chunk)) {
            // Handle backpressure from the filesystem if necessary
            await new Promise(resolve => fsCacheState.fileWriteStream.once('drain', resolve));
        }
    }
}

async function commitFsCacheIfNeeded(fsCacheState, oidHex) {
    if (fsCacheState.fileWriteStream) {
        await new Promise((resolve, reject) => {
            fsCacheState.fileWriteStream.on('finish', resolve);
            fsCacheState.fileWriteStream.on('error', reject); // Catch any final errors
            fsCacheState.fileWriteStream.end();
        });
        await fsPromises.rename(fsCacheState.tempCachePath, fsCacheState.finalCachePath);
        log(`[Cache] Successfully promoted OID ${oidHex} to local cache: ${fsCacheState.finalCachePath}`);
        fsCacheState.tempCachePath = null; // Prevent cleanup from deleting the committed file
    }
}

async function performPostFsCachePromotionCleanupIfNeeded(fsCacheState) {
    if (fsCacheState.fileWriteStream) {
        fsCacheState.fileWriteStream.destroy();
    }

    if (fsCacheState.tempCachePath) { // If it's not null, the rename hasn't happened
        try {
            await fsPromises.unlink(fsCacheState.tempCachePath);
            log(`[Cache] Cleaned up temporary file due to incomplete transfer: ${fsCacheState.tempCachePath}`);
        } catch (cleanupErr) {
            if (cleanupErr.code !== 'ENOENT') { // It's okay if the file is already gone
                log(`[Cache] ERROR during cleanup of temp file ${fsCacheState.tempCachePath}: ${cleanupErr.message}`);
            }
        }
    }
}

async function setupS3CachePromotion(streamDetails, oidHex) {
    let s3WriteStream = null;
    let s3UploadPromise = null;
    let isS3CachingAttempted = false;

    // Condition: We are NOT streaming from S3 and an S3 cache destination exists.
    const s3CacheSource = CACHE_SOURCES.find(s => s.s3);
    if (streamDetails.sourceType !== 's3' && s3CacheSource) {
        isS3CachingAttempted = true;
        log(`[Cache] OID ${oidHex} is eligible for S3 cache promotion.`);

        try {
            const s3Client = new S3Client({
                region: s3CacheSource.s3.region,
                endpoint: s3CacheSource.url,
                credentials: {
                    accessKeyId: s3CacheSource.s3.accessKeyId,
                    secretAccessKey: s3CacheSource.s3.secretAccessKey,
                },
                forcePathStyle: !!s3CacheSource.url,
            });

            // The key for the object in the S3 bucket
            const s3Key = unsafeResolvePath(s3CacheSource.s3.prefix || '', oidHex, s3CacheSource.structuredPath);

            // Create a PassThrough stream to pipe the data into the S3 upload
            s3WriteStream = new PassThrough();

            const upload = new Upload({
                client: s3Client,
                params: {
                    Bucket: s3CacheSource.s3.bucket,
                    Key: s3Key,
                    Body: s3WriteStream,
                },
            });

            log(`[Cache] Staging S3 upload for OID ${oidHex} to s3://${s3CacheSource.s3.bucket}/${s3Key}`);
            s3UploadPromise = upload.done();

            // Handle errors on the upload promise for early detection and logging
            s3UploadPromise.catch(err => {
                log(`[Cache] ERROR during S3 upload for OID ${oidHex}: ${err.message}. Aborting S3 cache write.`);
                if (s3WriteStream && !s3WriteStream.destroyed) {
                    s3WriteStream.destroy(err);
                }
                s3WriteStream = null; // Prevent further writes
            });

        } catch (cacheSetupErr) {
            log(`[Cache] ERROR setting up S3 cache for OID ${oidHex}: ${cacheSetupErr.message}. Client stream will continue without S3 caching.`);
            s3WriteStream = null; // Ensure we don't try to use it
            s3UploadPromise = null;
        }
    }

    return {
        s3WriteStream,
        s3UploadPromise,
        isS3CachingAttempted
    };
}

async function promoteS3CacheIfNeeded(s3CacheState, chunk) {
    if (s3CacheState.s3WriteStream && !s3CacheState.s3WriteStream.destroyed) {
        if (!s3CacheState.s3WriteStream.write(chunk)) {
            // Handle backpressure from the S3 upload stream
            await new Promise(resolve => s3CacheState.s3WriteStream.once('drain', resolve));
        }
    }
}

async function commitS3CacheIfNeeded(s3CacheState, oidHex) {
    if (s3CacheState.s3UploadPromise) {
        if (s3CacheState.s3WriteStream && !s3CacheState.s3WriteStream.destroyed) {
            s3CacheState.s3WriteStream.end(); // Signal the end of the stream
        }

        // Wrap the upload promise with logging
        const uploadPromise = s3CacheState.s3UploadPromise
            .then(() => {
                log(`[Cache] Successfully promoted OID ${oidHex} to S3 cache.`);
            })
            .catch(err => {
                log(
                    `[Cache] ERROR during S3 upload finalization for OID ${oidHex}: ${err.message}`
                );
            })
            .finally(() => {
                // Remove it from the queue when done
                const idx = s3global.queue.indexOf(uploadPromise);
                if (idx !== -1) {
                    s3global.queue.splice(idx, 1);
                }
            });

        // Add to queue
        s3global.queue.push(uploadPromise);

        // If too many uploads are in-flight, wait for at least one to finish
        if (s3global.queue.length >= s3global.maxConcurrent) {
            console.log(`[Cache] Reached max concurrent S3 uploads (${s3global.maxConcurrent}). Waiting for one to complete...`);
            await Promise.race(s3global.queue);
        }

        s3CacheState.s3UploadPromise = null;
    }
}

async function performPostS3CachePromotionCleanupIfNeeded(_s3CacheState) {
    // No-op, S3 impl doesn't require any cleanup at this moment
}

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
            } else if (source.s3) {
                log(`Attempting to fetch LFS object ${filenameOid} from S3 source: ${source.s3.bucket}`);

                const s3Client = source.s3.client;

                const s3Key = unsafeResolvePath(source.s3.prefix || '', filenameOid, structured);

                const getObjectParams = {
                    Bucket: source.s3.bucket,
                    Key: s3Key,
                };

                try {
                    const response = await s3Client.send(new GetObjectCommand(getObjectParams));
                    log(`Successfully fetched S3 object for OID ${filenameOid}. Key: ${s3Key}, Size: ${response.ContentLength}`);

                    return {
                        stream: response.Body,
                        size: BigInt(response.ContentLength),
                        filename: filenameOid,
                        sourceType: 's3'
                    };
                } catch (s3Error) {
                    if (s3Error.name === 'NoSuchKey') {
                        log(`S3 object not found for OID ${filenameOid} at key: ${s3Key}`);
                    } else {
                        throw s3Error;
                    }
                    continue;
                }
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

            //let fsCacheState = await setupFsCachePromotion(streamDetails, oidHex);
            let s3CacheState = await setupS3CachePromotion(streamDetails, oidHex);

            try {
                for await (const chunk of nodeStream) {
                    // Promote if needed
                    //await promoteFsCacheIfNeeded(fsCacheState, chunk);
                    if (!s3global.oidHexBannedFromPromote.has(oidHex)) {
                        await promoteS3CacheIfNeeded(s3CacheState, chunk);
                    } else {
                        log(`[Cache] Skipping S3 promotion for OID ${oidHex} as it is recently downloaded.`);
                    }

                    // Send chunk to client
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
                    } else {
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

                    try {
                        await Promise.all([
                            //commitFsCacheIfNeeded(fsCacheState, oidHex),
                            commitS3CacheIfNeeded(s3CacheState, oidHex),
                        ]);
                    } catch (cacheErr) {
                        log(`[Cache] ERROR during final cache promotion for OID ${oidHex}: ${cacheErr.message}`);
                    }
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

                //await performPostFsCachePromotionCleanupIfNeeded(fsCacheState);
                await performPostS3CachePromotionCleanupIfNeeded(s3CacheState);

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


function mainServer(
    portArg,
    objectsDirArg,
    objectsRepoArg,
    objectRepoTokenArg,
    s3EndpointArg,
    s3AccessKeyArg,
    s3SecretKeyArg,
    s3BucketArg,
    s3RegionArg
) {
    // CACHE_SOURCES order is important - earlier sources have higher priority for reads.
    CACHE_SOURCES = [];

    if (objectsDirArg) {
        const resolvedObjectsDir = path.resolve(objectsDirArg);
        log(`Adding local LFS cache source: file://${resolvedObjectsDir}/ (structured)`);
        CACHE_SOURCES.push({
            url: `file://${resolvedObjectsDir}/`,
            structuredPath: true,
            authorization: undefined,
            s3: undefined
        });
    }

    if (s3EndpointArg) {
        CACHE_SOURCES.push({
            url: s3EndpointArg,
            structuredPath: false,
            authorization: undefined,
            s3: {
                accessKeyId: s3AccessKeyArg,
                secretAccessKey: s3SecretKeyArg,
                bucket: s3BucketArg,
                region: s3RegionArg,
            }
        });

        const source = CACHE_SOURCES[CACHE_SOURCES.length - 1];
        const client = new S3Client({
            region: source.s3.region, // A region may be required depending on S3 provider
            endpoint: source.url, // Use the top-level URL from config as the endpoint
            credentials: {
                accessKeyId: source.s3.accessKeyId,
                secretAccessKey: source.s3.secretAccessKey,
            },
            forcePathStyle: !!source.url, // Required for most S3-compatible services
        });
        source.s3.client = client; // Attach the client for later use

        log(`Adding S3 cache source: ${s3EndpointArg} (structured: false, auth: ${!!(s3AccessKeyArg && s3SecretKeyArg)})`);
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
            authorization: objectRepoTokenArg ? `${objectRepoTokenArg}` : undefined,
            s3: undefined
        });
        log(`Adding HTTP LFS cache source: ${lfsBaseUrl} (structured: false, auth: ${!!objectRepoTokenArg})`);
    }

    if (CACHE_SOURCES.length === 0) {
        console.error('No cache sources configured. Please provide --objects-dir and/or --objects-repo.');
        return false;
    }

    const s3DataSource = CACHE_SOURCES.find(s => s.s3);
    const fsCacheDestination = CACHE_SOURCES.find(s => s.url.startsWith('file://'));

    if (s3DataSource && fsCacheDestination) {
        log("Initiating pre-download of small files from S3 to warm up the cache...");
        // Run in the background; don't block server startup.
        // Errors will be logged internally by the function.
        preDownloadSmallFilesFromS3(s3DataSource, fsCacheDestination)
            .catch(err => log(`[Pre-Download] A critical unexpected error occurred: ${err.message}`));
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
