import { app, EventGridEvent, InvocationContext } from "@azure/functions";
import { BlobClient, BlobServiceClient, StorageSharedKeyCredential } from "@azure/storage-blob";
import { fileTypeFromBuffer } from "file-type";
import { connect } from "amqplib";
import MP4Box from "mp4box";

const UPLOADED_FILES = "UPLOADED_FILES";

const maxFileSize = 1073741824;
const maxImageSize = 10485760;
const allowedImageMimeTypes = ["image/jpeg", "image/png", "image/gif", "image/bmp", "image/webp", "image/tiff", "image/svg+xml"];
const allowedVideoMimeTypes = ["video/mp4", "video/quicktime", "video/x-msvideo", "video/x-matroska", "video/webm"];
const allowedStoryVideoMimeTypes = ["video/mp4"];
const maxStoryVideoSize = 31457280;
const maxStoryDuration = 30;
let channelPromise = null;
let connectionPromise = null;

export async function chatterValidateUpload(event: EventGridEvent, context: InvocationContext): Promise<void> {
    if (!event.data) {
        return;
    }
    const blobUrl = event.data.url;
    if (typeof blobUrl !== "string") return;
    const connectionString = process.env.AZURE_STORAGE_CONNECTION_STRING;
    const containerName = process.env.AZURE_BLOB_CONTAINER_NAME;
    let entityType = "";

    const blobServiceClient = BlobServiceClient.fromConnectionString(connectionString);
    const containerClient = blobServiceClient.getContainerClient(containerName);

    const urlParts = blobUrl.split("/");
    const containerIndex = urlParts.indexOf(containerName);
    const blobPath = urlParts.slice(containerIndex + 1).join("/");
    const blobClient = containerClient.getBlobClient(blobPath);

    if (!(await blobClient.exists())) return;
    try {
        console.log(blobUrl);
        const buffer = await blobClient.downloadToBuffer(0, 4 * 1024);
        const contentLength = (await blobClient.getProperties()).contentLength;
        const [userId, category, entityId] = blobPath.split("/");
        const type = await fileTypeFromBuffer(buffer);
        
        if (type || !blobUrl.endsWith(type.ext)) {
            throw new Error("File type mismatch");
        }
        if (category === "stories") {
            await validateStory(contentLength, type.mime, blobClient);
            entityType = "STORY";
        }
        else if (category === "messages") {
            validateMessage(contentLength);
            entityType = "MESSAGE";
        }
        else if (category === "attachments") {
            validateAttachment(contentLength, type.mime);
            entityType = "ATTACHMENT";
        }
        else {
            throw new Error("Invalid category");
        }

        const channel = await getChannel();
        channel.sendToQueue(UPLOADED_FILES, Buffer.from(JSON.stringify({
            userId,
            entityId,
            filename: blobUrl,
            entityType,
        })));
    }
    catch (e) {
        await blobClient.delete();
        context.error(e);
    }

    context.log("Event grid function processed event:", event);
}

async function getConnection() {
    if (connectionPromise) {
        return connectionPromise;
    }
    connectionPromise = (async () => {
        try {
            const connection = await connect(process.env.AMQP_URL as string);
            connection.on("error", () => {
                connectionPromise = null;
            });

            connection.on("close", () => {
                connectionPromise = null;
            });
            return connection;
        } catch (error) {
            connectionPromise = null;
            throw error;
        }
    })();

    return connectionPromise;
};

async function getChannel() {
    if (channelPromise) return channelPromise;
    channelPromise = (async () => {
        const conn = await getConnection();
        const channel = await conn.createChannel();
        await channel.assertQueue(UPLOADED_FILES);

        channel.on("close", () => { channelPromise = null; });
        channel.on("error", () => { channelPromise = null; });

        return channel;
    })();
    return channelPromise;
}

async function validateStory(contentLength: number, contentType: string, blobClient: BlobClient) {
    if (allowedStoryVideoMimeTypes.includes(contentType)) {
        if (contentLength > maxStoryVideoSize) {
            throw new Error(`Story video size exceeds limit: ${maxStoryVideoSize} bytes`);
        }
        const duration = await getVideoDuration(blobClient, contentLength);
        if (duration > maxStoryDuration) {
            throw new Error(`Story video exceeds duration limit: ${maxStoryDuration}`);
        }
    }
    else if (allowedImageMimeTypes.includes(contentType)) {
        if (contentLength > maxImageSize) {
            throw new Error(`Story image size exceeds limit: ${maxStoryVideoSize} bytes`);
        }
    }
    else {
        throw new Error("Unsupported story file type");
    }
}

function validateMessage(contentLength: number) {
    if (contentLength > maxFileSize) {
        throw new Error(`Message file size exceeds limit: ${maxFileSize} bytes`);
    }
}

function validateAttachment(contentLength: number, contentType: string) {
    if (allowedImageMimeTypes.includes(contentType)) {
        if (contentLength > maxImageSize) {
            throw new Error(`Attachment image file exceeds limit: ${maxImageSize} bytes`);
        }
    }
    else if (allowedVideoMimeTypes.includes(contentType)) {
        if (contentLength > maxFileSize) {
            throw new Error(`Attachment video file exceeds limit: ${maxFileSize} bytes`);
        }
    }
    else {
        throw new Error("Unsupported attachment file type");
    }
}

async function getVideoDuration(blobClient: BlobClient, contentLength: number): Promise<number> {
    const startSize = Math.min(contentLength, 64 * 1024);
    let duration = await parseBuffer(blobClient, 0, startSize);
    if (!duration || isNaN(duration)) {
        const endSize = Math.min(contentLength, 128 * 1024);
        duration = await parseBuffer(blobClient, contentLength - endSize, endSize);
    }
    return duration || 0;
}

async function parseBuffer(blobClient: BlobClient, offset: number, length: number): Promise<number> {
    return new Promise(async (resolve) => {
        const mp4boxfile = MP4Box.createFile();
        const timeout = setTimeout(() => resolve(0), 1500);

        mp4boxfile.onReady = (info) => {
            clearTimeout(timeout);
            resolve(info.duration / info.timescale);
        };

        try {
            const downloadResponse = await blobClient.download(offset, length);
            const buffer = await streamToBuffer(downloadResponse.readableStreamBody);
            const arrayBuffer = buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength);
            // @ts-ignore
            arrayBuffer.fileStart = offset;
            // @ts-ignore
            mp4boxfile.appendBuffer(arrayBuffer);
            mp4boxfile.flush();
        } catch (e) {
            clearTimeout(timeout);
            resolve(0);
        }
    });
}

async function streamToBuffer(readableStream: NodeJS.ReadableStream): Promise<Buffer> {
    return new Promise((resolve, reject) => {
        const chunks: Buffer[] = [];
        readableStream.on("data", (data) => chunks.push(Buffer.isBuffer(data) ? data : Buffer.from(data)));
        readableStream.on("end", () => resolve(Buffer.concat(chunks)));
        readableStream.on("error", reject);
    });
}

app.eventGrid("chatterValidateUpload", {
    handler: chatterValidateUpload
});
