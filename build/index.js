"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.WsClientAdapter = void 0;
const ws_1 = __importDefault(require("ws"));
const nengi_buffers_1 = require("nengi-buffers");
function toBuffer(data) {
    if (Buffer.isBuffer(data)) {
        return data;
    }
    if (Array.isArray(data)) {
        return Buffer.concat(data);
    }
    if (data instanceof ArrayBuffer) {
        return Buffer.from(data);
    }
    const view = data;
    return Buffer.from(view.buffer, view.byteOffset, view.byteLength);
}
class WsClientAdapter {
    constructor(network, config = {}) {
        var _a;
        this.connected = false;
        this.stats = {
            snapshotsReceived: 0,
            bytesReceived: 0,
            bytesSent: 0
        };
        this.socket = null;
        this.network = network;
        this.binary = (_a = config.binary) !== null && _a !== void 0 ? _a : nengi_buffers_1.bufferBinary;
    }
    flush() {
        if (!this.socket) {
            return;
        }
        if (this.socket.readyState !== ws_1.default.OPEN) {
            return;
        }
        const buffer = this.network.createOutbound(this.binary);
        this.stats.bytesSent += buffer.byteLength;
        this.socket.send(buffer);
    }
    disconnect(code = 1000, reason = 'closed') {
        var _a;
        (_a = this.socket) === null || _a === void 0 ? void 0 : _a.close(code, reason);
    }
    setupWebsocket(socket) {
        this.socket = socket;
        socket.removeAllListeners('message');
        socket.on('message', data => {
            const buffer = toBuffer(data);
            this.stats.snapshotsReceived++;
            this.stats.bytesReceived += buffer.byteLength;
            const dr = this.binary.createReader(buffer);
            this.network.readSnapshot(dr);
        });
        socket.removeAllListeners('close');
        socket.on('close', (code, reason) => {
            this.connected = false;
            this.network.onDisconnect(reason.toString() || `closed:${code}`);
        });
        socket.removeAllListeners('error');
        socket.on('error', event => {
            this.network.onSocketError(event);
        });
    }
    connect(wsUrl, handshake) {
        return new Promise((resolve, reject) => {
            const socket = new ws_1.default(wsUrl, { perMessageDeflate: false });
            this.socket = socket;
            let settled = false;
            socket.on('open', () => {
                socket.send(this.network.createHandshake(handshake, this.binary));
            });
            socket.on('close', (code, reason) => {
                if (!settled) {
                    settled = true;
                    reject(reason.toString() || `closed:${code}`);
                    return;
                }
                this.connected = false;
                this.network.onDisconnect(reason.toString() || `closed:${code}`);
            });
            socket.on('error', event => {
                this.network.onSocketError(event);
                if (!settled) {
                    settled = true;
                    reject(event);
                }
            });
            socket.on('message', data => {
                const result = this.network.readHandshakeResponse(this.binary.createReader(toBuffer(data)));
                if (result.accepted) {
                    settled = true;
                    this.connected = true;
                    this.setupWebsocket(socket);
                    resolve(result);
                }
                else {
                    settled = true;
                    reject(result.reason);
                }
            });
        });
    }
}
exports.WsClientAdapter = WsClientAdapter;
