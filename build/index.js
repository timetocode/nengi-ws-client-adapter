"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.SimulatedWsClientAdapter = exports.WsClientAdapter = void 0;
const nengi_1 = require("nengi");
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
const liveTimers = {
    setTimeout(callback, delayMs) {
        return globalThis.setTimeout(callback, delayMs);
    },
    clearTimeout(handle) {
        globalThis.clearTimeout(handle);
    }
};
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
    flushPongs() {
        const socket = this.socket;
        if (!socket || socket.readyState !== ws_1.default.OPEN || !this.connected) {
            return;
        }
        try {
            this.network.flushPongs(this.binary, payload => {
                this.stats.bytesSent += payload.byteLength;
                socket.send(payload);
            });
        }
        catch (error) {
            this.network.onSocketError(error);
        }
    }
    disconnect(reason) {
        var _a;
        const payload = typeof reason === 'string' ? reason : JSON.stringify(reason !== null && reason !== void 0 ? reason : 'closed');
        (_a = this.socket) === null || _a === void 0 ? void 0 : _a.close(1000, payload);
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
    connect(wsUrl, handshake = {}) {
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
class SimulatedWsClientAdapter {
    constructor(network, config) {
        var _a;
        this.socket = null;
        this.connected = false;
        this.stats = {
            snapshotsReceived: 0,
            bytesReceived: 0,
            bytesSent: 0
        };
        if (!(config === null || config === void 0 ? void 0 : config.conditions)) {
            throw new Error('SimulatedWsClientAdapter requires config.conditions.');
        }
        this.network = network;
        this.binary = (_a = config.binary) !== null && _a !== void 0 ? _a : nengi_buffers_1.bufferBinary;
        this.conditions = new nengi_1.NetworkConditionLink(config.conditions, {
            timers: liveTimers
        });
    }
    flush() {
        if (!this.socket || this.socket.readyState !== ws_1.default.OPEN || !this.connected) {
            return;
        }
        this.send(this.network.createOutbound(this.binary));
    }
    flushPongs() {
        if (!this.socket || this.socket.readyState !== ws_1.default.OPEN || !this.connected) {
            return;
        }
        try {
            this.network.flushPongs(this.binary, payload => this.send(payload));
        }
        catch (error) {
            this.network.onSocketError(error);
        }
    }
    disconnect(reason) {
        var _a;
        this.conditions.clear();
        const payload = typeof reason === 'string' ? reason : JSON.stringify(reason !== null && reason !== void 0 ? reason : 'closed');
        (_a = this.socket) === null || _a === void 0 ? void 0 : _a.close(1000, payload);
        this.socket = null;
        this.connected = false;
    }
    configureNetworkConditions(conditions) {
        this.conditions.configure(conditions);
    }
    getNetworkConditionStatus() {
        return this.conditions.status();
    }
    connect(wsUrl, handshake = {}) {
        return new Promise((resolve, reject) => {
            const socket = new ws_1.default(wsUrl, { perMessageDeflate: false });
            this.socket = socket;
            let settled = false;
            socket.on('open', () => {
                this.send(this.network.createHandshake(handshake, this.binary), false);
            });
            socket.on('close', (code, reason) => {
                const wasConnected = this.connected;
                this.conditions.clear();
                this.socket = null;
                this.connected = false;
                const detail = reason.toString() || `closed:${code}`;
                if (!settled) {
                    settled = true;
                    reject(detail);
                    return;
                }
                if (wasConnected) {
                    this.network.onDisconnect(detail);
                }
            });
            socket.on('error', event => {
                this.network.onSocketError(event);
                if (!settled) {
                    settled = true;
                    reject(event);
                    this.conditions.clear();
                    socket.close();
                }
            });
            socket.on('message', data => {
                const buffer = toBuffer(data);
                this.conditions.sendServerToClient(buffer, payload => {
                    var _a;
                    if (socket !== this.socket || socket.readyState !== ws_1.default.OPEN) {
                        return;
                    }
                    if (settled && !this.connected) {
                        return;
                    }
                    if (!this.connected) {
                        const result = this.network.readHandshakeResponse(this.binary.createReader(payload));
                        if (result.accepted) {
                            settled = true;
                            this.connected = true;
                            resolve(result);
                        }
                        else {
                            settled = true;
                            socket.close(1000, typeof result.reason === 'string'
                                ? result.reason
                                : JSON.stringify((_a = result.reason) !== null && _a !== void 0 ? _a : 'closed'));
                            reject(result.reason);
                        }
                        return;
                    }
                    this.stats.bytesReceived += payload.byteLength;
                    this.stats.snapshotsReceived++;
                    this.network.readSnapshot(this.binary.createReader(payload));
                });
            });
        });
    }
    send(buffer, trackStats = true) {
        const socket = this.socket;
        if (!socket) {
            return;
        }
        this.conditions.sendClientToServer(buffer, payload => {
            if (socket === this.socket && socket.readyState === ws_1.default.OPEN) {
                if (trackStats) {
                    this.stats.bytesSent += payload.byteLength;
                }
                socket.send(payload);
            }
        });
    }
}
exports.SimulatedWsClientAdapter = SimulatedWsClientAdapter;
