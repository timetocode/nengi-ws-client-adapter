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
    if (Buffer.isBuffer(data))
        return data;
    if (Array.isArray(data))
        return Buffer.concat(data);
    return Buffer.from(data);
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
    constructor(config = {}) {
        var _a;
        this.clientAdapterVersion = 2;
        this.socket = null;
        this.handlers = null;
        this.stats = { messagesReceived: 0, bytesReceived: 0, bytesSent: 0 };
        this.binary = (_a = config.binary) !== null && _a !== void 0 ? _a : nengi_buffers_1.bufferBinary;
    }
    open(url, handlers) {
        const socket = new ws_1.default(url, { perMessageDeflate: false });
        this.socket = socket;
        this.handlers = handlers;
        socket.on('open', () => {
            if (this.socket === socket)
                handlers.onOpen();
        });
        socket.on('message', (data, isBinary) => {
            if (this.socket !== socket)
                return;
            if (!isBinary) {
                handlers.onError(new Error('The nengi transport requires binary messages.'));
                return;
            }
            const payload = toBuffer(data);
            const deliver = (message) => {
                if (this.socket !== socket)
                    return;
                this.stats.messagesReceived++;
                this.stats.bytesReceived += message.byteLength;
                handlers.onMessage(message);
            };
            if (this.link)
                this.link.sendServerToClient(payload, deliver);
            else
                deliver(payload);
        });
        socket.on('close', (code, reason) => {
            var _a;
            if (this.socket !== socket)
                return;
            this.socket = null;
            this.handlers = null;
            (_a = this.link) === null || _a === void 0 ? void 0 : _a.clear();
            handlers.onClose({ code, reason: reason.toString() });
        });
        // Keep an error listener even after cancellation: ws can emit an error
        // asynchronously when terminate() aborts an unfinished HTTP upgrade.
        socket.on('error', cause => {
            if (this.socket === socket)
                handlers.onError(cause);
        });
    }
    send(payload) {
        const socket = this.socket;
        const handlers = this.handlers;
        // The pending close event owns the reason; a late flush must not replace it.
        if (socket && (socket.readyState === ws_1.default.CLOSING || socket.readyState === ws_1.default.CLOSED))
            return;
        if (!socket || socket.readyState !== ws_1.default.OPEN)
            throw new Error('The WebSocket transport is not open.');
        const deliver = (message) => {
            if (this.socket !== socket)
                return;
            try {
                if (socket.readyState === ws_1.default.CLOSING || socket.readyState === ws_1.default.CLOSED)
                    return;
                if (socket.readyState !== ws_1.default.OPEN)
                    throw new Error('The WebSocket transport is not open.');
                socket.send(message, error => {
                    if (error && this.socket === socket)
                        handlers === null || handlers === void 0 ? void 0 : handlers.onError(error);
                });
                this.stats.bytesSent += message.byteLength;
            }
            catch (cause) {
                handlers === null || handlers === void 0 ? void 0 : handlers.onError(cause);
            }
        };
        if (this.link)
            this.link.sendClientToServer(payload, deliver);
        else
            deliver(payload);
    }
    close(reason, force) {
        var _a;
        const socket = this.socket;
        this.socket = null;
        this.handlers = null;
        (_a = this.link) === null || _a === void 0 ? void 0 : _a.clear();
        if (!socket)
            return;
        if (force || socket.readyState === ws_1.default.CONNECTING) {
            socket.terminate();
        }
        else {
            try {
                socket.close(1000, reason);
            }
            catch (_b) {
                socket.terminate();
            }
        }
    }
}
exports.WsClientAdapter = WsClientAdapter;
class SimulatedWsClientAdapter extends WsClientAdapter {
    constructor(config) {
        super(config);
        if (!(config === null || config === void 0 ? void 0 : config.conditions))
            throw new Error('SimulatedWsClientAdapter requires config.conditions.');
        this.conditions = new nengi_1.NetworkConditionLink(config.conditions, { timers: liveTimers });
        this.link = this.conditions;
    }
    configureNetworkConditions(conditions) {
        this.conditions.configure(conditions);
    }
    getNetworkConditionStatus() {
        return this.conditions.status();
    }
}
exports.SimulatedWsClientAdapter = SimulatedWsClientAdapter;
