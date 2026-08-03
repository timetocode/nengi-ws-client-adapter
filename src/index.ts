import {
    NetworkConditionLink
} from 'nengi'
import type {
    BinaryAdapter,
    ClientNetwork,
    IClientNetworkAdapter,
    NetworkConditions,
    NetworkConditionStatus
} from 'nengi'

import WebSocket, { RawData } from 'ws'
import { bufferBinary } from 'nengi-buffers'

function toBuffer(data: RawData): Buffer {
    if (Buffer.isBuffer(data)) {
        return data
    }
    if (Array.isArray(data)) {
        return Buffer.concat(data)
    }
    if (data instanceof ArrayBuffer) {
        return Buffer.from(data)
    }
    const view = data as ArrayBufferView
    return Buffer.from(view.buffer, view.byteOffset, view.byteLength)
}

export type WsClientAdapterStats = {
    snapshotsReceived: number
    bytesReceived: number
    bytesSent: number
}

export type WsClientAdapterConfig = {
    binary?: BinaryAdapter<Buffer>
}

export type SimulatedWsClientAdapterConfig = WsClientAdapterConfig & {
    conditions: NetworkConditions
}

const liveTimers = {
    setTimeout(callback: () => void, delayMs: number) {
        return globalThis.setTimeout(callback, delayMs)
    },
    clearTimeout(handle: unknown) {
        globalThis.clearTimeout(handle as ReturnType<typeof setTimeout>)
    }
}

class WsClientAdapter implements IClientNetworkAdapter<Buffer, Buffer, string> {
    socket: WebSocket | null
    network: ClientNetwork
    binary: BinaryAdapter<Buffer>
    connected = false
    stats: WsClientAdapterStats = {
        snapshotsReceived: 0,
        bytesReceived: 0,
        bytesSent: 0
    }

    constructor(network: ClientNetwork, config: WsClientAdapterConfig = {}) {
        this.socket = null
        this.network = network
        this.binary = config.binary ?? bufferBinary
    }

    flush() {
        if (!this.socket) {
            return
        }

        if (this.socket!.readyState !== WebSocket.OPEN) {
            return
        }

        const buffer = this.network.createOutbound(this.binary)
        this.stats.bytesSent += buffer.byteLength
        this.socket!.send(buffer)
    }

    flushPongs() {
        const socket = this.socket
        if (!socket || socket.readyState !== WebSocket.OPEN || !this.connected) {
            return
        }
        try {
            this.network.flushPongs(this.binary, payload => {
                this.stats.bytesSent += payload.byteLength
                socket.send(payload)
            })
        } catch (error) {
            this.network.onSocketError(error)
        }
    }

    disconnect(reason?: any) {
        const payload = typeof reason === 'string' ? reason : JSON.stringify(reason ?? 'closed')
        this.socket?.close(1000, payload)
    }

    private setupWebsocket(socket: WebSocket) {
        this.socket = socket

        socket.removeAllListeners('message')
        socket.on('message', data => {
            const buffer = toBuffer(data)
            this.stats.snapshotsReceived++
            this.stats.bytesReceived += buffer.byteLength
            const dr = this.binary.createReader(buffer)
            this.network.readSnapshot(dr)
        })

        socket.removeAllListeners('close')
        socket.on('close', (code, reason) => {
            this.connected = false
            this.network.onDisconnect(reason.toString() || `closed:${code}`)
        })

        socket.removeAllListeners('error')
        socket.on('error', event => {
            this.network.onSocketError(event)
        })
    }

    connect(wsUrl: string, handshake: any = {}) {
        return new Promise((resolve, reject) => {
            const socket = new WebSocket(wsUrl, { perMessageDeflate: false })
            this.socket = socket
            let settled = false

            socket.on('open', () => {
                socket.send(this.network.createHandshake(handshake, this.binary))
            })

            socket.on('close', (code, reason) => {
                if (!settled) {
                    settled = true
                    reject(reason.toString() || `closed:${code}`)
                    return
                }
                this.connected = false
                this.network.onDisconnect(reason.toString() || `closed:${code}`)
            })

            socket.on('error', event => {
                this.network.onSocketError(event)
                if (!settled) {
                    settled = true
                    reject(event)
                }
            })

            socket.on('message', data => {
                const result = this.network.readHandshakeResponse(this.binary.createReader(toBuffer(data)))
                if (result.accepted) {
                    settled = true
                    this.connected = true
                    this.setupWebsocket(socket)
                    resolve(result)
                } else {
                    settled = true
                    reject(result.reason)
                }
            })
        })
    }
}

class SimulatedWsClientAdapter implements IClientNetworkAdapter<Buffer, Buffer, string> {
    socket: WebSocket | null = null
    network: ClientNetwork
    binary: BinaryAdapter<Buffer>
    connected = false
    stats: WsClientAdapterStats = {
        snapshotsReceived: 0,
        bytesReceived: 0,
        bytesSent: 0
    }
    readonly conditions: NetworkConditionLink

    constructor(network: ClientNetwork, config: SimulatedWsClientAdapterConfig) {
        if (!config?.conditions) {
            throw new Error('SimulatedWsClientAdapter requires config.conditions.')
        }
        this.network = network
        this.binary = config.binary ?? bufferBinary
        this.conditions = new NetworkConditionLink(config.conditions, {
            timers: liveTimers
        })
    }

    flush() {
        if (!this.socket || this.socket.readyState !== WebSocket.OPEN || !this.connected) {
            return
        }
        this.send(this.network.createOutbound(this.binary))
    }

    flushPongs() {
        if (!this.socket || this.socket.readyState !== WebSocket.OPEN || !this.connected) {
            return
        }
        try {
            this.network.flushPongs(this.binary, payload => this.send(payload))
        } catch (error) {
            this.network.onSocketError(error)
        }
    }

    disconnect(reason?: any) {
        this.conditions.clear()
        const payload = typeof reason === 'string' ? reason : JSON.stringify(reason ?? 'closed')
        this.socket?.close(1000, payload)
        this.socket = null
        this.connected = false
    }

    configureNetworkConditions(conditions: NetworkConditions) {
        this.conditions.configure(conditions)
    }

    getNetworkConditionStatus(): NetworkConditionStatus {
        return this.conditions.status()
    }

    connect(wsUrl: string, handshake: any = {}) {
        return new Promise((resolve, reject) => {
            const socket = new WebSocket(wsUrl, { perMessageDeflate: false })
            this.socket = socket
            let settled = false

            socket.on('open', () => {
                this.send(this.network.createHandshake(handshake, this.binary), false)
            })

            socket.on('close', (code, reason) => {
                const wasConnected = this.connected
                this.conditions.clear()
                this.socket = null
                this.connected = false
                const detail = reason.toString() || `closed:${code}`
                if (!settled) {
                    settled = true
                    reject(detail)
                    return
                }
                if (wasConnected) {
                    this.network.onDisconnect(detail)
                }
            })

            socket.on('error', event => {
                this.network.onSocketError(event)
                if (!settled) {
                    settled = true
                    reject(event)
                    this.conditions.clear()
                    socket.close()
                }
            })

            socket.on('message', data => {
                const buffer = toBuffer(data)
                this.conditions.sendServerToClient(buffer, payload => {
                    if (socket !== this.socket || socket.readyState !== WebSocket.OPEN) {
                        return
                    }
                    if (settled && !this.connected) {
                        return
                    }
                    if (!this.connected) {
                        const result = this.network.readHandshakeResponse(this.binary.createReader(payload))
                        if (result.accepted) {
                            settled = true
                            this.connected = true
                            resolve(result)
                        } else {
                            settled = true
                            socket.close(1000, typeof result.reason === 'string'
                                ? result.reason
                                : JSON.stringify(result.reason ?? 'closed'))
                            reject(result.reason)
                        }
                        return
                    }
                    this.stats.bytesReceived += payload.byteLength
                    this.stats.snapshotsReceived++
                    this.network.readSnapshot(this.binary.createReader(payload))
                })
            })
        })
    }

    private send(buffer: Buffer, trackStats = true) {
        const socket = this.socket
        if (!socket) {
            return
        }
        this.conditions.sendClientToServer(buffer, payload => {
            if (socket === this.socket && socket.readyState === WebSocket.OPEN) {
                if (trackStats) {
                    this.stats.bytesSent += payload.byteLength
                }
                socket.send(payload)
            }
        })
    }
}

export { WsClientAdapter, SimulatedWsClientAdapter }
