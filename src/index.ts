import type { BinaryAdapter, ClientNetwork, IClientNetworkAdapter } from 'nengi'

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

    constructor(network: ClientNetwork, config: any = {}) {
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

export { WsClientAdapter }
