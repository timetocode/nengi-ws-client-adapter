import { NetworkConditionLink } from 'nengi'
import type {
    BinaryAdapter,
    ClientTransportHandlers,
    IClientNetworkAdapter,
    NetworkConditions,
    NetworkConditionStatus
} from 'nengi'
import WebSocket, { RawData } from 'ws'
import { bufferBinary } from 'nengi-buffers'

function toBuffer(data: RawData): Buffer {
    if (Buffer.isBuffer(data)) return data
    if (Array.isArray(data)) return Buffer.concat(data)
    return Buffer.from(data)
}

export type WsClientAdapterStats = {
    /** Binary transport messages, including the connection handshake. */
    messagesReceived: number
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
    readonly clientAdapterVersion = 2 as const
    socket: WebSocket | null = null
    binary: BinaryAdapter<Buffer>
    protected link?: NetworkConditionLink
    private handlers: ClientTransportHandlers<Buffer> | null = null
    stats: WsClientAdapterStats = { messagesReceived: 0, bytesReceived: 0, bytesSent: 0 }

    constructor(config: WsClientAdapterConfig = {}) {
        this.binary = config.binary ?? bufferBinary
    }

    open(url: string, handlers: ClientTransportHandlers<Buffer>) {
        const socket = new WebSocket(url, { perMessageDeflate: false })
        this.socket = socket
        this.handlers = handlers
        socket.on('open', () => {
            if (this.socket === socket) handlers.onOpen()
        })
        socket.on('message', (data, isBinary) => {
            if (this.socket !== socket) return
            if (!isBinary) {
                handlers.onError(new Error('The nengi transport requires binary messages.'))
                return
            }
            const payload = toBuffer(data)
            const deliver = (message: Buffer) => {
                if (this.socket !== socket) return
                this.stats.messagesReceived++
                this.stats.bytesReceived += message.byteLength
                handlers.onMessage(message)
            }
            if (this.link) this.link.sendServerToClient(payload, deliver)
            else deliver(payload)
        })
        socket.on('close', (code, reason) => {
            if (this.socket !== socket) return
            this.socket = null
            this.handlers = null
            this.link?.clear()
            handlers.onClose({ code, reason: reason.toString() })
        })
        // Keep an error listener even after cancellation: ws can emit an error
        // asynchronously when terminate() aborts an unfinished HTTP upgrade.
        socket.on('error', cause => {
            if (this.socket === socket) handlers.onError(cause)
        })
    }

    send(payload: Buffer) {
        const socket = this.socket
        const handlers = this.handlers
        // The pending close event owns the reason; a late flush must not replace it.
        if (socket && (socket.readyState === WebSocket.CLOSING || socket.readyState === WebSocket.CLOSED)) return
        if (!socket || socket.readyState !== WebSocket.OPEN) throw new Error('The WebSocket transport is not open.')
        const deliver = (message: Buffer) => {
            if (this.socket !== socket) return
            try {
                if (socket.readyState === WebSocket.CLOSING || socket.readyState === WebSocket.CLOSED) return
                if (socket.readyState !== WebSocket.OPEN) throw new Error('The WebSocket transport is not open.')
                socket.send(message, error => {
                    if (error && this.socket === socket) handlers?.onError(error)
                })
                this.stats.bytesSent += message.byteLength
            } catch (cause) {
                handlers?.onError(cause)
            }
        }
        if (this.link) this.link.sendClientToServer(payload, deliver)
        else deliver(payload)
    }

    close(reason: string, force: boolean) {
        const socket = this.socket
        this.socket = null
        this.handlers = null
        this.link?.clear()
        if (!socket) return
        if (force || socket.readyState === WebSocket.CONNECTING) {
            socket.terminate()
        } else {
            try {
                socket.close(1000, reason)
            } catch {
                socket.terminate()
            }
        }
    }
}

class SimulatedWsClientAdapter extends WsClientAdapter {
    readonly conditions: NetworkConditionLink

    constructor(config: SimulatedWsClientAdapterConfig) {
        super(config)
        if (!config?.conditions) throw new Error('SimulatedWsClientAdapter requires config.conditions.')
        this.conditions = new NetworkConditionLink(config.conditions, { timers: liveTimers })
        this.link = this.conditions
    }

    configureNetworkConditions(conditions: NetworkConditions) {
        this.conditions.configure(conditions)
    }

    getNetworkConditionStatus(): NetworkConditionStatus {
        return this.conditions.status()
    }
}

export { WsClientAdapter, SimulatedWsClientAdapter }
