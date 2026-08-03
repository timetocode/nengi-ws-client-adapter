import { AddressInfo } from 'node:net'
import { describe, expect, it, jest } from '@jest/globals'
import WebSocket, { WebSocketServer } from 'ws'
import {
    SimulatedWsClientAdapter,
    WsClientAdapter
} from './index'

function createNetwork() {
    return {
        createHandshake: jest.fn(() => Buffer.from([1])),
        createOutbound: jest.fn(() => Buffer.from([8])),
        readHandshakeResponse: jest.fn((): any => ({ accepted: true })),
        readSnapshot: jest.fn(),
        flushPongs: jest.fn((_binary: unknown, send: (payload: Buffer) => void) => {
            send(Buffer.from([9]))
            return 1
        }),
        onDisconnect: jest.fn(),
        onSocketError: jest.fn()
    }
}

function listen(server: WebSocketServer) {
    return new Promise<void>((resolve, reject) => {
        server.once('listening', resolve)
        server.once('error', reject)
    })
}

function closeServer(server: WebSocketServer) {
    return new Promise<void>(resolve => server.close(() => resolve()))
}

function nextConnection(server: WebSocketServer) {
    return new Promise<WebSocket>(resolve => server.once('connection', resolve))
}

function nextMessage(socket: WebSocket) {
    return new Promise<Buffer>(resolve => socket.once('message', data => {
        resolve(Buffer.isBuffer(data) ? data : Buffer.from(data as ArrayBuffer))
    }))
}

function nextClose(socket: WebSocket) {
    return new Promise<void>(resolve => socket.once('close', () => resolve()))
}

describe('Node ws client adapters', () => {
    it('conditions the handshake and sends automatic Pongs over a real socket', async () => {
        const server = new WebSocketServer({ host: '127.0.0.1', port: 0, perMessageDeflate: false })
        await listen(server)
        const connection = nextConnection(server)
        const network = createNetwork()
        const adapter = new SimulatedWsClientAdapter(network as any, {
            conditions: {
                seed: 11,
                clientToServer: { latencyMs: 5 },
                serverToClient: { latencyMs: 7 }
            }
        })
        network.readSnapshot.mockImplementation(() => adapter.flushPongs())
        const port = (server.address() as AddressInfo).port

        const connecting = adapter.connect(`ws://127.0.0.1:${port}`)
        const socket = await connection
        expect(await nextMessage(socket)).toEqual(Buffer.from([1]))
        socket.send(Buffer.from([2]))
        await expect(connecting).resolves.toMatchObject({ accepted: true })

        const pong = nextMessage(socket)
        socket.send(Buffer.from([3]))
        expect(await pong).toEqual(Buffer.from([9]))
        expect(network.readSnapshot).toHaveBeenCalledTimes(1)
        expect(adapter.stats.snapshotsReceived).toBe(1)
        expect(adapter.getNetworkConditionStatus().clientToServer.delivered).toBe(2)

        adapter.configureNetworkConditions({
            seed: 12,
            clientToServer: { latencyMs: 1000 }
        })
        adapter.flush()
        expect(adapter.getNetworkConditionStatus().clientToServer.queued).toBe(1)
        const closed = nextClose(socket)
        adapter.disconnect('done')
        await closed
        expect(adapter.getNetworkConditionStatus().clientToServer.queued).toBe(0)
        await closeServer(server)
    })

    it('reports established transport closure', async () => {
        const server = new WebSocketServer({ host: '127.0.0.1', port: 0, perMessageDeflate: false })
        await listen(server)
        const connection = nextConnection(server)
        const network = createNetwork()
        const adapter = new WsClientAdapter(network as any)
        const port = (server.address() as AddressInfo).port

        const connecting = adapter.connect(`ws://127.0.0.1:${port}`)
        const socket = await connection
        expect(await nextMessage(socket)).toEqual(Buffer.from([1]))
        socket.send(Buffer.from([2]))
        await connecting

        socket.close(1000, 'transport_lost')
        await new Promise<void>(resolve => {
            const check = () => network.onDisconnect.mock.calls.length > 0
                ? resolve()
                : setTimeout(check, 1)
            check()
        })
        expect(network.onDisconnect).toHaveBeenCalledWith('transport_lost')
        await closeServer(server)
    })
})
