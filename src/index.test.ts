import { describe, expect, it, jest } from '@jest/globals'
import net from 'node:net'
import { AddressInfo } from 'node:net'
import WebSocket, { WebSocketServer } from 'ws'
import { Client, Context, Instance, NetworkEvent } from 'nengi'
import { WsInstanceAdapter } from 'nengi-ws-instance-adapter'
import { SimulatedWsClientAdapter, WsClientAdapter } from './index'

async function game() {
    const instance = new Instance(new Context())
    instance.onConnect = async () => true
    const server = new WsInstanceAdapter(instance.adapterHost)
    await new Promise<void>(resolve => server.listen({ host: '127.0.0.1', port: 0 }, resolve))
    const url = `ws://127.0.0.1:${(server.server!.address() as AddressInfo).port}`
    return { instance, server, url }
}

const delay = (ms: number) => new Promise<void>(resolve => setTimeout(resolve, ms))

describe.each([WsClientAdapter, SimulatedWsClientAdapter])('%p transport lifecycle', Adapter => {
    function client(options = {}) {
        return new Client(new Context(), Adapter, 20, {
            conditions: { seed: 3, clientToServer: { latencyMs: 2 }, serverToClient: { latencyMs: 3 } }
        }, { connectTimeoutMs: 300, serverTimeoutMs: 1000, ...options })
    }

    it('returns public setup data over a real socket while retaining private context on the server', async () => {
        const { instance, server, url } = await game()
        const c = client()
        const serverData = { databaseId: BigInt(7), character: { name: '旅人 🪁' } }
        const clientData = { name: serverData.character.name, room: 'garden' }
        instance.onConnect = async () => ({ serverData, clientData })
        try {
            await expect(c.connect(url)).resolves.toEqual(clientData)
            expect(instance.queue.next()).toMatchObject({ type: NetworkEvent.UserConnected, payload: serverData })
            expect(c.network.processNextFrame()).toBeNull()
            instance.step()
            await delay(30)
            expect(c.network.processNextFrame()).not.toBeNull()
        } finally {
            c.disconnect()
            await server.shutdown()
        }
    })

    it.each(['upgrade', 'acceptance'])('bounds silent %s, terminates the socket, and permits retry', async silence => {
        const sockets = new Set<net.Socket>()
        const tcp = net.createServer()
        tcp.on('connection', socket => {
            sockets.add(socket)
            socket.on('close', () => sockets.delete(socket))
        })
        // A WebSocket listener accepts the upgrade but never answers nengi.
        let websocket: WebSocketServer | undefined
        if (silence === 'acceptance') {
            websocket = new WebSocketServer({ host: '127.0.0.1', port: 0 })
            await new Promise<void>(resolve => websocket!.once('listening', resolve))
        } else {
            await new Promise<void>(resolve => tcp.listen(0, '127.0.0.1', resolve))
        }
        const port = ((websocket?.address() ?? tcp.address()) as AddressInfo).port
        const c = client()
        const closed = jest.fn()
        c.setDisconnectHandler(closed)
        let server: WsInstanceAdapter | undefined
        try {
            const attempt = c.connect(`ws://127.0.0.1:${port}`)
            const socket = c.adapter.socket!
            await expect(attempt).rejects.toMatchObject({ code: 'CONNECT_TIMEOUT' })
            expect(c.connectionState).toBe('idle')
            expect(c.adapter.socket).toBeNull()
            await delay(20)
            expect(socket.readyState).toBe(3)
            expect(closed).not.toHaveBeenCalled()
            const ready = await game()
            server = ready.server
            await expect(c.connect(ready.url)).resolves.toBeUndefined()
            expect(c.connectionState).toBe('connected')
        } finally {
            c.disconnect()
            for (const socket of sockets) socket.destroy()
            if (websocket) {
                for (const socket of websocket.clients) socket.terminate()
                await new Promise<void>(resolve => websocket!.close(() => resolve()))
            } else await new Promise<void>(resolve => tcp.close(() => resolve()))
            await server?.shutdown()
        }
    })

    it.each(['denied', 'cancelled', 'invalid URL', 'invalid handshake'])('retries after %s without duplicate lifecycle notifications', async outcome => {
        const { instance, server, url } = await game()
        const c = client()
        const closed = jest.fn()
        c.setDisconnectHandler(closed)
        if (outcome === 'denied') instance.onConnect = async () => false
        if (outcome === 'cancelled') instance.onConnect = () => new Promise(() => {})
        try {
            const attempt = c.connect(outcome === 'invalid URL' ? 'bad-url' : url,
                outcome === 'invalid handshake' ? { invalid: BigInt(1) } : {})
            const rejected = expect(attempt).rejects.toBeDefined()
            if (outcome === 'cancelled') c.disconnect()
            await rejected
            expect(c.connectionState).toBe('idle')
            expect(c.adapter.socket).toBeNull()
            expect(closed).not.toHaveBeenCalled()
            instance.onConnect = async () => true
            await c.connect(url)
            instance.step()
            await delay(30)
            expect(c.network.processNextFrame()).not.toBeNull()
            expect(c.adapter.stats.messagesReceived).toBeGreaterThanOrEqual(2)
        } finally {
            c.disconnect()
            await server.shutdown()
        }
    })

    it('detects an accepted server that stops publishing and rejects pending requests', async () => {
        const { server, url } = await game()
        const c = client({ serverTimeoutMs: 100 })
        try {
            await c.connect(url)
            const closed = new Promise<any>(resolve => c.setDisconnectHandler(resolve))
            const response = c.request(1, {}, { timeoutMs: 0 })
            const rejected = expect(response).rejects.toMatchObject({ code: 'DISCONNECTED' })
            c.flush()
            await expect(closed).resolves.toMatchObject({ code: 'SERVER_TIMEOUT' })
            await rejected
            expect(c.connectionState).toBe('closed')
            expect(c.adapter.socket).toBeNull()
        } finally {
            c.disconnect()
            await server.shutdown()
        }
    })

    it('answers real Pings without application flush or frame draining', async () => {
        const { instance, server, url } = await game()
        const c = client()
        try {
            await c.connect(url)
            instance.step()
            await delay(35)
            const user = [...instance.users.values()][0]
            expect(user.clockSyncSamples).toBeGreaterThan(0)
            expect(c.network.commandFrameNumber).toBe(1)
            expect(c.network.getPendingFrameCount()).toBe(1)
        } finally {
            c.disconnect()
            await server.shutdown()
        }
    })

    it('finishes logical close synchronously even if the peer stops reading and the detail is circular', async () => {
        const { server, url } = await game()
        const c = client()
        try {
            await c.connect(url)
            const peer = [...server.server!.clients][0]
            peer.pause()
            const closed = jest.fn()
            c.setDisconnectHandler(closed)
            const response = c.request(1, {}, { timeoutMs: 0 })
            const rejected = expect(response).rejects.toMatchObject({ code: 'DISCONNECTED' })
            const detail: any = { text: 'é'.repeat(200) }
            detail.self = detail
            const socket = c.adapter.socket!
            c.disconnect(detail)
            expect(c.connectionState).toBe('closed')
            expect(closed).toHaveBeenCalledTimes(1)
            expect((closed.mock.calls[0][0] as { detail: unknown }).detail).toBe(detail)
            expect(c.adapter.socket).toBeNull()
            await rejected
            expect((socket as any)._closeTimer).toBeDefined()
            peer.resume()
        } finally {
            c.disconnect()
            await server.shutdown()
        }
    })

    it.each([WebSocket.CLOSING, WebSocket.CLOSED])('preserves the queued close reason when flush sees readyState %s', async state => {
        const { server, url } = await game()
        const c = client()
        const closed = jest.fn()
        c.setDisconnectHandler(closed)
        await c.connect(url)
        const socket = c.adapter.socket!
        // Hold the native close notification while exposing its earlier state.
        const readyState = jest.spyOn(socket, 'readyState', 'get').mockReturnValue(state)
        const send = jest.spyOn(socket, 'send')
        try {
            const pending = c.request(1, {}, { timeoutMs: 0 })
            const rejected = expect(pending).rejects.toMatchObject({ code: 'DISCONNECTED' })
            const sent = c.adapter.stats.bytesSent
            c.flush()
            expect(closed).not.toHaveBeenCalled()
            expect(c.connectionState).toBe('connected')
            expect(send).not.toHaveBeenCalled()
            expect(c.adapter.stats.bytesSent).toBe(sent)
            const reason = '{"reason":"participant_in_use"}'
            socket.emit('close', 1000, Buffer.from(reason))
            socket.emit('close', 1000, Buffer.from(reason))
            expect(closed).toHaveBeenCalledTimes(1)
            expect(closed).toHaveBeenCalledWith(expect.objectContaining({
                code: 'REMOTE_CLOSE', detail: { code: 1000, reason }
            }))
            await rejected
        } finally {
            readyState.mockRestore()
            send.mockRestore()
            socket.terminate()
            c.disconnect()
            await server.shutdown()
        }
    })

    it('keeps absent/connecting sockets and asynchronous write errors as failures', async () => {
        const { server, url } = await game()
        const c = client()
        const closed = jest.fn()
        c.setDisconnectHandler(closed)
        try {
            expect(() => c.adapter.send(Buffer.alloc(1))).toThrow('not open')
            const connecting = c.connect(url)
            expect(() => c.adapter.send(Buffer.alloc(1))).toThrow('not open')
            await connecting
            const cause = new Error('write failed')
            jest.spyOn(c.adapter.socket!, 'send').mockImplementation((...args: unknown[]) => {
                const callback = args[args.length - 1] as (error: Error) => void
                queueMicrotask(() => callback(cause))
            })
            const ended = new Promise<void>(resolve => c.setDisconnectHandler(info => {
                closed(info)
                resolve()
            }))
            c.flush()
            await ended
            expect(closed).toHaveBeenCalledTimes(1)
            expect(closed).toHaveBeenCalledWith(expect.objectContaining({ code: 'TRANSPORT_ERROR', cause }))
        } finally {
            c.disconnect()
            await server.shutdown()
        }
    })
})

it.each([WebSocket.CLOSING, WebSocket.CLOSED])('a delayed send preserves a later close reason at readyState %s', async state => {
    const { server, url } = await game()
    const c = new Client(new Context(), SimulatedWsClientAdapter, 20, {
        conditions: { seed: 1 }
    })
    const closed = jest.fn()
    c.setDisconnectHandler(closed)
    await c.connect(url)
    const socket = c.adapter.socket!
    const send = jest.spyOn(socket, 'send')
    const readyState = jest.spyOn(socket, 'readyState', 'get')
    try {
        c.adapter.configureNetworkConditions({ seed: 1, clientToServer: { latencyMs: 100 } })
        const pending = c.request(1, {}, { timeoutMs: 0 })
        const rejected = expect(pending).rejects.toMatchObject({ code: 'DISCONNECTED' })
        const sent = c.adapter.stats.bytesSent
        c.flush()
        const due = c.adapter.conditions.status().clientToServer.nextReleaseAtMs!
        expect(due).not.toBeNull()
        readyState.mockReturnValue(state)
        // Deliver synchronously so this race does not depend on machine timing.
        expect(c.adapter.conditions.advanceTo(due)).toBe(1)
        expect(closed).not.toHaveBeenCalled()
        expect(send).not.toHaveBeenCalled()
        expect(c.adapter.stats.bytesSent).toBe(sent)
        const reason = '{"reason":"participant_in_use"}'
        socket.emit('close', 1000, Buffer.from(reason))
        expect(closed).toHaveBeenCalledTimes(1)
        expect(closed).toHaveBeenCalledWith(expect.objectContaining({
            code: 'REMOTE_CLOSE', detail: { code: 1000, reason }
        }))
        await rejected
    } finally {
        readyState.mockRestore()
        send.mockRestore()
        socket.terminate()
        c.disconnect()
        await server.shutdown()
    }
})
