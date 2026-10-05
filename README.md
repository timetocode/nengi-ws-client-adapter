# nengi-ws-client-adapter

Node.js client adapter for nengi using `ws` and the `nengi-buffers` binary
backend. Use it for maintained bots, stress clients, command-line tools, and
service-to-service Nengi connections.

This package is independently versioned. Its `peerDependencies.nengi` declares
compatible core releases. The rc.128 contract baseline installs as:

```sh
npm install nengi@2.0.0-rc.128 \
    nengi-ws-client-adapter@2.0.0-rc.128 \
    nengi-buffers@2.0.0-rc.128
```

```ts
import { Client } from 'nengi'
import { WsClientAdapter } from 'nengi-ws-client-adapter'

const client = new Client(context, WsClientAdapter, 20)
await client.connect('ws://localhost:8079', { role: 'bot' })
```

For seeded live latency, jitter, and periodic stalls, use the dedicated
simulated adapter:

```ts
import { SimulatedWsClientAdapter } from 'nengi-ws-client-adapter'

const client = new Client(context, SimulatedWsClientAdapter, 20, {
    conditions: {
        seed: 7,
        clientToServer: { latencyMs: 50, jitterMs: 10 },
        serverToClient: { latencyMs: 90, jitterMs: 20 }
    }
})
```

The simulated adapter conditions the handshake and all later Nengi payloads
while preserving WebSocket ordering. Use the ordinary adapter when simulation
is not required.

Drain frames, perform meaningful commands or requests, and call
`client.flush()` at the intended client cadence. Ping/Pong responses are engine
traffic; core sends Pong-only packets independently of application flush
cadence.

Connection lifecycle is owned by core. `connect()` resolves with optional JSON setup data after
nengi acceptance; initial failures reject `ClientConnectionError`. Register
`client.setDisconnectHandler(info => ...)` for an established session's terminal
outcome and read `client.connectionState` for gameplay state. The separate
WebSocket-error callback and `adapter.connected` are removed.

Core defaults to a 10-second complete connection budget and a 30-second valid
receive budget. Configure `connectTimeoutMs` and `serverTimeoutMs` in the fifth
Client constructor argument, using `null` to disable explicitly. The optional
third connect argument accepts `{ signal }` for attempt cancellation. Failed
initial attempts may retry on the same Client; ended sessions need a fresh one.
`disconnect(detail?)` cleans up synchronously even if physical closure takes time.
Only a safe standard reason is sent to the peer; detail remains local.

A flush or delayed simulated send that races a native WebSocket close preserves
the pending close event's reason. Outgoing data is discarded once the socket is
closing; outstanding requests still reject on disconnection. Real send failures
remain transport errors, and core's configured deadlines still apply.

This adapter implements client transport contract version 2: its constructor
takes configuration only, and core calls `open`, `send`, and `close`. Update core
and client adapters together when migrating to rc.128. The transport contract
itself does not change the server wire format. See the nengi manual's migration guide.

Import only from package roots. See the
[nengi manual](https://github.com/timetocode/nengi/tree/rc/2.0.0/docs/ai) for
bot workloads, timing, and adapter guidance.

Transport stats expose `messagesReceived`, `bytesReceived`, and `bytesSent`.
The message count includes handshake traffic; it replaces `snapshotsReceived`.
