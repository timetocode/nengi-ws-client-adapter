# nengi-ws-client-adapter

Node.js client adapter for nengi using `ws` and the `nengi-buffers` binary
backend. Use it for maintained bots, stress clients, command-line tools, and
service-to-service Nengi connections.

Keep the complete Nengi package family on one exact version:

```sh
npm install nengi@2.0.0-rc.126 \
    nengi-ws-client-adapter@2.0.0-rc.126 \
    nengi-buffers@2.0.0-rc.126
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
traffic; the adapter sends Pong-only packets independently of application flush
cadence.

Import only from package roots. See the
[nengi manual](https://github.com/timetocode/nengi/tree/rc/2.0.0/docs/ai) for
bot workloads, timing, and adapter guidance.
