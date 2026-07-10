# nengi-ws-client-adapter

Node.js client adapter for nengi using `ws` and the `nengi-buffers` binary
backend. Use it for maintained bots, stress clients, command-line tools, and
service-to-service Nengi connections.

Keep the complete Nengi package family on one exact version:

```sh
npm install nengi@2.0.0-rc.125 \
    nengi-ws-client-adapter@2.0.0-rc.125 \
    nengi-buffers@2.0.0-rc.125
```

```ts
import { Client } from 'nengi'
import { WsClientAdapter } from 'nengi-ws-client-adapter'

const client = new Client(context, WsClientAdapter, 20)
await client.connect('ws://localhost:8079', { role: 'bot' })
```

Drain frames, perform meaningful commands or requests, and call
`client.flush()` at the intended client cadence. Ping/Pong responses are engine
traffic and are emitted during that flush boundary.

Import only from package roots. See the
[nengi manual](https://github.com/timetocode/nengi/tree/rc/2.0.0/docs/ai) for
bot workloads, timing, and adapter guidance.
