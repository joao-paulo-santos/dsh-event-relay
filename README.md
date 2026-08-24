# dsh-event-relay

The ONE host→browser push channel for DSH plugins: a single SSE route
(`/relay/events`) that holds every subscriber's response open and streams
JSON frames `{"topic","payload"}` as things happen.

## Producers (host plugins)

```js
const relay = ctx.get('eventRelay')            // optional — degrade if absent
relay.publish('my-topic', payload)             // direct
relay.forward('my/event')                      // tap an existing Cordis event (idempotent)
```

## Consumers (browser bundles)

```js
const relay = ctx.get('eventRelay')            // provided by this package's client half
const off = relay.subscribe('kanban', (topic, payload) => { ... })  // prefix match
// or raw: new EventSource('/relay/events?topics=kanban,notifications')
```

## Design

Transport, not a bus: Cordis events remain the host-side event system; this
only carries them across the plane boundary. Liveness, not correctness —
consumers should fall back to pull-on-focus when the relay is absent.
Route roots are composition-level contracts (`/api`, `/plugins`,
`/workspace-history`, `/notifications`, `/granular-settings` are taken).
