# dsh-event-relay

The ONE host→browser push channel for DSH plugins: a single SSE route
(`/relay/events`) that holds every subscriber's response open and streams
JSON frames `{"topic","payload"}` as things happen.

## Producers (host plugins)

```js
const relay = ctx.get('eventRelay')            // optional — degrade if absent
relay.publish('my-topic', payload)             // direct
ctx.on('my/event', (data) => relay.publish('my/event', data))   // mirror a Cordis event (do this in your plugin)
```

## Consumers (browser bundles)

```js
const relay = ctx.get('eventRelay')            // provided by this package's client half
const off = relay.subscribe('kanban', (topic, payload) => { ... })  // payload optional
// or raw: new EventSource('/relay/events?topics=kanban,notifications')
```

`payload` in the callback is whatever the publisher passed. Apply it directly
or ignore it and refetch your own truth — both are first-class styles (below).

## Design

Transport, not a bus: Cordis events remain the host-side event system; this
only carries them across the plane boundary.

Payload is optional, not doorbell-only. Some plugins use the relay purely as a
change signal — a topic-only frame that tells them to refetch their own truth.
Others consume the payload directly: the notifications demo pushes the
notification itself, a kanban board pushes change frames. `publish(topic,
payload)` — send whatever is useful; doorbells are one usage style, not the
contract.

Liveness, not correctness: the relay may be absent, and a consumer should
fall back to pull-on-focus (or pull on `__relay/open`) rather than depend on
the stream for state.
Route roots are composition-level contracts (`/api`, `/plugins`,
`/workspace-history`, `/notifications`, `/granular-settings` are taken).
