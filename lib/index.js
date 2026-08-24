/**
 * dsh-event-relay — host half.
 *
 * The ONE host→browser push channel: a single SSE route (/relay/events) that
 * holds every subscriber's response open and streams JSON frames
 *   data: {"topic":"<name>","payload":<json>}\n\n
 * as things happen. Host plugins reach it two ways:
 *
 *   ctx.get('eventRelay').publish(topic, payload)   — direct publish
 *   ctx.get('eventRelay').forward('my/event')       — tap an existing Cordis
 *     event: the relay adds ctx.on('my/event') and forwards its args (a
 *     single-argument event forwards that argument; multi-arg forwards an
 *     array). Idempotent.
 *
 * Browser bundles subscribe either with the standard API —
 *   new EventSource('/relay/events?topics=kanban,notifications')
 * (prefix matching: "kanban" matches "kanban/change") — or by injecting the
 * client service this package's browser half provides:
 *   ctx.get('eventRelay').subscribe(prefix, cb)  → unsubscribe fn.
 *
 * Design notes:
 * - This is TRANSPORT, not a bus: Cordis events stay the host-side event
 *   system; this relay only carries them across the plane boundary.
 * - Liveness, not correctness: if the relay is absent, consumers fall back to
 *   pull-on-focus; nothing depends on it for state.
 * - Route paths are composition-level contracts (taken today: /api, /plugins,
 *   /workspace-history, /notifications, /granular-settings).
 */

export const name = 'event-relay'

export const inject = ['webServer']

export function apply(ctx) {
  const webServer = ctx.webServer

  const clients = new Set()
  const forwarded = new Set()

  const writeFrame = (client, topic, payload) => {
    try {
      client.res.write('data: ' + JSON.stringify({ topic: topic, payload: payload === undefined ? null : payload }) + '\n\n')
    } catch (e) { /* a dead socket dies quietly; close handler cleans it up */ }
  }

  const broadcast = (topic, payload) => {
    for (const client of clients) {
      if (client.matches(topic)) writeFrame(client, topic, payload)
    }
  }

  /** The host-facing relay service. Producers never need it (they may only
   *  use forward() once at boot); it exists for direct publishers. */
  const relay = {
    publish(topic, payload) {
      if (typeof topic !== 'string' || topic === '') throw new Error('eventRelay.publish: topic string required')
      broadcast(topic, payload)
    },
    forward(eventName) {
      if (typeof eventName !== 'string' || eventName === '') throw new Error('eventRelay.forward: event name required')
      if (forwarded.has(eventName)) return
      forwarded.add(eventName)
      ctx.on(eventName, (...args) => {
        broadcast(eventName, args.length === 1 ? args[0] : args)
      })
    },
  }
  ctx.provide('eventRelay', relay)

  const disposers = []
  disposers.push(webServer.register({
    kind: 'exact',
    path: '/relay/events',
    handler: (req, res) => {
      const url = new URL(req.url, 'http://localhost')
      const topicsParam = url.searchParams.get('topics')
      const prefixes = topicsParam === null
        ? []
        : topicsParam.split(',').map((s) => s.trim()).filter((s) => s !== '')
      const client = {
        res,
        matches: (topic) => prefixes.length === 0
          || prefixes.some((p) => topic === p || topic.startsWith(p + '/')),
      }
      clients.add(client)
      res.writeHead(200, {
        'content-type': 'text/event-stream; charset=utf-8',
        'cache-control': 'no-store',
        'connection': 'keep-alive',
      })
      res.write(': connected\n\n')
      // Comment pings keep proxies from idling the stream out.
      const ping = setInterval(() => {
        try { res.write(': ping\n\n') } catch (e) { /* cleaned by close */ }
      }, 20000)
      client.ping = ping
      req.on('close', () => {
        clearInterval(ping)
        clients.delete(client)
      })
    },
  }))

  // Plugin teardown: end every held-open response and clear its ping (the
  // route disposer above runs first and removes the route itself).
  disposers.push(() => {
    for (const client of clients) {
      if (client.ping !== undefined) { try { clearInterval(client.ping) } catch (e) {} }
      try { client.res.end() } catch (e) {}
    }
    clients.clear()
  })

  return () => { for (const d of disposers) { try { d() } catch (e) {} } }
}
