/**
 * dsh-event-relay — browser half (hand-authored client bundle).
 *
 * Provides the client-plane `eventRelay` service: ONE shared EventSource for
 * the whole page, fan-out to topic subscribers (prefix match — "kanban"
 * matches "kanban/change"). Consumer bundles read it with
 * ctx.get('eventRelay') and never open their own stream.
 *
 * Wire-level least privilege (0.2): the stream is opened with ?topics= set to
 * the UNION of subscribed prefixes, and re-opened when a new prefix appears —
 * the server then only sends frames matching what this page actually listens
 * for, instead of every frame for the whole process.
 *
 * Reconnect signal (0.2): whenever the stream (re)opens — first connect or
 * EventSource auto-reconnect after a drop — subscribers of the synthetic
 * topic prefix '__relay' receive '__relay/open'. Consumers that apply frames
 * directly (instead of refetching on every change) use it to resynchronize,
 * bounding any staleness from frames missed while the stream was down.
 */
window.__ModuleLoader__.load({
  id: 'dsh-event-relay', factory: (require) => {
    var module = { exports: {} }; var exports = module.exports;

    const localSubscribers = []
    const subscribedTopics = new Set()
    let connection = null

    const dispatch = (frame) => {
      if (frame === null || typeof frame !== 'object' || typeof frame.topic !== 'string') return
      for (const entry of localSubscribers) {
        if (entry.prefix === '' || frame.topic === entry.prefix || frame.topic.startsWith(entry.prefix + '/')) {
          try { entry.cb(frame.topic, frame.payload) } catch (e) { /* consumer errors are contained */ }
        }
      }
    }

    const openConnection = (topics) => {
      try { if (connection !== null) connection.close() } catch (e) { }
      const q = topics === '' ? '' : '?topics=' + encodeURIComponent(topics)
      try {
        connection = new EventSource('/relay/events' + q)
        connection.onopen = () => { dispatch({ topic: '__relay/open', payload: null }) }
        connection.onmessage = (ev) => {
          try { dispatch(JSON.parse(ev.data)) } catch (e) { /* malformed frame: skip */ }
        }
      } catch (e) { connection = null }
    }

    const api = {
      /** Subscribe to a topic prefix. Returns an unsubscribe function. */
      subscribe(prefix, callback) {
        if (typeof callback !== 'function') throw new Error('eventRelay.subscribe(prefix, callback): callback function required')
        const p = String(prefix || '')
        const isNewPrefix = !subscribedTopics.has(p)
        if (isNewPrefix) subscribedTopics.add(p)
        if (connection === null || isNewPrefix) openConnection([...subscribedTopics].join(','))  // first open, or widen the wire filter
        const entry = { prefix: p, cb: callback }
        localSubscribers.push(entry)
        return () => {
          const i = localSubscribers.indexOf(entry)
          if (i >= 0) localSubscribers.splice(i, 1)
        }
      },
    }

    module.exports = {
      name: 'event-relay-client',
      inject: [],
      apply(ctx) {
        ctx.provide('eventRelay', api)
      },
    }
    return module.exports
  }
})
