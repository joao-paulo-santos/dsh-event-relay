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
 *
 * Debugging: the wire can be listened to from a terminal, very useful to
 * verify publishes/forwards end-to-end without a browser —
 *   curl -N 'http://127.0.0.1:3080/relay/events'
 * (add ?topics=<prefixes> to mirror the client's server-side filter, e.g.
 * ?topics=kanban; -N is required so curl streams instead of buffering).
 */
window.__ModuleLoader__.load({
  id: 'dsh-event-relay', factory: (require) => {
    var module = { exports: {} }; var exports = module.exports;

    const listenersByTopic = new Map()
    let connection = null

    const addListener = (topic, callback) => {
      let currentTopicListeners = listenersByTopic.get(topic)
      if (currentTopicListeners === undefined) {
        currentTopicListeners = new Set()
        listenersByTopic.set(topic, currentTopicListeners)
      }
      currentTopicListeners.add(callback)
    }

    const removeListener = (topic, callback) => {
      const currentTopicListeners = listenersByTopic.get(topic)
      if (currentTopicListeners === undefined) return
      currentTopicListeners.delete(callback)
      if (currentTopicListeners.size === 0) listenersByTopic.delete(topic)
    }

    const topicsWithListeners = () => [...listenersByTopic.keys()]

    const hasTopic = (topic) => listenersByTopic.has(topic)

    const hasAnyListeners = () => listenersByTopic.size > 0

    const dispatch = (frame) => {
      if (frame === null || typeof frame !== 'object' || typeof frame.topic !== 'string') return
      for (const [topic, listeners] of listenersByTopic) {
        if (topic === '' || frame.topic === topic || frame.topic.startsWith(topic + '/')) {
          for (const listener of listeners) {
            try { listener(frame.topic, frame.payload) } catch (e) { /* consumer errors are contained */ }
          }
        }
      }
    }

    const getWireUrl = (topics) => {
      if (topics.includes('')) return '/relay/events'
      const joined = topics.join(',')
      return joined === '' ? '/relay/events' : '/relay/events?topics=' + encodeURIComponent(joined)
    }

    const openConnection = (connectionUrl) => {
      try { if (connection !== null) connection.close() } catch (e) { }
      try {
        connection = new EventSource(connectionUrl)
        connection.onopen = () => { dispatch({ topic: '__relay/open', payload: null }) }
        connection.onmessage = (ev) => {
          try { dispatch(JSON.parse(ev.data)) } catch (e) { /* malformed frame: skip */ }
        }
      } catch (e) { connection = null }
    }

    const syncConnectionTopics = () => {
      if (!hasAnyListeners()) {
        try { if (connection !== null) connection.close() } catch (e) { }
        return
      }
      openConnection(getWireUrl(topicsWithListeners()))

    }

    const api = {
      /** Subscribe to a topic prefix. Returns an unsubscribe function. */
      subscribe(topic, callback) {
        if (typeof callback !== 'function') throw new Error('eventRelay.subscribe(topic, callback): callback function required')
        const sanitizedTopic = String(topic || '')
        const isNewTopic = !hasTopic(sanitizedTopic)
        addListener(sanitizedTopic, callback)
        if (isNewTopic || connection === null) syncConnectionTopics()

        return () => {
          removeListener(sanitizedTopic, callback)
          if (!hasTopic(sanitizedTopic)) syncConnectionTopics()
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
