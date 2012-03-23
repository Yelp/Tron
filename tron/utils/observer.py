
import logging

log = logging.getLogger('tron.utils.observer')

class Observable(object):
    """This class is an Observable in the Observer/Observable pattern. It stores
    specifications and callbacks which can be triggered by calling notify.
    """

    def __init__(self):
        self._listeners = dict()

    def listen(self, listen_spec, callback):
        """Attach another callback to the listen_spec.

        Listener Spec matches on:
            True                    Matches everything
            <string>                Matches only that event
            <sequence of strings>   Matches any of the events in the sequence
        """
        if isinstance(listen_spec, (basestring, bool)):
            self._listeners.setdefault(listen_spec, []).append(callback)
            return

        for spec in listen_spec:
            self._listeners.setdefault(spec, []).append(callback)

    def clear_listeners(self, listen_spec=None):
        """Remove all listeners for a given listen_spec. Removes all
        listeners if listen_spec is None.
        """
        if listen_spec is None or listen_spec is True:
            self._listeners.clear()
            return

        del self._listeners[listen_spec]

    def notify(self, event):
        """Notify all observers of the event."""
        log.debug("Notifying listeners for new event %r", event)

        listeners = self._listeners.get(True, []) + self._listeners.get(event, [])

        for listener in listeners:
            listener()