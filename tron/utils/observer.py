
import logging

log = logging.getLogger('tron.utils.observer')

class Observable(object):
    """This class is an Observable in the Observer/Observable pattern. It stores
    specifications and callbacks which can be triggered by calling notify.
    """

    def __init__(self):
        self._observers = dict()

    def attach(self, watch_spec, observer):
        """Attach another observer to the listen_spec.

        Listener Spec matches on:
            True                    Matches everything
            <string>                Matches only that event
            <sequence of strings>   Matches any of the events in the sequence
        """
        if isinstance(watch_spec, (basestring, bool)):
            self._observers.setdefault(watch_spec, []).append(observer)
            return

        for spec in watch_spec:
            self._observers.setdefault(spec, []).append(observer)

    def clear_watchers(self, watch_spec=None):
        """Remove all listeners for a given listen_spec. Removes all
        listeners if listen_spec is None.
        """
        if watch_spec is None or watch_spec is True:
            self._observers.clear()
            return

        del self._observers[watch_spec]

    def _get_watchers_for_event(self, event):
        """Returns the complete list of watchers for the event."""
        return self._observers.get(True, []) + self._observers.get(event, [])

    def notify(self, event):
        """Notify all observers of the event."""
        log.debug("Notifying listeners for new event %r", event)
        for watcher in self._get_watchers_for_event(event):
            watcher.watcher(self, event)


class Observer(object):
    """An observer in the Observer/Observable pattern.  Given an observable
    object will watch for notify calls.
    """

    def watch(self, observable, event=True):
        observable.attach(event, self)

    def watcher(self, observable, event):
        """Override this method to call a method to handle an event."""
        pass


# TODO: delete or implement
class CollectionObserver(object):
    """This Observer will watch for events from a collection of observables
    and allows callbacks to be registered for each event.
    """
    pass
