"""Implements the Observer/Observable pattern,"""
from __future__ import absolute_import
from __future__ import unicode_literals

import logging

from six import string_types

log = logging.getLogger(__name__)


class Observable(object):
    """An Observable in the Observer/Observable pattern. It stores
    specifications and Observers which can be notified of changes by calling
    notify.
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
        if isinstance(watch_spec, (string_types, bool)):
            self._observers.setdefault(watch_spec, []).append(observer)
            return

        for spec in watch_spec:
            self._observers.setdefault(spec, []).append(observer)

    def clear_observers(self, watch_spec=None):
        """Remove all observers for a given watch_spec. Removes all
        observers if listen_spec is None
        """
        if watch_spec is None or watch_spec is True:
            self._observers.clear()
            return

        del self._observers[watch_spec]

    def remove_observer(self, observer):
        """Remove an observer from all watch_specs."""
        for observers in self._observers.values():
            if observer in observers:
                observers.remove(observer)

    def _get_handlers_for_event(self, event):
        """Returns the complete list of handlers for the event."""
        return self._observers.get(True, []) + self._observers.get(event, [])

    def notify(self, event):
        """Notify all observers of the event."""
        log.debug("Notifying listeners for new event %r", event)
        for handler in self._get_handlers_for_event(event):
            handler.handler(self, event)


class Observer(object):
    """An observer in the Observer/Observable pattern.  Given an observable
    object will watch for notify calls.  Override handler to act on those
    notifications.
    """

    def watch(self, observable, event=True):
        """Adds this Observer as a watcher of the observable."""
        observable.attach(event, self)

    def watch_all(self, observables, event=True):
        for observable in observables:
            self.watch(observable, event)

    def handler(self, observable, event):
        """Override this method to call a method to handle events."""
        pass

    def stop_watching(self, observable):
        observable.remove_observer(self)
