"""Implements the Observer/Observable pattern,"""
import logging
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import Sequence
from typing import Union

log = logging.getLogger(__name__)


class Observable:
    """An Observable in the Observer/Observable pattern. It stores
    specifications and Observers which can be notified of changes by calling
    notify.
    """

    def __init__(self) -> None:
        self._observers: Dict[Union[str, bool], List["Observer"]] = {}

    def attach(self, watch_spec: Union[str, bool, Sequence[str]], observer: "Observer") -> None:
        """Attach another observer to the listen_spec.

        Listener Spec matches on:
            True                    Matches everything
            <string>                Matches only that event
            <sequence of strings>   Matches any of the events in the sequence
        """
        if isinstance(watch_spec, (str, bool)):
            self._observers.setdefault(watch_spec, []).append(observer)
            return

        for spec in watch_spec:
            self._observers.setdefault(spec, []).append(observer)

    def clear_observers(self, watch_spec: Optional[Union[str, bool]] = None) -> None:
        """Remove all observers for a given watch_spec. Removes all
        observers if listen_spec is None
        """
        if watch_spec is None or watch_spec is True:
            self._observers.clear()
            return

        del self._observers[watch_spec]

    def remove_observer(self, observer: "Observer") -> None:
        """Remove an observer from all watch_specs."""
        for observers in self._observers.values():
            if observer in observers:
                observers.remove(observer)

    def _get_handlers_for_event(self, event: str) -> List["Observer"]:
        """Returns the complete list of handlers for the event."""
        return self._observers.get(True, []) + self._observers.get(event, [])

    def notify(self, event: str, event_data: Any = None) -> None:
        """Notify all observers of the event."""
        handlers = self._get_handlers_for_event(event)
        log.debug(
            f"Notifying {len(handlers)} listeners for new event {event!r}",
        )
        for handler in handlers:
            handler.handler(self, event, event_data)


class Observer:
    """An observer in the Observer/Observable pattern.  Given an observable
    object will watch for notify calls.  Override handler to act on those
    notifications.
    """

    def watch(self, observable: "Observable", event: Union[str, bool, Sequence[str]] = True) -> None:
        """Adds this Observer as a watcher of the observable."""
        observable.attach(event, self)

    def watch_all(self, observables: Iterable["Observable"], event: Union[str, bool, Sequence[str]] = True) -> None:
        for observable in observables:
            self.watch(observable, event)

    def handler(self, observable: "Observable", event: str, event_data: Any) -> None:
        """Override this method to call a method to handle events."""
        pass

    def stop_watching(self, observable: "Observable") -> None:
        observable.remove_observer(self)
