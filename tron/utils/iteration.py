"""Iteration utility functions."""
from __future__ import absolute_import
from __future__ import unicode_literals


def build_filtered_func(func):
    def filtered_func(seq):
        """Filter out Nones and return the return of func."""
        if not seq:
            return None
        seq = list(filter(None, seq))
        if len(seq) == 0:
            return None
        return func(seq)

    return filtered_func


min_filter = build_filtered_func(min)
max_filter = build_filtered_func(max)


def list_all(seq):
    """Create a list from the sequence then evaluate all the entries using
    all(). This differs from the built-in all() which will short circuit
    on the first False.
    """
    return all(list(seq))
