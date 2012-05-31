"""Iteration utility functions."""


def build_filtered_func(func):
    def filtered_func(seq):
        """Filter out Nones and return the return of func."""
        if not seq:
            return None
        seq = filter(None, seq)
        if not seq:
            return None
        return func(seq)
    return filtered_func

min_filter = build_filtered_func(min)
max_filter = build_filtered_func(max)
