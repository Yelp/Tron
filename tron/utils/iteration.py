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


def list_all(seq):
    """Create a list from the sequence then evaluate all the entries using
    all(). This differs from the built-in all() which will short circuit
    on the first False.
    """
    return all(list(seq))