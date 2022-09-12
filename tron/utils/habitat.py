from functools import lru_cache


@lru_cache(maxsize=1)
def get_region() -> str:
    """
    Discover what region we're running in by reading this information from on-disk facts.

    Yelpers: for more information, see y/habitat
    """
    with open("/nail/etc/region") as f:
        return f.read().strip()


@lru_cache(maxsize=1)
def get_superregion() -> str:
    """
    Discover what region we're running in by reading this information from on-disk facts.

    Yelpers: for more information, see y/habitat
    """
    with open("/nail/etc/superregion") as f:
        return f.read().strip()


@lru_cache(maxsize=1)
def get_ecosystem() -> str:
    """
    Discover what ecosystem we're running in by reading this information from on-disk facts.

    Yelpers: for more information, see y/habitat
    """
    with open("/nail/etc/ecosystem") as f:
        return f.read().strip()
