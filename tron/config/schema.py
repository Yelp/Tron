"""
 Immutable config schema objects.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import namedtuple

MASTER_NAMESPACE = "MASTER"

CLEANUP_ACTION_NAME = 'cleanup'


def config_object_factory(name, required=None, optional=None):
    """
    Creates a namedtuple which has two additional attributes:
        required_keys:
            all keys required to be set on this configuration object
        optional keys:
            optional keys for this configuration object

    The tuple is created from required + optional
    """
    required = required or []
    optional = optional or []

    config_class = namedtuple(name, required + optional)

    # make last len(optional) args actually optional
    config_class.__new__.__defaults__ = (None, ) * len(optional)
    config_class.required_keys = required
    config_class.optional_keys = optional

    return config_class
