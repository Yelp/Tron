# -*- coding: utf-8 -*-

import os
import sys

cmd_folder = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.split(cmd_folder)[0])

from conf import *

master_doc = 'index'

project = u'trond'

# One entry per manual page. List of tuples
# (source start file, name, description, authors, manual section).
man_pages = [
    ('index', 'trond', u'trond documentation',
     [u'Yelp, Inc.'], 8)
]
