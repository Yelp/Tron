#!/usr/bin/env python3.6
"""
Write pid and stdout/stderr to a standard location before execing a command.
"""
import sys

from tron.bin.action_runner import main

sys.exit(main())
