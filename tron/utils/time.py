"""Time utilites"""
import datetime

# Global time override
_current_time_override = None

def override_current_time(current_time):
    """Interface for overriding the current time
    
    This is a test hook for forcing the app to think it's a specific time.
    """
    global _current_time_override
    _current_time_override = current_time

def current_time():
    """Standard interface for generating the current time."""
    if _current_time_override is not None:
        return _current_time_override
    else:
        return datetime.datetime.now()
        
