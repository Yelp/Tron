"""Time utilites"""
import datetime
import time

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

def current_timestamp():
    return to_timestamp(current_time())
        
def to_timestamp(time_val):
    """Generate a unix timestamp for the given datetime instance"""
    return time.mktime(time_val.timetuple())