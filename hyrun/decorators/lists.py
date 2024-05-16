from functools import wraps


def list_exec(func):
    """Decorate to execute a function on a list of arguments."""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        """Execute function on list."""
        if isinstance(args[0], list):
            return [func(self, arg, *args[1:], **kwargs) for arg in args[0]]
        else:
            return func(self, *args, **kwargs)
    return wrapper


def force_list(func):
    """Decorate to force a function to accept a list as the first argument."""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        """Force list."""
        if not isinstance(args[0], list):
            return func(self, [args[0]], *args[1:], **kwargs)
        else:
            return func(self, *args, **kwargs)
    return wrapper
