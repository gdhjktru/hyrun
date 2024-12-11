from functools import wraps


def force_list(func):
    """Decorate to force a function to accept a list as the first argument."""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        """Force list."""
        if not args:
            return func(self, *args, **kwargs)
        if not isinstance(args[0], list):
            return func(self, [args[0]], *args[1:], **kwargs)
        else:
            return func(self, *args, **kwargs)
    return wrapper
