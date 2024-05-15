from functools import wraps

def list_exec(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if isinstance(args[0], list):
            return [func(self, arg, *args[1:], **kwargs) for arg in args[0]]
        else:
            return func(self, *args, **kwargs)
    return wrapper


def force_list(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        if not isinstance(args[0], list):
            return func(self, [args[0]], *args[1:], **kwargs)
        else:
            return func(self, *args, **kwargs)
    return wrapper