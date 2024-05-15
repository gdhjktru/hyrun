from functools import wraps

def list_exec(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if isinstance(args[0], list):
            return [func(arg, *args[1:], **kwargs) for arg in args[0]]
        else:
            return func(*args, **kwargs)
    return wrapper



def force_list(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if not isinstance(args[0], list):
            return [args[0]]
        else:
            return func(*args, **kwargs)
    return wrapper