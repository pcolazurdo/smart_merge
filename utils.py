from contextlib import contextmanager


@contextmanager
def __alive_bar():
    def noop_func():
        pass
    def bar():  # for definite progress mode.
        return 
    noop = noop_func
    yield noop

def alive_bar(*args, **kwargs):    
    return __alive_bar()