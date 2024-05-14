import os
from signal import SIGKILL
from typing import Any, Callable

import psutil

_TWO_20 = float(2**20)

try:
    from multiprocessing import Pipe, Process
except ImportError:
    # from multiprocessing.dummy import Pipe
    raise


def memory_usage(proc: tuple[Callable, Any, Any], retval=False):
    """
    Return the memory usage of a process or piece of code

    Parameters
    ----------
    proc : {int, string, tuple}, optional
        The process to monitor. Is a tuple
        representing a Python function. The tuple contains three
        values (f, args, kw) and specifies to run the function
        f(*args, **kw).
        Set to -1 (default) for current process.

    retval : bool, optional
        For profiling python functions. Save the return value of the profiled
        function. Return value of memory_usage becomes a tuple:
        (mem_usage, retval)

    Returns
    -------
    mem_usage : list of floating-point values
        memory usage, in MiB. It's length is always < timeout / interval
        if max_usage is given, returns the two elements maximum memory and
        number of measurements effectuated
    ret : return value of the profiled function
        Only returned if retval is set to True
    """

    ret = -1
    max_iter = 1
    interval = 0.1

    if callable(proc):
        proc = (proc, (), {})

    if isinstance(proc, (list, tuple)):
        if len(proc) == 1:
            f, args, kw = (proc[0], (), {})
        elif len(proc) == 2:
            f, args, kw = (proc[0], proc[1], {})
        elif len(proc) == 3:
            f, args, kw = (proc[0], proc[1], proc[2])
        else:
            raise ValueError

        current_iter = 0
        while True:
            current_iter += 1
            child_conn, parent_conn = Pipe()  # this will store MemTimer's results
            p = MemTimer(os.getpid(), interval, child_conn)
            p.start()
            parent_conn.recv()  # wait until we start getting memory

            # When there is an exception in the "proc" - the (spawned) monitoring processes don't get killed.
            # Therefore, the whole process hangs indefinitely. Here, we are ensuring that the process gets killed!
            try:
                returned = f(*args, **kw)
                parent_conn.send(0)  # finish timing
                ret = parent_conn.recv()
                n_measurements = parent_conn.recv()
                # Convert the one element list produced by MemTimer to a singular value
                ret = ret[0]
                if retval:
                    ret = ret, returned
            except Exception:
                parent = psutil.Process(os.getpid())
                for child in parent.children(recursive=True):
                    os.kill(child.pid, SIGKILL)
                p.join(0)
                raise

            p.join(5 * interval)

            if (n_measurements > 4) or (current_iter == max_iter) or (interval < 1e-6):
                break
            interval /= 10.0
    else:
        raise ValueError("proc is no valid function")

    return ret


class MemTimer(Process):
    """
    Fetch memory consumption from over a time interval
    """

    def __init__(self, monitor_pid, interval, pipe, *args, **kw):
        self.monitor_pid = monitor_pid
        self.interval = interval
        self.pipe = pipe
        self.cont = True
        self.n_measurements = 1

        # get baseline memory usage
        self.mem_usage = [_get_memory(self.monitor_pid)]
        super(MemTimer, self).__init__(*args, **kw)

    def run(self):
        self.pipe.send(0)  # we're ready
        stop = False
        while True:
            cur_mem = _get_memory(self.monitor_pid)
            self.mem_usage[0] = max(cur_mem, self.mem_usage[0])
            self.n_measurements += 1
            if stop:
                break
            stop = self.pipe.poll(self.interval)
            # do one more iteration

        self.pipe.send(self.mem_usage)
        self.pipe.send(self.n_measurements)


def _get_memory(pid):
    # .. low function to get memory consumption ..
    if pid == -1:
        pid = os.getpid()

    # .. cross-platform but but requires psutil ..
    process = psutil.Process(pid)
    try:
        # avoid using get_memory_info since it does not exists
        # in psutil > 2.0 and accessing it will cause exception.
        meminfo_attr = "memory_info" if hasattr(process, "memory_info") else "get_memory_info"
        mem = getattr(process, meminfo_attr)()[0] / _TWO_20
        return mem

    except psutil.AccessDenied:
        pass
        # continue and try to get this from ps
