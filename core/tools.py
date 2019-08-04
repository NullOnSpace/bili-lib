import requests
from time import time
from functools import wraps
import threading


def get_url(url, params={}, cookies={}, max_tries=5, timeout=5):
    while True:
        try:
            res = requests.get(url, params=params,
                               cookies=cookies, timeout=timeout)
            res.raise_for_status()
        except requests.RequestException as e:
            with open('requests_errorlog', 'a') as fd:
                fd.write('|'.join((str(max_tries), str(e)))+'\n')
            max_tries -= 1
            if max_tries == 0:
                raise
        else:
            break
    return res


class Timer:
    fn_dict = dict()
    enclose_stack = []
    subthd_dict = dict()

    @classmethod
    def print_timer(cls, main_only=False):
        sub_dict = dict()
        for fn_dict, _ in cls.subthd_dict.values():
            for fn_name, (fn_count, fn_time) in fn_dict.items():
                if fn_name in sub_dict:
                    sub_dict[fn_name][0] += fn_count
                    sub_dict[fn_name][1] += fn_time
                else:
                    sub_dict[fn_name] = [fn_count, fn_time]
        if sub_dict and not main_only:
            len_fn_name = len(max(sub_dict.keys(), key=len))+1
            len_fn_count = len(max(map(
                lambda x: str(x[0]), sub_dict.values()),
                key=len))
            len_fn_time = len('%.1f' % max(map(
                lambda x: x[1], sub_dict.values()),
                ))+2
            print('in sub threads:')
            for fn_name, (fn_count, fn_time) in sub_dict.items():
                print('%-{}s run %{}s times cost totally %{}.1f sec'.format(
                    len_fn_name, len_fn_count, len_fn_time
                    ) % (fn_name, fn_count, fn_time)
                )
            print('in total %.2f' % sum(map(
                lambda x: x[1], sub_dict.values()
            )))
        print('in main thread:')
        len_fn_name = len(max(cls.fn_dict.keys(), key=len))+1
        len_fn_count = len(max(map(
            lambda x: str(x[0]), cls.fn_dict.values()),
            key=len))
        len_fn_time = len('%.1f' % max(map(
            lambda x: x[1], cls.fn_dict.values()),
            ))+2
        for fn_name, (fn_count, fn_time) in cls.fn_dict.items():
            print('%-{}s run %{}s times cost totally %{}.1f sec'.format(
                len_fn_name, len_fn_count, len_fn_time
                ) % (fn_name, fn_count, fn_time)
            )
        print('in total %.2f' % sum(map(
            lambda x: x[1], cls.fn_dict.values()
        )))

    @classmethod
    def pre_exec(cls, thd_id, fn_name):
        if thd_id:
            if thd_id not in cls.subthd_dict:
                # thd_id: [fn_dict, enclose_stack]
                cls.subthd_dict[thd_id] = [dict(), list()]
            thd_seq = cls.subthd_dict[thd_id]
            fn_dict = thd_seq[0]
            enclose_stack = thd_seq[1]
        else:
            enclose_stack = cls.enclose_stack
            fn_dict = cls.fn_dict
        if fn_name not in fn_dict:
            # fn_dict{fn_name: [fn_count , fn_time],}
            fn_dict[fn_name] = [0, 0]
        enclose_stack.append(0)


    @classmethod
    def post_exec(cls, thd_id, fn_name, total_time):
        if thd_id:
            if thd_id not in cls.subthd_dict:
                # in case clear Timer before thd ending
                return
            thd_seq = cls.subthd_dict[thd_id]
            fn_seq = thd_seq[0][fn_name]
            enclose_stack = thd_seq[1]
        else:
            fn_seq = cls.fn_dict[fn_name]
            enclose_stack = cls.enclose_stack
        # fn_seq[fn_count, fn_time]
        fn_seq[1] += total_time - enclose_stack.pop()
        fn_seq[0] += 1
        if enclose_stack:
            enclose_stack[-1] += total_time

    @classmethod
    def clear(cls):
        cls.fn_dict.clear()
        cls.enclose_stack.clear()
        cls.subthd_dict.clear()


def timeit(fn):
    """
    Decorator that reports the execution time.
    """
    @wraps(fn)
    def wrapper(*args, **kwargs):
        st_time = time()
        thd_id = (
            0 if threading.current_thread() is threading.main_thread()
            else threading.current_thread().ident
        )
        Timer.pre_exec(thd_id, fn.__qualname__)
        ret = fn(*args, **kwargs)
        total_time = time()-st_time
        Timer.post_exec(thd_id, fn.__qualname__, total_time)
        return ret
    return wrapper
