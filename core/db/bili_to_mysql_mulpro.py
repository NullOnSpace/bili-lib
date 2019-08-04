import threading
from functools import reduce
import os
import json
from bs4 import BeautifulSoup as bs
from multiprocessing import Process, Lock, BoundedSemaphore, Queue, RLock
from queue import Empty

from core.tools import Timer, timeit
from core.db.bili_table import (
    UserTable, AVTable, PTable, DMContentTable, DMTable, AVPM2MTable,
    DMSpeContentTable)
from conf import settings


class BiliManager:

    def __init__(self, q, avp_lock, content_lock, dm_lock, user_lock):
        self.p_dict = dict()  # aid: (oid, page)
        self.p_to_insert_list = list()
        self.av_dict = dict()  # aid: record
        self.user_to_insert_set = set()
        self.user_fetched_dict = dict()  # uid: id
        self.dm_dict = dict()  # dmid: record
        self.dm_content_to_insert_set = set()
        self.p_fetched_dict = dict()  # oid: id
        self.dm_content_fetched_dict = dict()  # content: id
        self.uid_set = set()
        self.content_set = set()
        self.avp_lock = avp_lock
        self.dm_lock = dm_lock
        self.user_lock = user_lock
        self.content_lock = content_lock
        self.av_q = q
        self.dm_limit = 200000

    @timeit
    def handle_av(self, aid):
        with self.avp_lock:
            res = AVTable.is_aid_exists(aid)
        if not res:
            with open(os.path.join(settings.DM_FILE, 'avs', aid)) as fd:
                av_json = json.load(fd)
            try:
                data = av_json['data']
                stat = data['stat']
                pages = data['pages']
            except KeyError:
                return
            record = (
                aid, len(data['pages']), stat['favorite'], stat['view'],
                stat['coin'], stat['share'], stat['like'], stat['dislike'],
                stat['reply'], data['ctime'], data['pubdate'],
                data['copyright'], data['title'], data['desc'],
            )
            self.av_dict[aid] = record
            with self.avp_lock:
                self.handle_ps(aid, data['pages'])

    @timeit
    def handle_ps(self, aid, p_list):
        records = list()
        if aid not in self.p_dict:
            self.p_dict[aid] = list()
        for p in p_list:
            oid = p['cid']
            self.p_dict[aid].append((oid, p['page']))
            if oid in self.p_fetched_dict:
                res = self.p_fetched_dict[oid]
            else:
                res = PTable.is_oid_exists(oid)
            if not res:
                record = [
                    oid,  p['duration'], p['part']
                ]
                self.handle_dm(oid)
                self.p_to_insert_list.append(record)
            else:
                self.p_fetched_dict[str(oid)] = res

    @timeit
    def handle_dm(self, oid):
        with open(os.path.join(settings.DM_FILE, 'dms', str(oid))) as fd:
            soup = bs(fd.read(), 'xml')
        d_list = soup.find_all('d')
        for d in d_list:
            attr = d['p'].split(',')
            time_in_video = float(attr[0])
            dm_position = int(attr[1])
            font_size = int(attr[2])
            font_color = int(attr[3])
            time_in_world = int(attr[4])
            dm_type = int(attr[5])
            uid = attr[6]
            dmid = int(attr[7])
            content = str(d.string).strip()
            self.dm_dict[dmid] = [
                time_in_video, dm_position, font_size, font_color,
                time_in_world, dm_type, uid, dmid, content, oid, 0
            ]
            self.uid_set.add(uid)
            if dm_position in (7, 8):  # special dm
                self.dm_dict[dmid][8] = 0  # content
                # img Blob or json
                with self.avp_lock:
                    self.dm_dict[dmid][-1] = \
                    DMSpeContentTable.insert_all_and_get_id((content,))
            else:
                self.content_set.add(content)

    @timeit
    def handle_contents(self):
        tprint('starting to handle contents...')
        self.dm_content_fetched_dict = dict.fromkeys(self.content_set)
        for content in self.content_set:
            self.handle_content(content)
        tprint('contents handled!')
        self.flush_content()

    @timeit
    def handle_content(self, content):
        with self.content_lock:
            res = DMContentTable.is_content_exists(content)
        if res:
            self.dm_content_fetched_dict[content] = res
        else:
            self.dm_content_to_insert_set.add(content)

    @timeit
    def handle_users(self):
        tprint('starting to handle users...')
        self.user_fetched_dict = dict.fromkeys(self.uid_set)
        for uid in self.uid_set:
            self.handle_user(uid)
        tprint('users handled!')
        self.flush_user()

    @timeit
    def handle_user(self, uid):
        with self.user_lock:
            res = UserTable.is_uid_exists(uid)
        if res:
            self.user_fetched_dict[uid] = res
        else:
            self.user_to_insert_set.add(uid)

    @timeit
    def flush_av_p(self):
        tprint('thread for av p av_p starts...')
        av_res_dict = dict()
        with self.avp_lock:
            AVTable.insert_many_all(tuple(self.av_dict.values()))
            av_res = AVTable.fetch_many_id_by_aid(tuple(self.av_dict.keys()))
        for id_aid_dict in av_res:
            av_res_dict[str(id_aid_dict['aid'])] = id_aid_dict['id']
        with self.avp_lock:
            PTable.insert_many_all(self.p_to_insert_list)
            p_rs = PTable.fetch_many_id_by_oid(
                    r[0] for r in self.p_to_insert_list)
        p_res_dict = dict()
        for id_oid_dict in p_rs:
            p_res_dict[str(id_oid_dict['oid'])] = id_oid_dict['id']
        self.p_fetched_dict.update(p_res_dict)
        av_p_m2m_list = list()
        for aid, records in self.p_dict.items():
            for oid, page in records:
                oid = str(oid)
                record = (av_res_dict[aid], self.p_fetched_dict[oid], page)
                av_p_m2m_list.append(record)
        with self.avp_lock:
            AVPM2MTable.insert_many_all(av_p_m2m_list)
        tprint('thread for av p av_p ends!')

    @timeit
    def flush_user(self):
        tprint('thread for user starts...')
        user_tuple = tuple(self.user_to_insert_set)
        with self.user_lock:
            UserTable.insert_many_all(user_tuple)
            user_res = UserTable.fetch_many_id_by_uid(user_tuple)
        for id_uid_dict in user_res:
            self.user_fetched_dict[id_uid_dict['uid']] = id_uid_dict['id']
        tprint('thread for user ends!')

    @timeit
    def flush_content(self):
        tprint('thread for content starts...')
        content_tp = tuple(self.dm_content_to_insert_set)
        with self.content_lock:
            DMContentTable.insert_many_all(content_tp)
            content_res = DMContentTable.fetch_many_id_by_content(content_tp)
        for id_cont_dict in content_res:
            self.dm_content_fetched_dict[id_cont_dict['content']] = \
                    id_cont_dict['id']
        tprint('thread for content ends!')

    def flush_dm(self):
        tprint('thread for dm starts...')
        for dmid, dm_record in self.dm_dict.items():
            user = dm_record[6]
            content = dm_record[8]
            oid = dm_record[9]
            dm_record[6] = self.user_fetched_dict[user]
            dm_record[9] = self.p_fetched_dict[str(oid)]
            if dm_record[1] not in (7, 8):
                dm_record[8] = self.dm_content_fetched_dict[content]
        records = tuple(self.dm_dict.values())
        self.dm_dict.clear()
        self.user_fetched_dict.clear()
        self.dm_content_fetched_dict.clear()
        with self.dm_lock:
            DMTable.insert_many_all(records)
        tprint('thread for dm ends...')

    @timeit
    def flush(self):
        tprint(len(self.dm_dict))
        threading.Thread(target=self.handle_users).start()
        threading.Thread(target=self.handle_contents).start()
        threading.Thread(target=self.flush_av_p).start()
        tprint('waiting for threads join before dm...')
        for thd in threading.enumerate():
            if thd is not threading.current_thread():
                thd.join()
        tprint('threads joined before dm')
        self.flush_dm()

    def main(self):
        while True:
            try:
                aid = self.av_q.get(block=False)
            except Empty:
                break
            self.handle_av(aid)
            if len(self.dm_dict) > self.dm_limit:
                self.flush()
                Timer.print_timer()
                Timer.clear()
                break
        if self.dm_dict:
            self.flush()
            for thd in threading.enumerate():
                if thd is not threading.current_thread():
                    thd.join()
            Timer.print_timer()
            Timer.clear()


def p(q, avp_lock, dm_lock, user_lock, content_lock):
    bm = BiliManager(q, avp_lock, dm_lock, user_lock, content_lock)
    bm.main()


def limit_print(lk):
    def dec(fn):
        def wrapper(*args, **kwargs):
            with lk:
                ret = fn(*args, **kwargs)
            return ret
        return wrapper
    return dec


def tprint(*args, **kwargs):
    pid = os.getpid()
    print(pid, *args, **kwargs)


print_lock = Lock()
timer_lock = Lock()
aid_q = Queue()
tprint = limit_print(print_lock)(tprint)
Timer.print_timer = limit_print(timer_lock)(Timer.print_timer)
def main(avid=None):
    if avid:
        aid_q.put(avid)
    else:
        for aid in os.listdir(os.path.join(settings.DM_FILE, 'avs')):
            aid_q.put(aid)
    r_locks = [RLock(), RLock(), RLock(), RLock()]
    while aid_q.qsize():
        try:
            subp = Process(
                target=p,
                args=(aid_q, *r_locks)
                   )
            subp.start()
            tprint('new subprocess starts')
            subp.join()
            tprint('last subprocess ends')
        except KeyboardInterrupt:
            break
