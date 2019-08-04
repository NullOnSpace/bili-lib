import requests
import json
import pprint
from bs4 import BeautifulSoup as bs
import os
import re
import time
import threading

from core.tools import get_url
from conf import settings


def reporter():
    while True:
        time.sleep(15)
        print('{} done {} remains'.format(counter, len(to_scan_list)))


def get_dm(oid, sema=None):
    oid = str(oid)
    payload = {'oid': oid}
    res = get_url(dm_url, params=payload)
    with open(os.path.join(dm_path, oid), 'wb') as fd:
        fd.write(res.content)
    if sema:
        sema.release()
    return res


def get_oid(aid):
    aid = str(aid)
    payload = {'aid': aid}
    res = get_url(oid_url, params=payload)
    with open(os.path.join(oid_path, aid), 'wb') as fd:
        fd.write(res.content)
    return res


def handle_aid(aid, sema_a, sema_o, daemon_thd):
    with sema_a:
        new_url = root_url+'video/av'+aid
        res = get_url(new_url, cookies=cookies)
        filter_url(bs(res.content, 'html.parser'))
        fetch_related(aid)
        av_res = get_oid(aid)
        av_json = av_res.json()
        if av_json['code'] != 0:
            print('message:', av_json['message'])
            return
        if av_json['data'].get('pages') is None:
            print('av', aid, 'no page')
            return
        oid_thds = list()
        for obj in av_json['data']['pages']:
            oid = obj['cid']
            sema_o.acquire()
            thd = threading.Thread(target=get_dm, args=(str(oid),sema_o))
            thd.start()
            oid_thds.append(thd)
        else:
            for subthd in threading.enumerate():
                if subthd not in (threading.main_thread(),
                    threading.current_thread(), daemon_thd, *oid_thds):
                    subthd.join()


def download_aid(aid):
    new_url = root_url+'video/av'+aid
    res = get_url(new_url, cookies=cookies)
    filter_url(bs(res.content, 'html.parser'))
    av_res = get_oid(aid)
    av_json = av_res.json()
    if av_json['code'] != 0:
        print('message:', av_json['message'])
        return
    if av_json['data'].get('pages') is None:
        print('av', aid, 'no page')
        return
    for obj in av_json['data']['pages']:
        oid = obj['cid']
        get_dm(str(oid))
    else:
        print(aid, 'done')


def filter_url(soup):
    a_list = soup.find_all('a')
    for link in a_list:
        if link.get('href'):
            url = link['href']
            match_obj = re.search('/video/av(\d+)', url)
            if match_obj:
                aid = match_obj.group(1)
                if aid not in to_scan_list+os.listdir(oid_path):
                    to_scan_list.append(aid)


def fetch_related(aid):
    related_url = 'https://api.bilibili.com/x/web-interface/archive/related'
    payload = {'aid': str(aid)}
    res = get_url(related_url, params=payload)
    js_res = res.json()
    for ele in js_res['data']:
        new_aid = str(ele['aid'])
        if new_aid not in to_scan_list+os.listdir(oid_path):
            to_scan_list.append(str(ele['aid']))


def main():
    with open(settings.TO_SCAN, 'r') as fd:
        to_scan_list.extend(json.load(fd))
    global counter
    counter = 10000
    print('top10:', to_scan_list[:10])
    thd = threading.Thread(target=reporter)
    thd.setDaemon(True)
    thd.start()
    scanned = set(os.listdir(oid_path))
    sema_pool = threading.Semaphore(value=10)
    sema_pool2 = threading.Semaphore(value=8)
    while to_scan_list:
        try:
            aid = to_scan_list.pop(0)
            if aid in scanned:
                continue
            sema_pool2.acquire()
            sema_pool2.release()
            threading.Thread(
                target=handle_aid(aid, sema_pool2, sema_pool, thd)).start()
            scanned.add(aid)
            counter -= 1
            if counter == 0:
                break
        except KeyboardInterrupt:
            break
        except requests.HTTPError:
            continue
        except Exception:
            print(av_json)
            if aid in os.listdir(oid_path):
                os.remove(os.path.join(oid_path, aid))
            raise


root_url = 'https://www.bilibili.com/'
oid_url = 'https://api.bilibili.com/x/web-interface/view'
dm_url = 'https://api.bilibili.com/x/v1/dm/list.so'

oid_path = os.path.join(settings.DM_FILE, 'avs')
dm_path = os.path.join(settings.DM_FILE, 'dms')

to_scan_list = list()

cookies = {
    'dssid': '86ed8fc8a0c4e4cb6', 'fts': '1528953020',
    'dsess': 'BAh7CkkiD3Nlc3Npb25faWQGOgZFVEkiFThmYzhhMGM0ZTRjYjYwNDgGOwBG%0ASSIJY3NyZgY7AEZJIiU0MDQ2Mjc4YjVhOGJlNzE1MmIyY2FhMTA0ZjNkNTA2%0AZQY7AEZJIg10cmFja2luZwY7AEZ7B0kiFEhUVFBfVVNFUl9BR0VOVAY7AFRJ%0AIi04NmE2NDJiNWVjMWVjZWRlNjYyYWRmNDc2YTdkNGVkZjBkOGFkNTJmBjsA%0ARkkiGUhUVFBfQUNDRVBUX0xBTkdVQUdFBjsAVEkiLWZiZmMxYjhmZTg0YmFi%0ANjA2ZTA5ODExYTQ1YjlmZGI2MWM4ZDE1ZGIGOwBGSSIKY3RpbWUGOwBGbCsH%0AvvghW0kiCGNpcAY7AEYiEzE4MC4xMTAuNzAuMjI4%0A--1ca162c02bfb95e7938bc209cd979fa2fe12164c',
    'buvid3': '701F4484-F2A1-4B51-BC0B-98B3855544E432080infoc',
    'rpdid': 'kwimoislqqdosiqlskwxw', 'LIVE_PLAYER_TYPE': '1',
    'LIVE_BUVID': '1e563880af9694e05154b2cd845fc275',
    'LIVE_BUVID__ckMd5': 'c7c6c83775f08cae', 'im_notify_type_2925953': '0',
    'sid': 'hw5btqjb', 'CURRENT_QUALITY': '80', 'stardustvideo': '1',
    'CURRENT_FNVAL': '16',
    'UM_distinctid': '16919985ccf748-0b5dec4aacd535-24414032-100200-16919985cd047a',
    'bp_t_offset_2925953': '245165151394976108',
    '_dfcaptcha': '19ae4569cecab98ec0c5183b9213718c'
}

if __name__ == '__main__':
    to_scan_list = list()
    aid = '13690129'
    res = get_url(root_url+'video/av'+aid, cookies=cookies)
    filter_url(bs(res.content, 'html.parser'))
    fetch_related(aid)
    try:
        main()
    except Exception:
        raise
    finally:
        with open(settings.TO_SCAN, 'w') as fd:
            json.dump(to_scan_list, fd)
