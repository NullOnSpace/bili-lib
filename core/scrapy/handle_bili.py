import json
import re
import os
from bs4 import BeautifulSoup as bs
from collections import defaultdict


root_path = '/media/data/bili/avs'
max_title_length = 0
max_desc_length = 0
copyright = set()
redirect_count = 0
title_flag = True
desc_flag = True
part_flag = True
part_length = 0
duration = 0
oids = set()
oid_path = '/media/data/bili/dms'
oid_set = set(os.listdir(oid_path))
for filename in os.listdir(root_path):
    with open(os.path.join(root_path, filename), 'rb') as fd:
        js_obj = json.load(fd)
    if js_obj['code'] != 0:
        print(filename, js_obj['message'])
        continue
    data = js_obj['data']
    # for character in data['title']:
    #     if title_flag and len(bytes(character, encoding='utf8'))>3:
    #         title_flag = False
    #         print('title:', character)
    # for character in data['desc']:
    #     if desc_flag and len(bytes(character, encoding='utf8'))>3:
    #         desc_flag = False
    #         print('desc:', character)
    # if len(data['title'])>max_title_length:
    #     max_title_length = len(data['title'])
    # if len(data['desc'])>max_desc_length:
    #     max_desc_length = len(data['desc'])
    # copyright.add(data['copyright'])
    if data.get('pages'):
        pages = data['pages']
        for page in pages:
            # if page['duration']>duration:
            #     duration = page['duration']
            # if len(page['part'])>part_length:
            #     part_length = len(page['part'])
            # for character in page['part']:
            #     if part_flag and len(bytes(character, encoding='utf8'))>3:
            #         part_flag = False
            #         print('part:', character)
            oid = page['cid']
            if str(oid) not in oid_set:
                with open('incompleteaid', 'a') as fd:
                    fd.write(filename+'\n')
            # if oid in oids:
            #     print(filename, oid)
            # else:
            #     oids.add(oid)
    # if data.get('redirect_url'):
    #     redirect_count += 1
    #     assert re.search(r'/(ep|ss)\d+', data['redirect_url']), data['redirect_url']
print('title length:', max_title_length)
print('desc length:', max_desc_length)
print('copyright:', copyright)
print('redirect_count:', redirect_count)
print('part_length:', part_length)
print('duration:', duration)

'''
root_path2 = '/media/data/bili/dms'
max_txt_length = 0
longest_txt = str()
stat = defaultdict(set)

for filename in os.listdir(root_path2):
    with open(os.path.join(root_path2, filename)) as fd:
        soup = bs(fd.read(), 'xml')
    d_list = soup.find_all('d')
    for d in d_list:
        p_list = d['p'].split(',')
        # assert len(p_list)==8, 'p length:'+d['p']
        # stat['dm_position'].add(p_list[1])
        # stat['font_size'].add(p_list[2])
        # stat['dm_type'].add(p_list[5])
        # stat['len_user'].add(len(p_list[6]))
        # stat['userid'].add(len(p_list[7]))
        # stat['font_color'].add(int(p_list[3]))
        txt = str(d.string).strip()
        # if p_list[1] not in ('1', '4', '5') or p_list[5] != '0':
        #     with open('bili_dmtype', 'a') as fd:
        #         fd.write('{},{},{}\n'.format(p_list[1], p_list[5], txt))
        # for character in txt:
        #     char_len = len(bytes(character, encoding='utf8'))
        #     stat['encode_len'].add(char_len)
        if len(txt)>max_txt_length:
            max_txt_length = len(txt)
            longest_txt = str(txt)
# print(stat)
# print(max(stat['font_color']))
print('max txt len:', max_txt_length, longest_txt)
max_blob_length=0
longest_blob = ''
with open('bili_dmtype', 'r') as fd:
    s_list = fd.readlines()
for s in s_list:
    if s.strip():
        *_, txt = s.split(',',2)
        btxt = txt.strip().encode(encoding='utf8')
        if len(btxt)>max_blob_length:
            longest_blob = btxt
            max_blob_length = len(btxt)
print(max_blob_length)
print(longest_blob)
'''
