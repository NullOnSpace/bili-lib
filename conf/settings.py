import sys
import os


BASE_DIR = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))

DM_FILE = '/media/file/bili' or '/home/hikaru/git/PyBilibili/file/bili'
TO_SCAN = os.path.join(BASE_DIR, 'file/bili_to_scan')

DB_SETTINGS = {
    'default': {
        'host': '127.0.0.1',
        'port': 3306,
        'user': 'root',
        'password': 'cashme0304mush',
        'db': 'bilibili',
        'charset': 'utf8mb4',
    },
}
