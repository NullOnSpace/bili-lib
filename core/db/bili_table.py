import pymysql
import zlib

from conf import settings

def get_conn():
    return pymysql.connect(**settings.DB_SETTINGS['default'])


class AVTable:
    conn = get_conn()

    @classmethod
    def is_aid_exists(cls, aid):
        with cls.conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            query = """
            SELECT id
            FROM bili_av
            WHERE aid=%s;
            """
            rows = cursor.execute(query, aid)
            res = cursor.fetchone()['id'] if rows else False
        return res

    @classmethod
    def insert_all(cls, record: tuple):
        """
        record:
        (aid, page_c, favorite_c, view_c, coin_c, share_c, like_c,
        dislike_c, reply_c, ctime, pubdate, copyright, title, `desc`)
        """
        with cls.conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            query = """
            INSERT INTO `bili_av`
            (aid, page_c, favorite_c, view_c, coin_c, share_c, like_c,
            dislike_c, reply_c, ctime, pubdate, copyright, title, `desc`)
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            rows = cursor.execute(query, record)
            cls.conn.commit()

    @classmethod
    def insert_many_all(cls, records: tuple):
        """
        record in records:
        (aid, page_c, favorite_c, view_c, coin_c, share_c, like_c,
        dislike_c, reply_c, ctime, pubdate, copyright, title, `desc`)
        """
        with cls.conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            query = """
            INSERT INTO `bili_av`
            (aid, page_c, favorite_c, view_c, coin_c, share_c, like_c,
            dislike_c, reply_c, ctime, pubdate, copyright, title, `desc`)
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            rows = cursor.executemany(query, records)
            cls.conn.commit()

    @classmethod
    def fetch_many_id_by_aid(cls, aid_tuple):
        res = list()
        with cls.conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            query = """
            SELECT id, aid
            FROM bili_av
            WHERE aid=%s;
            """
            for aid in aid_tuple:
                cursor.execute(query, aid)
                res.append(cursor.fetchone())
        return res


class UserTable:
    conn = get_conn()

    @classmethod
    def is_uid_exists(cls, uid):
        with cls.conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            query = """
            SELECT id
            FROM bili_user
            WHERE uid=%s;
            """
            rows = cursor.execute(query, uid)
            res = cursor.fetchone()['id'] if rows else False
        return res

    @classmethod
    def insert_all(cls, record: tuple):
        """
        record:
        (uid, )
        """
        with cls.conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            query = """
            INSERT INTO `bili_user`
            (uid)
            VALUES(%s)
            """
            rows = cursor.execute(query, record)
            cls.conn.commit()

    @classmethod
    def insert_many_all(cls, records: tuple):
        """
        record in records:
        (uid, )
        """
        with cls.conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            query = """
            INSERT INTO `bili_user`
            (uid)
            VALUES(%s)
            """
            rows = cursor.executemany(query, records)
            cls.conn.commit()

    @classmethod
    def fetch_many_id_by_uid(cls, uid_tuple):
        res = list()
        with cls.conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            query = """
            SELECT id, uid
            FROM bili_user
            WHERE uid=%s;
            """
            for uid in uid_tuple:
                cursor.execute(query, uid)
                res.append(cursor.fetchone())
        return res


class PTable:
    conn = get_conn()

    @classmethod
    def is_oid_exists(cls, oid):
        with cls.conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            query = """
            SELECT id
            FROM bili_p
            WHERE oid=%s;
            """
            rows = cursor.execute(query, oid)
            res = cursor.fetchone()['id'] if rows else False
        return res

    @classmethod
    def insert_all(cls, record: tuple):
        """
        record:
        (oid, duration, title)
        """
        with cls.conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            query = """
            INSERT INTO `bili_p`
            (oid, duration, title)
            VALUES(%s, %s, %s)
            """
            rows = cursor.execute(query, record)
            cls.conn.commit()

    @classmethod
    def insert_many_all(cls, records: tuple):
        """
        record in records:
        (oid, duration, title)
        """
        with cls.conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            query = """
            INSERT INTO `bili_p`
            (oid, duration, title)
            VALUES(%s, %s, %s)
            """
            rows = cursor.executemany(query, records)
            cls.conn.commit()

    @classmethod
    def fetch_many_id_by_oid(cls, oid_tuple):
        res = list()
        with cls.conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            query = """
            SELECT id, oid
            FROM bili_p
            WHERE oid=%s;
            """
            for oid in oid_tuple:
                cursor.execute(query, oid)
                res.append(cursor.fetchone())
        return res


class DMContentTable:
    conn = get_conn()

    @classmethod
    def is_content_exists(cls, content):
        with cls.conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            query = """
            SELECT id
            FROM bili_dm_content
            WHERE content_crc32=%s and content=%s;
            """
            content_crc32 = zlib.crc32(content.encode('utf8'))
            rows = cursor.execute(query, (content_crc32, content))
            res = cursor.fetchone()['id'] if rows else False
        return res

    @classmethod
    def insert_all(cls, record: tuple):
        """
        record:
        (content, )
        """
        with cls.conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            query = """
            INSERT INTO `bili_dm_content`
            (content_crc32, content)
            VALUES(%s, %s)
            """
            content = record[0]
            content_crc32 = zlib.crc32(content)
            rows = cursor.execute(query, (content_crc32, content))
            cls.conn.commit()

    @classmethod
    def insert_many_all(cls, records: tuple):
        """
        record in records:
        content
        """
        with cls.conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            query = """
            INSERT INTO `bili_dm_content`
            (content_crc32, content)
            VALUES(%s, %s)
            """
            real_records = tuple(zip(map(lambda x: zlib.crc32(x.encode('utf8')),
                                         records),
                                     records))
            rows = cursor.executemany(query, real_records)
            cls.conn.commit()

    @classmethod
    def fetch_many_id_by_content(cls, content_tuple):
        res = list()
        with cls.conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            query = """
            SELECT id, content
            FROM bili_dm_content
            WHERE content_crc32=%s and content=%s;
            """
            for content in content_tuple:
                content_crc32 = zlib.crc32(content.encode('utf8'))
                rows = cursor.execute(query, (content_crc32, content))
                if not rows:
                    print(cursor.mogrify(query, (content_crc32, content)))
                    continue
                res.append(cursor.fetchone())
        return res


class DMTable:
    conn = get_conn()

    @classmethod
    def is_dm_exists(cls, dmid):
        with cls.conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            query = """
            SELECT id
            FROM bili_dm
            WHERE dmid=%s;
            """
            rows = cursor.execute(query, content)
            res = cursor.fetchone()['id'] if rows else False
        return res

    @classmethod
    def insert_all(cls, record: tuple):
        """
        record:
        (time_in_video, dm_position, font_size, font_color, time_in_world,
        dm_type, user_id, dmid, dm_content_id, p_id, spe_content_id)
        """
        with cls.conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            query = """
            INSERT INTO `bili_dm`
            (time_in_video, dm_position, font_size, font_color, time_in_world,
            dm_type, user_id, dmid, dm_content_id, p_id, spe_content_id)
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            rows = cursor.execute(query, record)
            cls.conn.commit()

    @classmethod
    def insert_many_all(cls, records: tuple):
        """
        record in records:
        (time_in_video, dm_position, font_size, font_color, time_in_world,
        dm_type, user_id, dmid, dm_content_id, p_id, spe_content_id)
        """
        with cls.conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            query = """
            INSERT INTO `bili_dm`
            (time_in_video, dm_position, font_size, font_color, time_in_world,
            dm_type, user_id, dmid, dm_content_id, p_id, spe_content_id)
            VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            rows = cursor.executemany(query, records)
            cls.conn.commit()

    @classmethod
    def fetch_many_id_by_dmid(cls, dmid_tuple):
        res = list()
        with cls.conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            query = """
            SELECT id, dmid
            FROM bili_dm
            WHERE dmid=%s;
            """
            for dmid in dmid_tuple:
                cursor.execute(query, dmid)
                res.append(cursor.fetchone())
        return res


class AVPM2MTable:
    conn = get_conn()

    @classmethod
    def insert_all(cls, record: tuple):
        """
        record:
        (av_id, p_id, page_in_av)
        """
        with cls.conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            query = """
            INSERT INTO `bili_av_p_m2m`
            (av_id, p_id, page_in_av)
            VALUES(%s, %s, %s)
            """
            rows = cursor.execute(query, record)
            cls.conn.commit()

    @classmethod
    def insert_many_all(cls, records: tuple):
        """
        record in records:
        (av_id, p_id, page_in_av)
        """
        with cls.conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            query = """
            INSERT INTO `bili_av_p_m2m`
            (av_id, p_id, page_in_av)
            VALUES(%s, %s, %s)
            """
            rows = cursor.executemany(query, records)
            cls.conn.commit()

    @classmethod
    def fetch_many_id_pageInAv_by_avId_pId(cls, tp):
        res = list()
        with cls.conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            query = """
            SELECT id, av_id, p_id, page_in_av
            FROM bili_av_p_m2m
            WHERE av_id=%s and p_id=%s;
            """
            for av_p_tp in tp:
                cursor.execute(query, av_p_tp)
                res.append(cursor.fetchone())
        return res


class DMSpeContentTable:
    conn = get_conn()

    @classmethod
    def insert_all_and_get_id(cls, record):
        """
        record:
        (spe_content,)
        """
        with cls.conn.cursor(cursor=pymysql.cursors.DictCursor) as cursor:
            query = """
            INSERT INTO `bili_dm_spe_content`
            (spe_content)
            VALUES(%s)
            """
            cursor.execute(query, record)
            cls.conn.commit()
            return int(cursor.lastrowid)

