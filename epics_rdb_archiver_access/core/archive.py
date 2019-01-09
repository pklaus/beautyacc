import psycopg2, psycopg2.extras

from epics_rdb_archiver_access.util.caching import cached_property
from functools import lru_cache

class ArchiveConnection:

    def __init__(self, host, user="report", port=5432, dbname="archive"):
        self._conn = psycopg2.connect(dbname=dbname, user=user, host=host, port=port)

    def dispose(self):
        self._conn.close()

class CoreArchive(ArchiveConnection):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._initialize()

    def _initialize(self):
        #self._channel_mapping = {'by_id': {}, 'by_pvname': {}}
        # Quering those properties once to have them cached:
        self.all_pv_names
        self.channelid_to_pvname_map
        # Marking the initialization as 'done'
        self._initialized = True

    @lru_cache(maxsize=4096)
    def channelid_of_pvname(self, pv_name):
        sql = """SELECT channel_id FROM archive.channel
                 WHERE archive.channel.name = '{}';""".format(pv_name)
        with self._conn.cursor() as curs:
            curs.execute(sql)
            data = curs.fetchone()
            return data[0] if data else None

    @cached_property
    def all_pv_names(self):
        sql = "SELECT name FROM archive.channel;"
        with self._conn.cursor() as curs:
            curs.execute(sql)
            pv_names = curs.fetchall()
        pv_names = [el[0] for el in pv_names]
        return pv_names

    @cached_property
    def channelid_to_pvname_map(self):
        sql = "SELECT channel_id, name FROM archive.channel;"
        with self._conn.cursor() as curs:
            curs.execute(sql)
            id_map = {el[0]: el[1] for el in curs.fetchall()}
        return id_map

    @cached_property
    def pvname_to_channelid_map(self):
        return {value: key for key, value in self.channelid_to_pvname_map.items()}
