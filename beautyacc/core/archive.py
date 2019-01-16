import psycopg2, psycopg2.extras

from beautyacc.util.caching import cached_property
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
        self.pvname_to_channelid_map
        # Marking the initialization as 'done'
        self._initialized = True

    @lru_cache(maxsize=4096)
    def channelid_of_pvname(self, pvname):
        try:
            return self.pvname_to_channelid_map[pvname]
        except KeyError:
            return None

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
