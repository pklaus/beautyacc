import psycopg2, psycopg2.extras

from beautyacc.util.caching import cached_property
from functools import lru_cache

from .models import (Channel, ChannelGroup, SampleMode, Severity, Status,
                     NumMetadata, EnumMetadata)

class ArchiveConnection:

    def __init__(self, host, user="report", port=5432, dbname="archive"):
        self._conn = psycopg2.connect(dbname=dbname, user=user, host=host, port=port)

    def dispose(self):
        self._conn.close()

    def close(self):
        self.dispose()

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
        # Auto-generate methods according to classes in beautyacc.core.models
        self._populate_fetch_methods()

    @lru_cache(maxsize=65536)
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

    @cached_property
    def grpid_to_grpname_map(self):
        sql = "SELECT grp_id, name FROM chan_grp;"
        with self._conn.cursor() as curs:
            curs.execute(sql)
            id_map = {el[0]: el[1] for el in curs.fetchall()}
        return id_map

    @cached_property
    def grpname_to_grpid_map(self):
        return {value: key for key, value in self.grpid_to_grpname_map.items()}

    @cached_property
    def all_groups(self):
        return self.get_all(ChannelGroup)

    @cached_property
    def all_channels(self):
        return self.get_all(Channel)

    def get_all(self, cls):
        sql = "SELECT * FROM archive.{table};".format(
                  table=cls.table)
        with self._conn.cursor() as curs:
            curs.execute(sql)
            data = curs.fetchall()
        if data is not None:
            return sorted([cls(*row) for row in data])
        else:
            return []

    def _populate_fetch_methods(self):
        """
        populates the following methods:
        .fetch_channel(channel_id)
        .fetch_chan_grp(group_id)
        .fetch_smpl_mode()
        .fetch_severity()
        .fetch_status()
        .fetch_num_metadata()
        """
        for cls in (Channel, ChannelGroup, SampleMode, Severity, Status,
                    NumMetadata):
            def func_factory(self, cls):
                def func(cls_id):
                    sql = "SELECT * FROM archive.{table} WHERE " \
                          "archive.{table}.{id_field} = %s;".format(
                              table=cls.table,
                              id_field=cls.id_field)
                    with self._conn.cursor() as curs:
                        curs.execute(sql, (cls_id,))
                        data = curs.fetchone()
                    if data is not None:
                        return cls(*data)
                    else:
                        return None
                return func
            func = func_factory(self, cls)
            func = lru_cache(maxsize=65536)(func)
            setattr(self, 'fetch_' + cls.table, func)
            setattr(self, 'fetch_' + cls.__name__, func)

    @lru_cache(maxsize=65536)
    def fetch_enum_metadata(self, channel_id):
        sql = "SELECT enum_nbr, enum_val FROM archive.enum_metadata WHERE " \
              "archive.enum_metadata.channel_id = %s;"
        with self._conn.cursor() as curs:
            curs.execute(sql, (channel_id,))
            enum_map = {el[0]: el[1] for el in curs.fetchall()}
        return enum_map



    def _core_sql_single_pv(self, channel_id, average=None, start=None, end=None, target='float_val'):
        if average:
            assert average in ["second", "minute", "hour", "day", "week", "month", "quarter", "year"]
            sql = "SELECT date_trunc('{}', smpl_time) AS smpl_time, avg({}) AS value ".format(average, target)
        else:
            sql = "SELECT smpl_time, {} AS value ".format(target)
        sql += "FROM archive.sample "
        sql += "WHERE archive.sample.channel_id = {} ".format(channel_id)
        if start: sql += "AND smpl_time >= '{}'::timestamp ".format(start.isoformat(sep=' '))
        if end:   sql += "AND smpl_time <  '{}'::timestamp ".format(end.isoformat(sep=' '))
        if average:
            sql += "GROUP  BY 1"
        sql += ";"
        return sql

    def iter_single_pv(self, pv_name, target='float_val', **kwargs):
        import random, string
        if type(pv_name) == int:
            channel_id = pv_name
        else:
            channel_id = self.channelid_of_pvname(pv_name)
        sql = self._core_sql_single_pv(channel_id, target=target, **kwargs)
        cursor_name_n = 32
        cursor_name = ''.join(random.choices(string.ascii_uppercase + string.digits, k=cursor_name_n))
        with self._conn.cursor(name=cursor_name) as curs:
            curs.itersize = 100000
            curs.execute(sql)
            for row in curs:
                yield row
            # or:
            #while True:
            #    rows = cursor.fetchmany(5000)
            #    if not rows:
            #        break
            #    yield rows
