#!/usr/bin/env python

from beautyacc.core import CoreArchive

import numpy as np

class NumpyArchive:

    def numpy_sql_single_pv(self, channel_id, average=None, start=None, end=None, target='float_val'):
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

    def numpy_get_single_pv(self, pv_name, target='float_val', **kwargs):
        if type(pv_name) == int:
            channel_id = pv_name
        else:
            channel_id = self.channelid_of_pvname(pv_name)
        sql = self.numpy_sql_single_pv(channel_id, target=target, **kwargs)
        with self._conn.cursor() as curs:
            curs.execute(sql)
            data = curs.fetchall()
        return np.array([d[0].timestamp() for d in data]), np.array([d[1] for d in data])
