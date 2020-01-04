#!/usr/bin/env python

from beautyacc.core import CoreArchive

import pandas as pd

class PandasArchive:

    def sql_single_pv(self, channel_id, average=None, start=None, end=None, target='float_val'):
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

    def get_single_pv(self, pv_name, target='float_val', **kwargs):
        if type(pv_name) == int:
            channel_id = pv_name
        else:
            channel_id = self.channelid_of_pvname(pv_name)
        sql = self.sql_single_pv(channel_id, target=target, **kwargs)
        df = pd.read_sql_query(sql, self._conn, index_col='smpl_time')
        if target in ('float_val', 'num_val'):
            df['value'] = pd.to_numeric(df['value'])
        return df['value']

    def sql_all_pvs(self, start=None, end=None, target='float_val'):
        sql = "SELECT smpl_time, {}, channel_id ".format(target)
        sql += "FROM archive.sample "
        if start or end: sql += "WHERE "
        if start: sql += "smpl_time >= '{}'::timestamp ".format(start.isoformat(sep=' '))
        if start and end: sql += "AND "
        if end:   sql += "smpl_time <  '{}'::timestamp ".format(end.isoformat(sep=' '))
        sql += ";"
        return sql

    def get_all_pvs(self, **kwargs):
        sql = self.sql_all_pvs(**kwargs)
        df = pd.read_sql_query(sql, self._conn, index_col='smpl_time')
        return df

    def sql_single_pv_resample_6h(self, channel_id, include_min_max=False, start=None, end=None, target='float_val', **kwargs):
        sql = "SELECT smpl_time::date + (extract(hour from smpl_time)::int/6*6)*(interval '1 hour') AS smpl_time, "
        sql += "avg({}) ".format(target)
        if include_min_max:
            sql += ", min({}), max({})".format(target, target)
        sql += "FROM sample "
        sql += "WHERE channel_id = {} ".format(channel_id)
        if start: sql += "AND smpl_time >= '{}'::timestamp ".format(start.isoformat(sep=' '))
        if end:   sql += "AND smpl_time <  '{}'::timestamp ".format(end.isoformat(sep=' '))
        sql += "GROUP BY 1 "
        sql += "ORDER BY 1;"
        return sql

    def get_single_pv_resample_6h(self, pv_name, include_min_max=False, **kwargs):
        channel_id = self.channelid_of_pvname(pv_name)
        sql = self.sql_single_pv_resample_6h(channel_id,
                                             include_min_max=include_min_max,
                                             **kwargs)
        df = pd.read_sql_query(sql, self._conn, index_col='smpl_time')
        df['avg'] = pd.to_numeric(df['avg'])
        if include_min_max:
            df['min'] = pd.to_numeric(df['min'])
            df['max'] = pd.to_numeric(df['max'])
        return df

    def get_single_pv_resample(self, pv_name, rule, **kwargs):
        """
        here, we resample the pandas dataframe
        disadvantage: the full data has to be
        fetched from the server first...
        """
        channel_id = self.channelid_of_pvname(pv_name)
        sql = self.sql_single_pv(channel_id, **kwargs)
        df = pd.read_sql_query(sql, self._conn, index_col='smpl_time')

        df['value'] = pd.to_numeric(df['value'])

        dfr = df.resample(rule)
        dff = dfr.mean()
        dff['min'] = dfr.min().value
        dff['max'] = dfr.max().value
        dff['count'] = dfr.count().value
        dff['first'] = dfr.first()
        dff['last'] = dfr.last()

        for i, row in dff.iterrows():
            if i == 0:
                continue
            if dff.at[i, 'count'] == 0:
                correct_value = dff.at[i-1, 'last']
                if correct_value != correct_value:
                    correct_value = dff.at[i-1, 'value']
                dff.at[i, 'value'] = correct_value
        return dff
