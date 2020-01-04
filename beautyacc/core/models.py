"""
Models of the Archiver
Mostly representing the DB tables
as set up in
https://github.com/pklaus/beauty_docker/blob/master/db/01_postgres_schema.sql
"""

from attr import attrib, attrs


@attrs
class Channel(object):
    table = 'channel'
    id_field = 'channel_id'
    channel_id = attrib(type=int)
    name = attrib(type=str)
    descr = attrib(type=str)
    grp_id = attrib(type=int)
    smpl_mode_id = attrib(type=int)
    smpl_val = attrib(type=float)
    smpl_per = attrib(type=float)
    retent_id = attrib(type=int)
    retent_val = attrib(type=float)

@attrs
class ChannelGroup(object):
    table = 'chan_grp'
    id_field = 'grp_id'
    grp_id = attrib(type=int)
    name = attrib(type=str)
    eng_id = attrib(type=int)
    descr = attrib(type=str)
    enabling_chan_id = attrib(type=int)

@attrs
class SampleMode(object):
    table = 'smpl_mode'
    id_field = 'smpl_mode_id'
    smpl_mode_id = attrib(type=int)
    name = attrib(type=str)
    descr = attrib(type=str)

@attrs
class Severity(object):
    table = 'severity'
    id_field = 'severity_id'
    severity_id = attrib(type=int)
    name = attrib(type=str)

@attrs
class Status(object):
    table = 'status'
    id_field = 'status_id'
    status_id = attrib(type=int)
    name = attrib(type=str)

@attrs
class NumMetadata(object):
    table = 'num_metadata'
    id_field = 'channel_id'
    channel_id = attrib(type=int)
    low_disp_rng = attrib(type=float)
    high_disp_rng = attrib(type=float)
    low_warn_lmt = attrib(type=float)
    high_warn_lmt = attrib(type=float)
    low_alarm_lmt = attrib(type=float)
    high_alarm_lmt = attrib(type=float)
    prec = attrib(type=int)
    unit = attrib(type=str)

@attrs
class EnumMetadata(object):
    table = 'enum_metadata'
    id_field = 'channel_id'
    channel_id = attrib(type=int)
    enum_nbr = attrib(type=int)
    enum_val = attrib(str)
