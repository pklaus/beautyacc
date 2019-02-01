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
    desr = attrib(type=str)
    grp_id = attrib(type=int)
    smpl_mode_id = attrib(type=int)
    smpl_val = attrib(type=float)
    smpl_per = attrib(type=float)
    retent_id = attrib(type=int)
    retent_val = attrib(type=float)
