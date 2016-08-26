#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
permit_db_utils.py -- Building Permit Database misc utilities
Author: Garin Wally; Aug 2016

"""

import os
import re
from glob import glob

import pandas as pd

import dslw
from tkit.cli import StatusLine, handle_ex

status = StatusLine()


class ParseAddr(object):
    def __init__(self, addr, city="", state=""):
        self.raw = addr
        self.city = city
        self.state = state

        if addr:
            self.parsed = self.parse_addr(addr)
            if city:
                self.parsed.append(city)
            if state:
                self.parsed.append(state)
            self.sql_like = "'{} %{} %'".format(self.st_numb, self.st_name)
        else:
            self.parsed = u""
            self.sql_like = u""

    def parse_addr(self, a):
        """Parses address into a list of st number and st name."""
        suffixes = re.compile(
            "(AVE|ST|RD|WAY|DR|LN|CT|PL|BLVD|LP|RISE)(?!\w)")
        self.st_suffix = suffixes.findall(a)[0]
        # To list
        addr_split = a.split()
        # Take the left side of '-' if exists in index 0
        if "-" in addr_split[0]:
            addr_split[0] = addr_split[0].split("-")[0]
            a = " ".join(addr_split)
        # Remove appartment
        try:
            a = a.replace(re.findall("APT \w+", a)[0], "")  # Full word?
        except IndexError:
            pass
        # Remove single-letter at end of string (usually heading or APT)
        try:
            a = a.replace(re.findall(" \D{1}$", a)[0], "")
        except IndexError:
            pass
        self.st_numb = re.findall("\d+ ", a)[0].strip()
        self.st_name = re.findall("\w{3,} ", a)[-1].strip()
        # Likely a single mistake '03RD ST'
        if self.st_name.startswith("0"):
            self.st_name = self.st_name.replace("0", "")
        return [self.st_numb, self.st_name, self.st_suffix]

    def __repr__(self):
        return str(self.sql_like)


def make_sqllist(row):
    if type(row[0]) is tuple:
        row = [r[0] for r in row]
    return ", ".join(["'{}'".format(v) for v in row])


def reset_db(conn):
    """Drops tables with a backup and clones the backup."""
    _c = conn.cursor()
    bks = [t for t in conn.get_tables() if t.endswith("_bk")]
    drops = [t.split("_")[0] for t in bks]
    sql = ("DROP TABLE IF EXISTS {0}; "
           "SELECT CloneTable('main', '{0}_bk', '{0}', 0);")
    [_c.execute(sql.format(t)) for t in drops]
    _c.execute("VACUUM")
    return


# =============================================================================
# INSPECTION FUNCTIONS

def get_outer_join(conn, permit_table):
    """Get permit geocodes/parcelids that do not join with ufda_parcels"""
    _c = conn.cursor()
    join_qry = ("SELECT {0}.address, {0}.geocode, ufda_parcels.parcelid "
                "FROM {0} LEFT OUTER JOIN ufda_parcels "
                "ON {0}.geocode=ufda_parcels.parcelid "
                "WHERE ufda_parcels.parcelid IS NULL").format(permit_table)
    outer = _c.execute(join_qry).fetchall()
    return outer


def get_outer_addr_join(conn, permit_table):
    """Get permit addrs that do not join with the ufda_addr table"""
    _c = conn.cursor()
    join_qry = ("SELECT {0}.address, {0}.geocode, ufda_addrs.parcelid "
                "FROM {0} LEFT OUTER JOIN ufda_addrs "
                "ON {0}.address=ufda_addrs.fulladdress "
                "WHERE ufda_addrs.parcelid IS NULL").format(permit_table)
    outer = _c.execute(join_qry).fetchall()
    return outer


def matches_parcels(conn, permit_table):
    _c = conn.cursor()
    check = ("SELECT {0}.geocode "
             "FROM {0} JOIN ufda_parcels "
             "ON {0}.geocode=ufda_parcels.parcelid "
             "WHERE ufda_parcels.geometry IS NOT NULL").format(permit_table)
    all_permits = "SELECT * FROM {}".format(permit_table)
    n_permits = len(_c.execute(all_permits).fetchall())
    parcel_joins = len(_c.execute(check).fetchall())
    return (parcel_joins//n_permits, "{}/{}".format(parcel_joins, n_permits))


def matches_addrs(conn, permit_table):
    """Counts successful joins by address."""
    _c = conn.cursor()
    check = ("SELECT {0}.geocode "
             "FROM {0} JOIN ufda_addrs "
             "ON {0}.address = ufda_addrs.fulladdress").format(permit_table)
    all_permits = "SELECT * FROM {}".format(permit_table)
    n_permits = len(_c.execute(all_permits).fetchall())
    addr_joins = len(_c.execute(check).fetchall())
    return (addr_joins//n_permits, "{}/{}".format(addr_joins, n_permits))


# =============================================================================
# REPAIR FUNCTIONS


def fix_addrs(conn, permit_table):
    """Fixes permit addr and geo by parse-matching addr to ufda_addrs."""
    _c = conn.cursor()
    addrs = set(_c.execute("SELECT fulladdress FROM ufda_addrs"))
    permit_addr = set(
        _c.execute("SELECT address FROM {}".format(permit_table)))
    # Addresses that exist in permits and not in the address points
    fix_addrs = list(permit_addr.difference(addrs))
    for old_addr in fix_addrs:
        if not old_addr[0]:
            continue
        addr_qry = ("SELECT parcelid, fulladdress FROM ufda_addrs "
                    "WHERE fulladdress LIKE {}".format(ParseAddr(old_addr[0])))
        correct = _c.execute(addr_qry).fetchall()
        update_q = (
            "UPDATE {} SET address=?, notes = 'CHANGED: {}' "
            "WHERE address=?")
        if correct:
            for match in correct:
                _c.execute(update_q.format(permit_table, old_addr[0]),
                           (match[1], old_addr[0]))
    new_addrs = set(_c.execute("SELECT address FROM {}".format(permit_table)))
    not_fixed = list(new_addrs.difference(addrs))
    return not_fixed


def correct_misplaced(conn, permit_table):
    """UPDATE permit POINTs that are not within the right parcel.
    'PointOnSurface()' is better than 'Centroid()' -- ALWAYS within polygon."""
    _c = conn.cursor()
    q = ("SELECT AsText(PointOnSurface(u.geometry)), SRID(u.geometry), "
         "p.geocode "
         "FROM {} p, ufda_parcels u "
         "WHERE p.geocode = u.parcelid "
         "AND NOT Contains(u.geometry, p.geometry)").format(permit_table)
    rows = _c.execute(q).fetchall()
    update = "UPDATE {} SET geometry=GeomFromText(?, ?) WHERE geocode=?"
    for r in rows:
        _c.execute(update.format(permit_table), r)
    return


def correct_invalid_geoms(conn, permit_table):
    _c = conn.cursor()
    srid_qry = "SELECT SRID(geometry) FROM {} WHERE geometry IS NOT NULL"
    srid = _c.execute(srid_qry.format(permit_table)).fetchone()[0]
    invalid_qry = ("SELECT address FROM {} "
                   "WHERE IsValid(geometry) IS -1")
    invalids = _c.execute(invalid_qry.format(permit_table)).fetchall()
    if not invalids:
        return
    parcel_center = ("SELECT AsText(ST_Centroid(p.geometry)), p.parcelid "
                     "FROM ufda_parcels p JOIN ufda_addrs a "
                     "ON Intersects(a.geometry, p.geometry) "
                     "AND a.fulladdress='{}'")
    for row in invalids:
        point = _c.execute(parcel_center.format(row[0])).fetchone()[0]
        update_qry = ("UPDATE {} SET geometry=GeomFromText('{}', {}) "
                      "WHERE address=?")
        _c.execute(update_qry.format(permit_table, point, srid), row)
    return


def correct_null_geoms(conn, permit_table):
    """Replaces NULL geometry with ufda_addr geometry."""
    _c = conn.cursor()
    addr_qry = "SELECT address FROM {} WHERE geometry IS NULL"
    addrs = _c.execute(addr_qry.format(permit_table)).fetchall()
    if not addrs:
        return
    for addr in addrs:
        q = ("SELECT a.parcelid, AsText(a.geometry), SRID(a.geometry) "
             "FROM ufda_addrs a "
             "JOIN {} p ON p.address=a.fulladdress "
             "WHERE p.address = ?")
        _c.execute(q.format(permit_table), addr)
        fixes = _c.fetchone()
        update = "UPDATE {} SET geocode=?, geometry=GeomFromText(?, ?)"
        if fixes:
            _c.execute(update.format(permit_table), fixes)
    return


# LAST RESORT...
def get_addr_geom(conn, permit_table):
    _c = conn.cursor()
    _c.execute(dslw.utils.AddGeometryColumn(permit_table, 2256, 'POINT'))
    s = ("SELECT a.geometry, p.address "
         "FROM {} p JOIN ufda_addrs a "
         " ON a.fulladdress=p.address "
         "WHERE p.geometry IS NULL").format(permit_table)

    u = ("UPDATE {} SET geometry=? "
         "WHERE address=? AND geometry IS NULL").format(permit_table)
    g = _c.execute(s).fetchall()
    for t in g:
        _c.execute(u, t)
    return


def get_parcel_geom(conn, permit_table):
    _c = conn.cursor()
    s = ("SELECT PointOnSurface(u.geometry), p.geocode "
         "FROM {} p JOIN ufda_parcels u "
         " ON u.parcelid=p.geocode "
         "WHERE p.geometry IS NULL").format(permit_table)

    u = ("UPDATE {} SET geometry=? "
         "WHERE geocode=? AND geometry IS NULL").format(permit_table)
    g = _c.execute(s).fetchall()
    for t in g:
        _c.execute(u, t)
    return

'''
def addrs2points(conn, permit_table):
    _c = conn.cursor()
    sql = "SELECT address FROM {} WHERE geometry IS NULL"
    addrs = _c.execute(sql.format(permit_table)).fetchall()
    for addr in addrs:
        geocoder = geopy.Nominatim()
        g = geocoder.geocode(addr[0])
        point = "POINT({} {})".format(g.longitude, g.latitude)
        u = ("UPDATE {} SET geometry=ST_Transform(GeomFromText(?, 4326), 2256)"
             " WHERE address=? AND geometry IS NULL").format(permit_table)
        _c.execute(u, (point, addr[0]))
    return
'''


# =============================================================================
# SPATIAL DATA CREATION FUNCTIONS

'''
def dissolve_points(conn, permit_table):
    _c = conn.cursor()
    sql = ("")
'''

# DO NOT USE
# -- USE density.sql instead
'''
def dev_density(conn, permits_dissolved):
    _c = conn.cursor()
    temp_duacs = "{}_duac_temp".format(permits_dissolved)
    year = re.findall("\d+", permits_dissolved)[0]
    final_table = "duacs{}".format(year)
    create = ("CREATE TABLE {0} AS "
              "SELECT p.geocode AS geocode, SUM(p.dwellings) AS dwellings, "
              " z.BASE AS zoning, ST_Multi(ST_Collect(p.geometry)) AS geometry "
              "FROM {1} p "
              "JOIN ufda_zoning z "
              "  ON Intersects(p.geometry, z.geometry) "
              "GROUP BY p.geocode "
              "HAVING SUM(p.dwellings) > 1 " #AND SUM(p.dwellings) < 100 "
              "ORDER BY SUM(p.dwellings) DESC; "
              ).format(temp_duacs, permits_dissolved)
    duacs = ("CREATE TABLE {1} AS "
             "  SELECT p.*, FLOOR(dwellings/(Area(u.geometry)/43560)) AS duac "
             "  FROM {0} p "
             "  JOIN parcels_dis u "
             "    ON Intersects(p.geometry, u.geometry) "
             "  ORDER BY duac DESC;").format(temp_duacs, final_table)
    geom = ("SELECT RecoverGeometryColumn('{0}', 'geometry', 2256, "
            "  'MULTIPOINT', 'XY');").format(final_table)
    drop = "DROP TABLE {0};".format(temp_duacs)
    _c.execute(create)
    _c.execute(duacs)
    _c.execute(geom)
    _c.execute(drop)
    dslw.lite2csv(conn, final_table, "reports/{}_dev.csv".format(final_table))
    return
'''


def dissolve(conn, permit_table):
    _c = conn.cursor()
    sql = (
        "CREATE TABLE {0}_dis AS SELECT permit_number, geocode, address, "
        " dwellings, ST_Multi(ST_Collect(geometry)) AS geometry "
        "FROM {0} GROUP BY  permit_number; "
        "SELECT RecoverGeometryColumn('{0}_dis', 'geometry', 2256, "
        " 'MULTIPOINT', 'XY');").format(permit_table)
    _c.execute(sql)
    return
