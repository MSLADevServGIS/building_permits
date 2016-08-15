#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
permits.py -- Building Permit Database Script
Author: Garin Wally; April-July 2016

This script processes building permit data from Accela, builds a new SQLite
database ('permits.sqlite'), inserts relevant featureclasses from an ESRI File
Geodatabase, and creates permits<year> features by joining permits with either
ufda_addr points or ufda_parcels centroid points.
See the documentation for more information.
"""

import os
import sys
import traceback
import re
from glob import glob

import pandas as pd
import geopy
# from termcolor import cprint

import dslw
from tkit.cli import StatusLine, handle_ex

status = StatusLine()

# =============================================================================
# DATA

# Output SQLite database
db = "permits.sqlite"

# Input geodatabase and featureclasses
PARCELS_DIS = os.path.abspath("data/shps/ufda_parcels_dis")
ZONING = os.path.abspath("data/shps/zoning")
GDB = "data/permit_features.gdb"
UFDA_NHOODS = (GDB, "ufda_nhoods")
UFDA_ADDRS = (GDB, "ufda_addrs")
UFDA_PARCELS = (GDB, "ufda_parcels")


# =============================================================================
# VARS

# Output reports
RES_REPORT = "data/processed/res{}.csv"
# com_report = "com{}.csv"
# pub_report = "pub{}.csv"

# All construction permit codes
res_codes = {
    'BNMRA': "New Multifamily 3-4 Units",
    'BNMRB': "New Multifamily 5+ Units",  # Technically COM permit
    'BNRDX': "New Duplex",
    'BNSFR': "New Single Family Residence",
    'BNSFT': "New Single Family Townhouse",
    'BNROS': "New Shelter/Dorm/Etc",
    '': "None specified",
    'None': "None specified"
    }

com_codes = {
    'BNCOP': "New Office/Bank/Professional Building",
    'BNCSC': "New Store/Customer Service",
    'BNCSS': "New Service Station/Repair Garage",
    'BNCID': "New Industrial",
    'BNCON': "New Other",
    'BNRHM': "New Hotel/Motel/Cabin",
    'BO/S/C': "Other Commercial"
    }

pub_codes = {
    'BNCCR': "New Church/Religious Building",
    'BNCHI': "New Hospital/Institution",
    'BNCPG': "New Parking Garage",
    'BNCPW': "New Public Works Facility",
    'BNCSE': "New Education",
    'BNCSR': "New Recreation"
    }


# =============================================================================
# REPORT CREATION FUNCTION

# TODO: setup_data

def process_accela(all_permits):
    """Cleans, preps, and exports building permit reports."""
    # Open raw constuction-permit report as DataFrame 'all_const'
    all_const = pd.read_excel(all_permits)

    # =========================================================================
    # CLEAN

    # Drop columns will all NULL values
    all_const.dropna(axis=1, how='all', inplace=True)

    # Rename cols from values in row 3: lowercase and replace spaces with "_"
    all_const.columns = all_const.ix[3].apply(
        lambda x: x.lower().replace(" ", "_"))

    # Shorten 'dwellings' column name
    # NOTE: units are not always dwellings
    #   (e.g. carport with 2 units means 2 cars)
    all_const.rename(columns={"number_of_dwellings": "dwellings"},
                     inplace=True)

    # Rename index column 'ix'
    all_const.columns.name = 'ix'

    # Drop rows 0-3
    all_const.drop([0, 1, 2, 3], inplace=True)

    # Rename subtype column to permit_type
    all_const.rename(columns={"subtype": "permit_type"},
                     inplace=True)
    # Remove Subtype field descriptions
    all_const['permit_type'].fillna("None", inplace=True)
    all_const['permit_type'] = all_const['permit_type'].apply(
        lambda x: x.split(" ")[0])

    # Convert NULL dwellings to 0
    all_const['dwellings'].fillna(0, inplace=True)
    # Convert Dwellings to integer
    all_const['dwellings'] = all_const['dwellings'].apply(lambda x: int(x))

    # Convert NULL addresses to ""
    all_const['address'].fillna("", inplace=True)

    # Convert Geocode to text
    all_const['geocode'] = all_const['geocode'].apply(lambda x: str(x))

    # Deal with non-unique addresses ...wait what?
    all_const['address'] = all_const['address'].apply(
        lambda x: x.split(" #")[0].strip())

    # Add City column to improve geocoding results
    all_const["city"] = "Missoula"
    all_const = all_const.sort(["permit_number", "address", "dwellings"])

    # =========================================================================
    # GET PERMIT YEAR / ADD YEAR TO OUTPUT REPORT

    years = set()
    all_const["permit_issued_date"].apply(lambda x: years.add(x.year))
    assert len(years) == 1, \
        "Input data shall only consist of one calendar year"

    year = years.pop()
    res_report = RES_REPORT.format(year)

    # =========================================================================
    # GENERATE REPORTS

    # Create DataFrames for each group of building codes
    '''
    res_const = all_const[(all_const['subtype'].isin(res_codes.keys())) &
                          (all_const['dwellings'] > 0)]
    '''
    # Residential Permit Query
    res_const = all_const[
        # Get permits with >= 3 units filed as commercial
        ((all_const["dwellings"] >= 3) &
            (all_const["construction_type"] == "Commercial Construction")) |
        # Get residential construction of only listed subtypes and
        #   dwellings >= 1
        ((all_const["permit_type"].isin(res_codes.keys()) &
            all_const["dwellings"] >= 1))        
    # Finally, groupby permit number, remove duplicates and fix index col
    # ].groupby("permit_number").first().reset_index()
        ].groupby(["permit_number", "geocode"]).first().reset_index()

    # TODO: maybe make reports for other construction types too?
    #com_const = all_const[all_const['subtype'].isin(com_codes.keys())]
    #pub_const = all_const[all_const['subtype'].isin(pub_codes.keys())]

    # Export
    #res_out = res_const.groupby('permit_number').first().reset_index()
    #res_out.to_excel(res_report, index=False)
    res_const.to_csv(res_report, index=False)

    '''
    com_out = com_const.groupby('permit_number').first().reset_index()
    com_out.to_excel(com_report, index=False)

    pub_out = pub_const.groupby('permit_number').first().reset_index()
    pub_out.to_excel(pub_report, index=False)
    '''
    return


# =============================================================================
# UTILITIES


def load_data(conn):
    """Loads data and shows messages."""
    print("Loading spatial data...")
    _c = conn.cursor()

    status.write("  ufda neighborhoods...")
    if "ufda_nhoods" not in TABLES:
        dslw.addons.ogr2lite(conn, UFDA_NHOODS)
        status.success()
    else:
        status.custom("[SKIP]", "yellow")

    status.write("  address points...")
    if "ufda_addrs" not in TABLES:
        dslw.addons.ogr2lite(conn, UFDA_ADDRS)
        status.success()
    else:
        status.custom("[SKIP]", "yellow")

    status.write("  parcels...")
    if "ufda_parcels" not in TABLES:
        dslw.addons.ogr2lite(conn, UFDA_PARCELS)
        status.success()
    else:
        status.custom("[SKIP]", "yellow")

    status.write("  dissolved parcels...")
    if "parcels_dis" not in TABLES:
        sql = dslw.utils.ImportSHP(PARCELS_DIS, "parcels_dis", "UTF-8", 2256,
                                   coerce2D=1)
        if _c.execute(sql).fetchone() == (0,):
            status.failure()
        else:
            status.success()
    else:
        status.custom("[SKIP]", "yellow")

    status.write("  zoning...")
    if "ufda_zoning" not in TABLES:
        sql = dslw.utils.ImportSHP(ZONING, "ufda_zoning", "UTF-8", 2256,
                                   coerce2D=1)
        if _c.execute(sql).fetchone() == (0,):
            status.failure()
        else:
            status.success()
    else:
        status.custom("[SKIP]", "yellow")
    return


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
            #"UPDATE {} SET geocode=?, address=?, notes = 'CHANGED: {}' "
            "UPDATE {} SET address=?, notes = 'CHANGED: {}' "
            "WHERE address=?")
        if correct:
            for match in correct:
                _c.execute(update_q.format(permit_table, old_addr[0]),
                           #(match[0], match[1], old_addr[0]))
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


def dissolve_points(conn, permit_table):
    _c = conn.cursor()
    sql = ("")


def dev_density(conn, permit_table):
    _c = conn.cursor()
    duac = ("SELECT p.address, p.permit_type, z.base, p.dwellings, "
            " ST_Area(u.geometry)/43560 as sqft, "
            " FLOOR(p.dwellings/(ST_Area(u.geometry)/43560.0)) as duac "
            "FROM {} p, parcels_dis u, ufda_zoning z "
            "WHERE ST_Intersects(p.geometry, u.geometry)"
            " AND ST_Intersects(p.geometry, z.geometry)"
            " AND duac IS NOT NULL AND permit_type NOT LIKE 'BNS%' "
            "ORDER BY duac DESC").format(
                permit_table)
    rows = _c.execute(duac).fetchall()
    df = pd.DataFrame(rows, columns=["Address", "DevType", "Zoning",
                                     "Dwellings", "SqFt", "DUAC"])
    return df


# =============================================================================
# RUN IT

if __name__ == "__main__":
    # Show script info
    print(__doc__)
    print("")

    out_dir = os.getcwd()

    # Setup db
    status.write("Making/connecting to database...")
    conn = dslw.SpatialDB(db, verbose=False)
    cur = conn.cursor()
    TABLES = conn.get_tables()
    status.success()
    # print("Done")

    # Find Accela permit data (.xlsx)
    reports = [os.path.abspath(f) for f in glob("data/accela_data/*.xlsx")]
    #const_reports = [f for f in os.listdir(out_dir) if f.startswith("all") and
    #                 f.endswith(".xlsx")]
    if not reports:
        raise IOError("No Accela data found in data/accela_data/")

    print("Processing Accela data...")
    for rpt_path in reports:
        rpt_name = os.path.basename(rpt_path)
        status.write("  {}...".format(rpt_name))  # Messaging
        # Get year from filename
        year = re.findall("\d+", rpt_name)[0]
        process_accela(rpt_path)
        # Find processed output
        csv_rpt = "data/processed/res{}.csv".format(year)
        name = os.path.basename(csv_rpt).split(".")[0]
        # Load csv into SQLite db if it's not there already
        if name not in TABLES:
            dslw.csv2lite(conn, csv_rpt)
            # Add a 'notes' column
            cur.execute("ALTER TABLE {} ADD COLUMN notes TEXT".format(name))
            status.success()
        else:
            status.custom("[SKIP]", "yellow")

    # Load spatial data
    try:
        load_data(conn)
    except Exception as e:
        handle_ex(e)

    sp_tables = [t for t in conn.get_tables() if t.startswith("ufda")]
    for table in sp_tables:
        select_null = "SELECT * FROM {} WHERE geometry IS NULL".format(table)
        delete_sql = "DELETE FROM {} WHERE geometry IS NULL".format(table)
        # DELETE NULL geometry -- avoids problems
        if cur.execute(select_null).fetchall():
            status.custom("Deleting NULL geometry from {}".format(table),
                          "yellow")
            cur.execute(delete_sql)

    # Backup permit tables -- if no edits are made to the spatial data,
    #  we don't have to rerun this entire process as much! Brilliant!
    print("Backing up permit tables...")
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    # TODO: we already have the TABLES variable we could use...
    all_ptables = [t[0] for t in cur.fetchall() if t[0].startswith("res")]
    permit_tables = [t for t in all_ptables if t.startswith("res") and
                     "_" not in t]
    for table in permit_tables:
        status.write("  {}...".format(table))
        if "{}_bk".format(table) not in all_ptables:
            cur.execute("SELECT CloneTable('main', '{0}', '{0}_bk', 0)".format(
                table))
            status.success()
        else:
            status.custom("[SKIP]", "yellow")

    # Fix and make joins if permit table doesn't have a 'geometry' column
    print("Spatializing Permits...")
    for table in permit_tables:
        status.write("  {}...".format(table))
        if "geometry" not in conn.get_column_names(table):
            fix_addrs(conn, table)
            if table == 'res2014':
                raymond = ("UPDATE res2014 "
                           "SET geocode='04220014210010000' "
                           "WHERE address = '2250 RAYMOND AVE'")
                cur.execute(raymond)
            get_addr_geom(conn, table)
            get_parcel_geom(conn, table)
            status.success()
        else:
            status.custom("[SKIP]", "yellow")

    print("Generating Reports...")
    for table in permit_tables:
        status.write("  {}...".format(table))
        df = dev_density(conn, table)
        df.to_csv("reports/density_{}.csv".format(table), index=False)
        status.success()                

    print("")
    status.custom("COMPLETE", "cyan")
    raw_input("Press <Enter> to exit. ")


# =============================================================================
# =============================================================================
