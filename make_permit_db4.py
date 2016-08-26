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
import re
import sys
from glob import glob

import pandas as pd

import dslw
from tkit.cli import StatusLine, handle_ex

from tools import data
from tools import process


status = StatusLine()


# =============================================================================
# DATA

# Output SQLite database
DB = "permits.sqlite"


'''
PARCELS_DIS = os.path.abspath("data/shps/ufda_parcels_dis")
ZONING = os.path.abspath("data/shps/zoning")
GDB = "data/permit_features.gdb"
UFDA_NHOODS = (GDB, "ufda_nhoods")
UFDA_ADDRS = (GDB, "ufda_addrs")
UFDA_PARCELS = (GDB, "ufda_parcels")
'''

CITY_REPORTS = [os.path.abspath(f) for f in
                glob("data/city_permits/raw/*.xlsx")]
CNTY_REPORTS = [os.path.abspath(f) for f in
                glob("data/county_permits/raw/*.xlsx")]


# =============================================================================
# UTILITIES


# TODO: new permit_features.sqlite db
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
        _c.execute(sql)
        _c.execute("UPDATE parcels_dis "
                   "SET geometry=CastToMultiPolygon(ST_MakeValid(geometry)) "
                   "WHERE IsValid(geometry) <> 1;")
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


def main():
    # Setup db
    status.write("Making/connecting to database...")
    conn = dslw.SpatialDB(DB, verbose=False)
    cur = conn.cursor()
    TABLES = conn.get_tables()
    status.success()

    # =========================================================================
    # PROCESS CITY
    print("Processing City permits...")
    for rpt_path in CITY_REPORTS:
        rpt_name = os.path.basename(rpt_path)
        status.write("  {}...".format(rpt_name))
        # Get year from filename
        year = re.findall("\d+", rpt_name)[0]
        process.city_permits(rpt_path)
        # Find processed output
        csv_rpt = os.path.abspath(
            "data/city_permits/processed/city_res{}.csv".format(year))
        name = os.path.basename(csv_rpt).split(".")[0]
        # Load csv into SQLite db if it's not there already
        if name not in TABLES:
            dslw.csv2lite(conn, csv_rpt)
            # Add a 'notes' column
            cur.execute("ALTER TABLE {} ADD COLUMN notes TEXT".format(name))
            status.success()
        else:
            status.custom("[SKIP]", "yellow")

    # =========================================================================
    # PROCESS COUNTY
    print("Processing County permits...")
    for rpt_path in CNTY_REPORTS:
        rpt_name = os.path.basename(rpt_path)
        status.write("  {}...".format(rpt_name))
        # 2015 Odyssey switch
        out = "data/county_permits/processed/cnty_res2015.csv"
        if "2015" in rpt_name:
            if glob(out):
                status.custom("[SKIP]", "yellow")
                continue
            process.combine_odyssey(*[i for i in CNTY_REPORTS if "2015" in i])
            year = "2015"
        else:
            # Get year from filename
            year = re.findall("\d+", rpt_name)[0]
            # PROCESS
            process.county_permits(rpt_path)
        # Find processed output
        csv_rpt = os.path.abspath(
            "data/county_permits/processed/cnty_res{}.csv".format(year))
        name = os.path.basename(csv_rpt).split(".")[0]
        # Load csv into SQLite db if it's not there already
        if name not in TABLES:
            dslw.csv2lite(conn, csv_rpt)
            # Add a 'notes' column
            cur.execute("ALTER TABLE {} ADD COLUMN notes TEXT".format(name))
            status.success()
        else:
            status.custom("[SKIP]", "yellow")

    # =========================================================================
    # BACKUP PERMIT TABLES
    print("Backing up permit tables...")
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    all_ptables = [t[0] for t in cur.fetchall() if "res" in t[0]]
    permit_tables = [t for t in all_ptables if "_bk" not in t]
    for table in permit_tables:
        status.write("  {}...".format(table))
        if "{}_bk".format(table) not in all_ptables:
            cur.execute("SELECT CloneTable('main', '{0}', '{0}_bk', 1)".format(
                table))
            status.success()
        else:
            status.custom("[SKIP]", "yellow")

    # =========================================================================
    # LOAD SPATIAL DATA
    # ATTACH the database containing the spatial data
    cur.execute("ATTACH DATABASE '{}' AS permit_features;".format(
        data.FEATURES_DB))
    # Load/"Clone" each feature -- this is much faster and can comfortably be
    #  done more often than a full data update (i.e. FC2FC)
    for feature in data.ALL_FEAURES.keys():
        sql = ("SELECT CloneTable('{1}', '{0}', "
               "'{0}', 1);")
        cur.execute(sql.format(feature, data.FEATURES_DB))

        select_null = "SELECT * FROM {} WHERE geometry IS NULL".format(feature)
        delete_sql = "DELETE FROM {} WHERE geometry IS NULL".format(feature)
        # DELETE NULL geometry -- avoids problems
        if cur.execute(select_null).fetchall():
            status.custom("Deleting NULL geometry from {}".format(feature),
                          "yellow")
            cur.execute(delete_sql)
    '''


    # Fix and make joins if permit table doesn't have a 'geometry' column
    print("Spatializing Permits...")
    for table in permit_tables:
        status.write("  {}...".format(table))
        if "geometry" not in conn.get_column_names(table):
            cur.execute("SELECT AddGeometryColumn(?, 'geometry', "
                        "2256, 'MULTIPOINT', 'XY', 0);", (table,))
            cur.execute(open("tools/density.sql", "r").read().format(table))
            status.success()
        else:
            status.custom("[SKIP]", "yellow")

    status.write("Generating density report...")
    cur.execute(open("tools/density.sql", "r").read())
    status.success()
    '''
    print("")
    status.custom("COMPLETE", "cyan")
    raw_input("Press <Enter> to exit. ")


if __name__ == "__main__":
    # Show script info
    print(__doc__)
    print("")
    
    # Set working directory
    out_dir = os.getcwd()

    # RUN IT!
    try:
        main()
    except:
        handle_ex()

# =============================================================================
# =============================================================================
