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

ALL_FEATURES = data.ALL_FEATURES.keys() + ["parcels_dis"]


CITY_REPORTS = [os.path.abspath(f) for f in
                glob("data/city_permits/raw/*.xlsx")]
CNTY_REPORTS = [os.path.abspath(f) for f in
                glob("data/county_permits/raw/*.xlsx")]


# =============================================================================
# UTILITIES


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
        # Handle the 2015 Odyssey switch
        out = "data/county_permits/processed/cnty_res2015.csv"
        if "2015" in rpt_name:
            if not glob(out):
                process.combine_odyssey(
                    *[i for i in CNTY_REPORTS if "2015" in i])
            year = "2015"
        else:
            # Get year from filename
            year = re.findall("\d+", rpt_name)[0]
            # PROCESS as normal
            process.county_permits(rpt_path)
        # Find processed output
        csv_rpt = os.path.abspath(
            "data/county_permits/processed/cnty_res{}.csv".format(year))
        name = os.path.basename(csv_rpt).split(".")[0]
        # Load csv into SQLite db if it's not there already
        if name not in TABLES:
            dslw.csv2lite(conn, csv_rpt)
            # Add a 'notes' column, quitely pass if exists
            try:
                cur.execute(
                    "ALTER TABLE {} ADD COLUMN notes TEXT".format(name))
            except dslw.apsw.SQLError:
                pass
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
    print("Loadin spatial data...")
    cur.execute("ATTACH DATABASE '{}' AS permit_features;".format(
        data.FEATURES_DB))
    # Load/"Clone" each feature -- this is much faster and can comfortably be
    #  done more often than a full data update (i.e. FC2FC)
    for feature in ALL_FEAURES:
        status.write("  {}...".format(table))
        if feature in conn.get_tables():
            status.custom("[SKIP]", "yellow")
            continue
        sql = "SELECT CloneTable('{1}', '{0}', '{0}', 1);"
        cur.execute(sql.format(feature, data.FEATURES_DB))
        status.success()

    '''
    # =========================================================================
    # SPATIALIZE PERMITS
    print("Spatializing Permits...")
    for feature in ALL_FEAURES:
        status.write("  {}...".format(feature))
        if "geometry" not in conn.get_column_names(feature):
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
