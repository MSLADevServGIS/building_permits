#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
permits.py -- Building Permit Database Script
Author: Garin Wally; April-July 2016

This script processes building permit data from Accela, builds a new SQLite
database ('permits.sqlite'), inserts relevant spatial data from the
permit_features.sqlite SQLite/SpatiaLite database, and 'spatializes' those
permits by joining permits with either ufda_addr points or ufda_parcels
centroid points (technically it's not a centroid (PointOnSurface)).
See the documentation for more information.
"""

__version__ = '0.4'

import os
import re
import sys
from glob import glob

import pandas as pd

import dslw
from aside import status, handle_ex

from tools import data
from tools import process


# =============================================================================
# DATA

# Output SQLite database
DB = "permits.sqlite"

ALL_FEATURES = data.ALL_FEATURES.keys() + data.OTHER_FEATURES

FEATURES_DB = os.path.abspath(
    os.path.join(".", "data", "permit_features.sqlite"))

PERMIT_TABLES = []

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
    # Get list of PERMIT_TABLES
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    all_ptables = [t[0] for t in cur.fetchall() if "res" in t[0]]
    # Send the permit tables to the global variable declared earlier
    PERMIT_TABLES.extend([t for t in all_ptables if "_bk" not in t])
    
    for table in PERMIT_TABLES:
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
    print("Loading spatial data...")
    cur.execute("ATTACH DATABASE '{}' AS permit_features;".format(
        FEATURES_DB))
    # Load/"Clone" each feature -- this is much faster and can comfortably be
    #  done more often than a full data update (i.e. FC2FC)
    for feature in ALL_FEATURES:
        status.write("  {}...".format(feature))
        if feature in conn.get_tables():
            status.custom("[SKIP]", "yellow")
            continue
        sql = "SELECT CloneTable('permit_features', '{0}', '{0}', 1);"
        cur.execute(sql.format(feature))
        dslw.utils.reproject(conn, feature, 2256)
        cur.execute("SELECT CreateSpatialIndex('{}', 'geometry');".format(
            feature))
        status.success()

    # =========================================================================
    # CREATE AND POPULATE overrides TABLE
    status.write("Populating overrides...")
    ovr_create = ("CREATE TABLE overrides (permit_number TEXT, "
                  "address TEXT, geocode TEXT);")
    if "overrides" not in TABLES:
        cur.execute(ovr_create)
    insert_sql = "INSERT INTO overrides VALUES ('{0}', '{1}', '{2}');"
    for line in open("data/overrides.txt", 'r').readlines():
        line = line.strip()
        try:
            cur.execute(insert_sql.format(*line.split(", ")))
        except dslw.apsw.SQLError:
            pass
    status.success()

    # =========================================================================
    # SPATIALIZE PERMITS
    print("Spatializing Permits...")
    for table in PERMIT_TABLES:
        status.write("  {}...".format(table))
        cur.execute("PRAGMA table_info('{}')".format(table))
        if "geometry" not in [f[1] for f in cur.fetchall()]:
            # Call the spatialize.sql script and send it the current table
            cur.execute(open("tools/spatialize.sql", "r").read().format(table))
            # TODO: dslw.utils.execute_script(conn, "tools/spatialize.sql", table)
            cur.fetchall()
            status.success()
        else:
            status.custom("[SKIP]", "yellow")

    # =========================================================================
    # GENERATE REPORTS
    print("Generating density tables...")
    for table in PERMIT_TABLES:
        year = table.split("res")[1]
        if "cnty" in table:
            year = str(year) + "_cnty"
            # E.g. 'density2015_cnty' / 'sfrdev2015_cnty'
        status.write("  {}...".format(density_table))
        if density_table or th_dev_table not in conn.get_tables():
            # Call the spatialize.sql script and send it the current table
            cur.execute(open("tools/density.sql", "r").read().format(
                table, density_table, th_dev_table))
            # TODO: dslw.utils.execute_script(conn, "tools/spatialize.sql", table)
            cur.fetchall()
            status.success()
        else:
            status.custom("[SKIP]", "yellow")

    # =========================================================================
    # FINISH
    status.write("VACUUMing...")
    cur.execute("VACUUM;")
    status.success()

if __name__ == "__main__":
    # Show script info
    print(__doc__)
    print("")
    
    # Set working directory
    out_dir = os.getcwd()

    # RUN IT!
    try:
        main()
        print("")
        status.custom("COMPLETE", "cyan")
        raw_input("Press <Enter> to exit. ")
    except:
        handle_ex()

# =============================================================================
# =============================================================================
