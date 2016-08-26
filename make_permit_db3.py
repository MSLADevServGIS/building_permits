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
from glob import glob

import pandas as pd

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

    # Drop rows 0-3 which are just headings
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
    '''
    com_const = all_const[all_const['subtype'].isin(com_codes.keys())]
    pub_const = all_const[all_const['subtype'].isin(pub_codes.keys())]
    '''
    # Export
    res_out = res_const.groupby('permit_number').first().reset_index()
    # res_out.to_excel(res_report, index=False)
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
    # const_reports = [f for f in os.listdir(out_dir) if f.startswith("all") and
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
    sp_tables.append('parcels_dis')
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
            cur.execute("SELECT AddGeometryColumn(?, 'geometry', "
                        "2256, 'MULTIPOINT', 'XY', 0);", (table,))
            cur.execute(open("tools/density.sql", "r").read().format(table))
            status.success()
        else:
            status.custom("[SKIP]", "yellow")

    status.write("Generating density report...")
    cur.execute(open("tools/density.sql", "r").read())
    status.success()

    print("")
    status.custom("COMPLETE", "cyan")
    raw_input("Press <Enter> to exit. ")


# =============================================================================
# =============================================================================
