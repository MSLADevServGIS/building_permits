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

import os
import re
import sys
from glob import glob

import yaml
import pandas as pd

import dslw
from aside import status, handle_ex


__version__ = '0.4'


# =============================================================================
# DATA

FEATURES_DB = os.path.abspath("./data/permit_features.sqlite")
FEATURES_DB = FEATURES_DB.replace("\\", "/")

DATA_SOURCES = ("//cityfiles/DEVServices/WallyG/projects"
                "/building_permits/data/data_sources.yaml")

# Input geodatabase and featureclass paths
NETWORK_BASE = r"\\cityfiles\DEVServices\ArcExplorer\Data"
SDE_BASE = r"Database Connections\Features.sde\SDEFeatures.GIS."

# Dictionary of tuples that will be unpacked by '*' into arguments for
#  FeatureClassToFeatureClass_management()
ALL_FEATURES = yaml.load(open(DATA_SOURCES, "r"))
# Add the database name as the second (index 1) item in the list
for key in ALL_FEATURES.keys():
    ALL_FEATURES[key].insert(1, FEATURES_DB)

# Include derrived data
OTHER_FEATURES = [
    "condos_dis"
    ]


# Output SQLite database
DB = "permits.sqlite"

ALL_FEATURES = ALL_FEATURES.keys() + OTHER_FEATURES

FEATURES_DB = os.path.abspath(
    os.path.join(".", "data", "permit_features.sqlite"))

PERMIT_TABLES = []

CITY_REPORTS = [os.path.abspath(f) for f in
                glob("data/city_permits/raw/*.xlsx")]
CNTY_REPORTS = [os.path.abspath(f) for f in
                glob("data/county_permits/raw/*.xlsx")]


# =============================================================================
# VARS

# Output reports
CITY_OUT = "data/city_permits/processed/city_res{}.csv"
# com_report = "com{}.csv"
# pub_report = "pub{}.csv"

# All construction permit codes
res_codes = {
    'BNMRA': "New Multifamily 3-4 Units",
    'BNMRB': "New Multifamily 5+ Units",  # Technically COM permit
    'BNCON': "New Other",  # Mixed use
    'BNRDX': "New Duplex",
    'BNSFR': "New Single Family Residence",
    'BNSFT': "New Single Family Townhouse",
    'BNROS': "New Shelter/Dorm/Etc",
    'BAARC': "Add/Alter/Remodel Commercial",
    '': "None specified",
    'None': "None specified"
    }

com_codes = {
    'BNCON': "New Other",  # Mixed use, see res_codes
    'BNCOP': "New Office/Bank/Professional Building",
    'BNCSC': "New Store/Customer Service",
    'BNCSS': "New Service Station/Repair Garage",
    'BNCID': "New Industrial",
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
# COUNTY VARIABLES

CNTY_OUT = "data/county_permits/processed/cnty_res{}.csv"

CITIES = ["missoula", "bonner"]  # TODO: Lolo, French Town, Piltzville???

UNITS = {
    "sfr": 1,
    "sf": 1,
    "single": 1,
    "duplex": 2,
    "multi": "???"
    }

RENAMED_COLUMNS = {
    "Permit Id": "permit_number",
    "Geo Code": "geocode",
    "Issued Date": "permit_issued_date",
    "Property Address": "address",
    "Type Of Work": "permit_type",
    "Description": "description",
    "Property City": "city"
    }

ORDERED_COLUMNS = [
    "permit_number",
    "geocode",
    "permit_issued_date",
    "address",
    "dwellings",
    "permit_type",
    "description",
    "city"
    ]

# NOTE: "(?i)" is the regular expression for case independant
DESC_KEYWORDS = re.compile("(?i)" + "|".join(k for k in UNITS.keys()))

NA_VALUES = ["na", "n/a", "NA", "N/A", "nan"]


def calc_units(x):
    if re.findall(DESC_KEYWORDS, x):
        return UNITS[re.findall(DESC_KEYWORDS, x)[0].lower()]
    else:
        return 0


# =============================================================================
# PROCESSING FUNCTIONS

def city_permits(all_permits):
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
    #all_const['address'] = all_const['address'].apply(
    #    lambda x: x.split(" #")[0].strip())

    # Add City column to improve geocoding results -- not used anymore
    all_const["city"] = "Missoula"

    # Select all records that don't have 'MSTR' in the address
    all_const = all_const[~all_const.address.str.contains("MSTR")]

    # Sort data
    all_const = all_const.sort(["permit_number", "address", "dwellings"])

    # =========================================================================
    # GET PERMIT YEAR / ADD YEAR TO OUTPUT REPORT

    years = set()
    all_const["permit_issued_date"].apply(lambda x: years.add(x.year))
    assert len(years) == 1, \
        "Input data shall only consist of one calendar year"

    year = years.pop()

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
         (all_const["construction_type"] == "Commercial Construction") &
         (all_const["permit_type"].isin(res_codes.keys()))) |
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
    res_const.to_csv(CITY_OUT.format(year), index=False)

    '''
    com_out = com_const.groupby('permit_number').first().reset_index()
    com_out.to_excel(com_report, index=False)

    pub_out = pub_const.groupby('permit_number').first().reset_index()
    pub_out.to_excel(pub_report, index=False)
    '''
    return res_const


def county_permits(permits, out=True):
    """Processes County building permits in the Odyssey-system format."""
    # Only accept XLSX files
    if not permits.lower().endswith(".xlsx"):
        raise IOError("Input must be manually cleaned and converted to XLSX")
    # Get year from filename
    year = re.findall("\d+", permits)[0]
    # Read the data
    df = pd.read_excel(permits)
    # Convert NA_VALUES to real NaN (technically a pandas subclass of float)
    df = df.applymap(lambda x: pd.np.nan if x in NA_VALUES else x)
    # Rename columns and drop those that aren't listed in the rename process
    df.rename(columns=RENAMED_COLUMNS, inplace=True)
    [df.drop(col, 1, inplace=True) for col in df.columns
     if col not in RENAMED_COLUMNS.values()]
    # and drop rows where all values are NaN
    df.dropna(how="all", inplace=True)

    # Capitalize addresses
    df["address"] = df["address"].str.upper()
    # Convert description field to str
    df["description"] = df["description"].astype(str)
    # Clean geocodes (convert to str, and remove dashes (-))
    df["geocode"] = df["geocode"].astype(str)
    df["geocode"] = df["geocode"].apply(lambda x: x.replace("-", ""))

    # Calculate dwellings
    df["dwellings"] = df["description"].apply(calc_units)

    # Query out New Construction, in nearby CITIES, that contain DESC_KEYWORDS
    res_const = df[(df["permit_type"] == "New Construction") &
                   (df["city"].apply(lambda x: x.lower() in CITIES)) &
                   (df["description"].str.contains(DESC_KEYWORDS))].copy()

    # Standardize date column
    res_const["permit_issued_date"] = pd.to_datetime(
        res_const["permit_issued_date"], infer_datetime_format=True)
    # Order columns and sort by date
    res_const = res_const[ORDERED_COLUMNS].sort("permit_issued_date")
    if out:
        res_const.to_csv(CNTY_OUT.format(year), index=False)
    return res_const


def combine_odyssey(permits1, permits2, output_intermediate=False):
    """Used for the 2015 conversion to Odyssey permit system."""
    # Process and combine the two 2015 permit sets
    df_one = county_permits(permits1, output_intermediate)
    df_two = county_permits(permits2, output_intermediate)
    full_df = df_one.append(df_two)
    # Standardize date column
    full_df["permit_issued_date"] = pd.to_datetime(
        full_df["permit_issued_date"], infer_datetime_format=True)
    # Order columns and sort by date
    full_df = full_df[ORDERED_COLUMNS].sort("permit_issued_date")
    full_df.to_csv(CNTY_OUT.format("2015"), index=False)
    return

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
        density_table = "density{}".format(year)
        th_dev_table = "th_dev{}".format(year)
        if "cnty" in table:
            density_table = "cnty" + density_table
            th_dev_table = "cnty" + th_dev_table
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
