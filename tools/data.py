#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
data.py -- Data updating script.
Author: Garin Wally; Aug 2016

This script defines the location of data used by the permits db / UFDA project.
It grabs the data from those specified locations and dumps it into an SQLite
database 'permit_features.sqlite' which serves as the main spatial data
repository for the aformentioned projects. The ufda and permits databases can
then ATTACH to this database and quickly "Clone" data from it -- much quicker
than waiting for arcpy.FeatureClassToFeatureClass_management().

Double-clicking this script will update the permit_features.sqlite database
which will take a while...

"""

import os
import re
from glob import glob

import pandas as pd
# NOTE: arcpy must be imported after the dslw.SpatialDB connection is made!

import dslw
from tkit.cli import StatusLine, handle_ex

status = StatusLine()


# =============================================================================
# DATA

FEATURES_DB = os.path.abspath("../data/permit_features.sqlite")

# Input geodatabase and featureclass paths
NETWORK_BASE = r"\\cityfiles\DEVServices\ArcExplorer\Data"
SDE_BASE = r"Database Connections\Features.sde\SDEFeatures.GIS."

# Dictionary of tuples that will be unpacked by '*' into arguments for
#  FeatureClassToFeatureClass_management()
ALL_FEATURES = {
    
    "ufda_parcels": (
        SDE_BASE + r"Parcels\SDEFeatures.GIS.Parcels",
        FEATURES_DB,
        "ufda_parcels"),

    "ufda_addrs": (
        os.path.join(
            NETWORK_BASE,
            r"Structures\Address.gdb\AddressStructurePoint_ft"),
        FEATURES_DB,
        "ufda_addrs"),

    "gp_bounds": (  # TODO: move to SDE
        os.path.join(
            NETWORK_BASE,
            (r"GrowthPolicy2014\Data\LandUseMap_dist.gdb\BaseFeatures"
             r"\GrowthPolicy_boundary")),
        FEATURES_DB,
        "gp_bounds"),

    "ufda_nhoods": (  # TODO: move to SDE
        (r"\\cityfiles\DEVServices\WallyG\projects\small_projects"
         r"\for_DaveGray\Transpo_update_housing"
         r"\UFDA_bound_update.gdb\UFDA_cleaned"),
        FEATURES_DB,
        "ufda_nhoods"),

    "ufda_zoning": (
        (r"\\cityfiles\DEVServices\WallyG\projects\zoning_updates\data"
         r"\MASTER_MSLAZONE.gdb\updates\MSLAZONE"),
        FEATURES_DB,
        "ufda_zoning")
    }


# =============================================================================
# UTILITIES


def load():
    print("Loading spatial data...")
    # Load all raw data
    for feature in ALL_FEATURES.keys():
        status.write("  {}...".format(feature))
        if feature not in TABLES:
            arcpy.FeatureClassToFeatureClass_conversion(
                *ALL_FEATURES[feature])
            status.success()
        else:
            status.custom("[SKIP]", "yellow")

    # parcels_dis
    status.write("  dissolved parcels...")
    if "parcels_dis" not in TABLES:
        arcpy.FeatureClassToFeatureClass_conversion(
            ALL_FEATURES["ufda_parcels"][0], "in_memory", "parcels")
        arcpy.AddField_management("in_memory/parcels", "dis_geo", "TEXT")
        arcpy.CalculateField_management(
            "in_memory/parcels", "dis_geo", "!ParcelID![:-4]", "PYTHON")
        arcpy.Dissolve_management(
            "in_memory/parcels", "in_memory/parcels_dis", "dis_geo")
        arcpy.FeatureClassToFeatureClass_conversion(
            "in_memory/parcels_dis", FEATURES_DB, "parcels_dis")
        status.success()
    else:
        status.custom("[SKIP]", "yellow")
    return


def clean():
    # (Re-)Connect to database
    status.write("Creating/connecting to database...")
    conn = dslw.SpatialDB(FEATURES_DB, verbose=False)
    cur = conn.cursor()
    status.success()

    # Fix spatial data
    print("Fixing spatial data...")
    for feature in ALL_FEATURES.keys() + ["parcels_dis"]:
        status.write("  {}...".format(feature))
        geo_column = dslw.utils.get_geo_column(conn, feature)
        dslw.utils.normalize_table(conn, feature, geo_column, 102700)
        status.success()

    cur.execute("VACUUM;")


def main():
    """Loads data and shows messages."""
    # Import arcpy
    status.write("Importing arcpy...")
    import arcpy
    status.success()

    # Load spatial data
    load()
    # Clean it (dirty E$RI)
    clean()


if __name__ == '__main__':
    print(__doc__)
    # Set up database
    status.write("Creating/connecting to database...")
    # Output SQLite database
    conn = dslw.SpatialDB(FEATURES_DB, verbose=False)
    TABLES = conn.get_tables()
    cur = conn.cursor()
    # Check if ref sys in spatial_ref_sys table
    check_ref_sys = "SELECT * FROM spatial_ref_sys WHERE auth_srid = {}"
    if not cur.execute(check_ref_sys.format(102700)).fetchone():
        conn.insert_srid(102700)
    conn.close()
    status.success()
    try:
        main()
        print("")
        status.custom("COMPLETE", "cyan")
        raw_input("Press <Enter> to exit.")
    except:
        handle_ex()


