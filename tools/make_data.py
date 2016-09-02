#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
make_data.py -- Data updating script.
Author: Garin Wally; Aug 2016

This script grabs the data from those locations specified in 'data.py' and
dumps it into an SQLite database 'permit_features.sqlite' which serves as the
main spatial data repository for the UFDA and building permit projects.
The ufda and permits databases can then ATTACH to this database and quickly
"Clone" data from it -- much quicker than waiting for the slow and dirty
arcpy.FeatureClassToFeatureClass_management().

Double-clicking this script will create the permit_features.sqlite database
which will take a while...
Go to lunch or something.

"""

import os
import re
from glob import glob
import subprocess

import pandas as pd
# NOTE: arcpy must be imported after the dslw.SpatialDB connection is made!

import dslw
from tkit.cli import StatusLine, handle_ex, nix, nix_decorator, wait

import data

status = StatusLine()


# =============================================================================
# UTILITIES


@nix_decorator
def make_db():
    """Makes the sqlite db.
    Msg:
        Create/connect to database
    """
    # Set up database
    #status.write("Creating/connecting to database...")
    # Output SQLite database
    conn = dslw.SpatialDB(data.FEATURES_DB, verbose=False)
    global TABLES
    TABLES = conn.get_tables()
    cur = conn.cursor()
    # Check if ref sys in spatial_ref_sys table
    check_ref_sys = "SELECT * FROM spatial_ref_sys WHERE auth_srid = {}"
    if not cur.execute(check_ref_sys.format(102700)).fetchone():
        conn.insert_srid(102700)
    cur.close()
    conn.close()
    del conn, cur
    #status.success()


def load():
    print("Loading spatial data...")
    sr = arcpy.SpatialReference(2256)  # Montana St Plane that QGIS can read
    # Load all raw data
    for feature in data.ALL_FEATURES.keys():
        status.write("  {}...".format(feature))
        if feature not in TABLES:
            # Project layer into memory
            arcpy.Project_management(
                data.ALL_FEATURES[feature][0],  # Data path
                "in_memory/{}".format(feature),
                sr)
            arcpy.FeatureClassToFeatureClass_conversion(
                "in_memory/{}".format(feature),
                *data.ALL_FEATURES[feature][1:])  # Ouput loc and name
            status.success()
        else:
            status.custom("[SKIP]", "yellow")

    # condos_dis
    status.write("  dissolved condos...")
    if "condos_dis" not in TABLES:
        # Project layer into memory
        arcpy.Project_management(
                data.SDE_BASE + r"Parcels\SDEFeatures.GIS.All_Condos",
                "in_memory/condos",
                sr)
        # Dissolve by townhome/condo name
        arcpy.Dissolve_management(
            "in_memory/condos", "in_memory/condos_dis", "Name")
        # Copy to the output database
        arcpy.FeatureClassToFeatureClass_conversion(
            "in_memory/condos_dis", data.FEATURES_DB, "condos_dis")
        status.success()
    else:
        status.custom("[SKIP]", "yellow")
    return


def main():
    """Loads data and shows messages."""
    # Prep
    make_db()
    
    # Import arcpy
    nix.write("Import arcpy")
    import arcpy
    nix.ok()
    print("")

    # Load spatial data
    load()


if __name__ == '__main__':
    print(__doc__)
    try:
        main()
        print("")
        status.custom("COMPLETE", "cyan")
        wait()
    except:
        handle_ex()


