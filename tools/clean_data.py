#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
clean_data.py -- Data cleaning script.
Author: Garin Wally; Aug 2016

Once the data has been created, this script 'cleans' the data using a series
of SQL statements that fix the tables created by arcpy.FC2FC.

"""

import dslw
from aside import status, handle_ex, nix, wait

import data


# =============================================================================
# UTILITIES

def main():
    conn = dslw.SpatialDB(data.FEATURES_DB, verbose=False)
    cur = conn.cursor()
    nix.info("Connected to database")
    print("")
    print("Cleaning features...")
    features = data.ALL_FEATURES.keys()
    features.extend(data.OTHER_FEATURES)
    for feature in features:
        status.write("  normalizing {}...".format(feature))
        geo_col = dslw.utils.get_geo_column(conn, feature)
        dslw.utils.normalize_table(conn, feature, geo_col, 102700)
        dslw.utils.reproject(conn, feature, 2256)
        status.success()


if __name__ == '__main__':
    print(__doc__)
    try:
        main()
        print("")
        status.custom("COMPLETE", "cyan")
        wait()
    except:
        handle_ex()


