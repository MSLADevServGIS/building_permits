#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys

import dslw


YEAR = sys.argv[1]

DB = "permits.sqlite"
CONN = dslw.SpatialDB(DB)

def region_summary(year, aggregation_feature, agg_name_field):
    cur = CONN.cursor()
    cur.execute(open("tools/region_summary.sql", "r").read().format(
        year, aggregation_feature, agg_name_field))
    return


if __name__ == "__main__":
    region_summary(YEAR, "ufda_nhoods", "nhood_name")
'''
    sql = ("SELECT nhood_name, SUM(sum_dwellings) "
           "FROM"
           "  (SELECT * FROM sfr_rs{0} "
           "   UNION "
           "   SELECT * FROM sfr_rs2015_cnty) "
           "GROUP BY nhood_name;").format(YEAR)
    try:
        cur = CONN.cursor()
        cur.execute(sql)
    except:
        pass
'''
