#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
permits.py -- Building Permit Database Script
Author: Garin Wally; April-July 2016

This script processes building permit data from Accela builds a new SQLite database and inserts relevant featureclasses
from a geodatabase.
"""


#import argparse
import os
import sys
import traceback
import re
from glob import glob

import pandas as pd
from termcolor import cprint

import dslw


# =============================================================================
# DATA

# Output SQLite database
db = "permits.sqlite"

# Input geodatabase and featureclasses
GDB = "permit_features.gdb"
UFDA_NHOODS = (GDB, "ufda_nhoods")
UFDA_ADDRS = (GDB, "ufda_addrs")
UFDA_PARCELS = (GDB, "ufda_parcels")


# =============================================================================
# VARS

# Output reports
RES_REPORT = "res{}.csv"
com_report = "com{}.csv"
pub_report = "pub{}.csv"

# All construction permit codes
res_codes = {
    'BNMRA': "New Multifamily 3-4 Units",
    'BNMRB': "New Multifamily 5+ Units",
    'BNRDX': "New Duplex",
    'BNSFR': "New Single Family Residence",
    'BNSFT': "New Single Family Townhouse",
    'BNROS': "New Shelter/Dorm/Etc",
    '': "None specified"
    }

com_codes = {
    'BNCOP': "New Office/Bank/Professional Building",
    'BNCSC': "New Store/Customer Service",
    'BNCSS': "New Service Station/Repair Garage",
    'BNCID': "New Industrial",
    'BNCON': "New Other",
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
# FUNCTIONS


# TODO: setup_data
# TODO: rename make_reports -> process_accela?

def make_reports(all_permits):
    """Cleans, preps, and exports building permit reports."""
    # Open raw constuction-permit report as DataFrame 'all_const'
    all_const = pd.read_excel(all_permits)

    # =========================================================================
    # CLEAN

    # Drop Null columns
    all_const.dropna(axis=1, how='all', inplace=True)

    # Rename cols from values in row 3: lowercase and replace spaces with "_"
    all_const.columns = all_const.ix[3].apply(
        lambda x: x.lower().replace(" ", "_"))

    # Shorten 'dwellings' column name
    all_const.rename(columns={"number_of_dwellings": "dwellings"},
                     inplace=True)

    # Rename index column 'ix'
    all_const.columns.name = 'ix'

    # Drop headers in rows 0-3
    all_const.drop([0, 1, 2, 3], inplace=True)

    # Remove Subtype field descriptions
    all_const['subtype'].fillna("None", inplace=True)
    all_const['subtype'] = all_const['subtype'].apply(
        lambda x: x.split(" ")[0])

    # Convert NULL dwellings to 0
    all_const['dwellings'].fillna(0, inplace=True)
    # Convert Dwellings to integer
    all_const['dwellings'] = all_const['dwellings'].apply(lambda x: int(x))

    # Convert NULL addresses to ""
    all_const['address'].fillna("", inplace=True)

    # Convert Geocode to text
    all_const['geocode'] = all_const['geocode'].apply(lambda x: str(x))

    # Deal with non-unique addresses
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
        # Residential construction with >= 3 units is filed as commercial
        ((all_const["dwellings"] >= 3) &
            (all_const["construction_type"] == "Commercial Construction")) |
        # Get residential construction of only listed subtypes and
        #   dwellings >= 1
        ((all_const["subtype"].isin(res_codes.keys()) &
            all_const["dwellings"] >= 1))
        # Finally, groupby permit number, remove duplicates and fix index col
        ].groupby("permit_number").first().reset_index()

    # TODO:
    #com_const = all_const[all_const['subtype'].isin(com_codes.keys())]
    #pub_const = all_const[all_const['subtype'].isin(pub_codes.keys())]

    # Export
    #res_out = res_const.groupby('permit_number').first().reset_index()
    #res_out.to_excel(res_report, index=False)
    res_const.to_csv(res_report, index=False)

    # TODO:
    '''
    com_out = com_const.groupby('permit_number').first().reset_index()
    com_out.to_excel(com_report, index=False)

    pub_out = pub_const.groupby('permit_number').first().reset_index()
    pub_out.to_excel(pub_report, index=False)
    '''
    return


def load_data(conn, permit_csv):
    """Loads data and shows messages."""
    print("Loading permits...")
    dslw.csv2lite(conn, permit_csv)
    print("Done")
    
    print("Loading ufda neighborhoods...")
    dslw.addons.ogr2lite(conn, UFDA_NHOODS)
    print("Done")
    
    print("Loading address points...")
    dslw.addons.ogr2lite(conn, UFDA_ADDRS)
    print("Done")
    
    print("Loading parcels...")
    dslw.addons.ogr2lite(conn, UFDA_PARCELS)
    print("Done")
    return


# EDITED
# All permits should, at least, join to a parcel (condos and townhomes won't)
# Do all permit geocodes match existing parcel parcelids?
def get_outer_join(conn, permit_table, geom_table="ufda_parcels"):  # DONT USE ADDRs
    """Get members of outer left join."""
    _c = conn.cursor()
    join_qry = ("SELECT {0}.address, {0}.geocode, {1}.parcelid "
                "FROM {0} LEFT OUTER JOIN {1} "
                "ON {0}.geocode={1}.parcelid "
                "WHERE {1}.parcelid IS NULL").format(permit_table, geom_table)
    outer = _c.execute(join_qry).fetchall()
    return outer


# Only fix permit geocode by exact address's geocode
def fix_outer_joins(conn, permit_table):
    """Attempts to fix unjoined members by matching addresses.

    Permit address must exactly match ufda_addrs.fulladdress.
    """
    _c = conn.cursor()
    all_unjoined = get_outer_join(conn, permit_table, "ufda_parcels")
    for unjoined in all_unjoined:
        if not unjoined[0]:
            continue
        get_geo_qry = ("SELECT parcelid FROM ufda_addrs "
                       "WHERE fulladdress = '{}'").format(unjoined[0])
        update_geo = _c.execute(get_geo_qry).fetchone()
        if not update_geo:
            continue
        _c.execute("UPDATE {} SET geocode=? "
                    "WHERE address=?".format(permit_table),
                    (update_geo[0], unjoined[0]))
    new_unjoined = get_outer_join(conn, permit_table, "ufda_parcels")
    return "Fixed {} of {}".format(
        len(all_unjoined)-len(new_unjoined), len(all_unjoined))


# EDITED
def get_outer_addr_joins(conn, permit_table):
    """Get members of outer left join."""
    _c = conn.cursor()
    join_qry = ("SELECT {0}.address, {0}.geocode, ufda_addrs.parcelid "
                "FROM {0} LEFT OUTER JOIN ufda_addrs "
                "ON {0}.address=ufda_addrs.fulladdress "
                "WHERE ufda_addrs.parcelid IS NULL").format(permit_table)
    outer = _c.execute(join_qry).fetchall()
    return outer

'''
def repair_geocodes(connection, permit_table):
    """Attempts to fix mis-matched permit addresses and geocodes."""
    _c = connection.cursor()
    # Get the permits that didn't join with parcels
    permit_left_join = """
        SELECT t..address, t..geocode, ufda_parcels.parcelid
        FROM t.
        LEFT JOIN ufda_parcels ON t..geocode = ufda_parcels.parcelid
        WHERE ufda_parcels.parcelid IS NULL""".replace(
            "t.", "{}".format(permit_table))
    unjoined = _c.execute(permit_left_join).fetchall()
    # Get all county structure addresses
    addr_qry = "SELECT fulladdress, parcelid FROM ufda_addrs"
    addrs = _c.execute(addr_qry).fetchall()
    # Update procedure to fix bad geocodes & addresses
    update_qry = "UPDATE t. SET address=?, geocode=? WHERE address = ?"
    update_qry = update_qry.replace("t.", permit_table)
    for permit in unjoined:
        s = permit[0].split(" ")
        if "-" in s[0]:
            s[0] = s[0].split("-")[0]
        try:
            update_values = addrs[[r[0].startswith(s[0]) and
                                   r[0].endswith(" ".join(s[-2:]))
                                   for r in addrs].index(True)]
            _c.execute(update_qry, (update_values + (permit[0],)))
        except ValueError:
            pass
    new_unjoined = _c.execute(permit_left_join).fetchall()
    if not new_unjoined:
        return True
    print(new_unjoined)
    return False
'''

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
        suffixes = re.compile("(AVE|ST|RD|WAY|DR|LN|CT|PL|BLVD|LP|RISE)(?!\w)")
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
        # Remove single-letter at end of string (usually direction heading or APT)
        try:
            a = a.replace(re.findall(" \D{1}$", a)[0], "")
        except IndexError:
            pass
        self.st_numb = re.findall("\d+ ", a)[0].strip()
        self.st_name = re.findall("\w{3,} ", a)[-1].strip()
        # Likely a single mistake '03RD ST'
        if self.st_name.startswith("0"):
            self.st_name = st_name.replace("0", "")
        return [self.st_numb, self.st_name, self.st_suffix]
        
    def __repr__(self):
        return str(self.parsed)


def LikeAddr(a, sql=True):
    """Parses address into an SQL LIKE query value."""
    # To list
    addr_split = a.split()
    # Take the left side of '-' if exists in index 0
    if "-" in addr_split[0]:
        addr_split[0] = addr_split[0].split("-")[0]
        a = " ".join(addr_split)
    # Remove appartment
    try:
        a = a.replace(re.findall("APT \w+", a)[0], "")
    except IndexError:
        pass
    # Remove single-letter at end of string (usually direction heading or APT)
    try:
        a = a.replace(re.findall(" \D{1}$", a)[0], "")
    except IndexError:
        pass
    st_numb = re.findall("\d+ ", a)[0].strip()
    st_name = re.findall("\w{3,} ", a)[-1].strip()
    # Likely a single mistake '03RD ST'
    if st_name.startswith("0"):
        st_name = st_name.replace("0", "")
    addr_qry = "'{} %{} %'".format(st_numb, st_name)
    return addr_qry

'''
def repair_geocodes(connection, permit_table):
    """Attempts to fix mis-matched permit addresses and geocodes."""
    _c = connection.cursor()
    addrs = set(_c.execute("SELECT fulladdress FROM ufda_addrs"))
    parcels = set(_c.execute("SELECT parcelid FROM ufda_parcels"))

    permit_geo = set(_c.execute("SELECT geocode FROM {}".format(permit_table)))
    fix_geos = list(permit_geo.difference(parcels))
    for geo in fix_geos:
        fix_addr = _c.execute("""
            SELECT fulladdress, parcelid
            FROM ufda_addrs WHERE parcelid = ?""", (geo[0],)).fetchall()
        for fxa in fix_addr:
            _c.execute("UPDATE {} SET geocode=? WHERE address=?".format(permit_table),
                       (fxa[1], fxa[0]))
'''

# Fix geocode and address of permit using matching parsed address
def fix_by_addrs(conn, permit_table):
    """Fixes data by parsing and matching permit addr to ufda_addrs."""
    _c = conn.cursor()
    addrs = set(_c.execute("SELECT fulladdress FROM ufda_addrs"))
    parcels = set(_c.execute("SELECT parcelid FROM ufda_parcels"))    
    permit_addr = set(_c.execute("SELECT address FROM {}".format(permit_table)))
    # Addresses that exist in permits and not in the address points
    fix_addrs = list(permit_addr.difference(addrs))
    bad_addrs = len(fix_addrs)
    for old_addr in fix_addrs:
        if not old_addr[0]:
            continue
        addr_qry = ("SELECT parcelid, fulladdress FROM ufda_addrs "
                    "WHERE fulladdress LIKE {}".format(LikeAddr(old_addr[0])))
        correct = _c.execute(addr_qry).fetchall()
        update_q = (
            "UPDATE {} SET geocode=?, address=?, description = 'CHANGED: {}' "
            "WHERE address=?")
        if correct:
            for match in correct:
                _c.execute(update_q.format(permit_table, old_addr[0]),
                          (match[0], match[1], old_addr[0]))
    new_addrs = set(_c.execute("SELECT address FROM {}".format(permit_table)))
    not_fixed = len(list(new_addrs.difference(addrs)))
    total_fixed = bad_addrs - not_fixed
    # print("Fixed {} of {} addresses".format(total_fixed, bad_addrs))
    # Doesn't solve all problems so 'force'
    return


# Try to fix geocode of permit where address isn't in ufda_addrs
def force_match_addr(conn, permit_table):
    """Changes the permit address to match ufda_addr JOINed with ufda_parcels"""
    _c = conn.cursor()
    for row in get_outer_addr_joins(conn, permit_table):
        q = ("SELECT fulladdress, parcelid "
             "FROM ufda_addrs "
             "WHERE parcelid = '{}'").format(row[1])
        matches = _c.execute(q).fetchall()
        if not matches:
            continue
        elif type(matches) is list:
            lens = [len(r[0]) for r in matches]
            shortest = matches[lens.index(min(lens))]
            matches = shortest
        update = ("UPDATE {} SET address = ?, description = 'CHANGED: {}' "
                  "WHERE geocode = ?").format(permit_table, row[0])
        print(row, matches)
        _c.execute(update, (matches[0], row[1]))
    # Still doesn't resolve addresses that don't exists in ufda_addrs
    return


# Function to geocode (verb) addresses that don't exist in ufda_addrs,
#   select the intersecting parcel, and update either the geocode and/or addr
# WONT WORK cause Nominatim sucks

def matches_parcels(conn, permit_table):
    _c = conn.cursor()
    check = """
    SELECT {0}.geocode
    FROM {0} JOIN ufda_parcels
    ON {0}.geocode = ufda_parcels.parcelid
    """.format(permit_table)  # rm 'DISTINCT'
    all_permits = "SELECT * FROM {}".format(permit_table)
    n_permits = len(_c.execute(all_permits).fetchall())
    addr_joins = len(_c.execute(check).fetchall())
    if addr_joins == n_permits:
      return True
    return "{} joins of {}".format(addr_joins, n_permits)


def matches_addrs(conn, permit_table):
    _c = conn.cursor()
    check = """
    SELECT {0}.geocode
    FROM {0} JOIN ufda_addrs
    ON {0}.address = ufda_addrs.fulladdress
    """.format(permit_table)  # rm 'DISTINCT'
    all_permits = "SELECT * FROM {}".format(permit_table)
    n_permits = len(_c.execute(all_permits).fetchall())
    addr_joins = len(_c.execute(check).fetchall())
    if addr_joins == n_permits:
      return True
    return "{} joins of {}".format(addr_joins, n_permits)


def match_addr_procedure(conn, permit_table):
    fix_outer_joins(conn, permit_table)
    fix_by_addrs(conn, permit_table)
    force_match_addr(conn, permit_table)
    if matches_addrs(conn, permit_table) is True:
        return True
    return False


'''
import dslw
from permits import *

conn = dslw.SpatialDB("permits.sqlite")
cur = conn.cursor()


'''

# =============================================================================
# RUN IT

if __name__ == "__main__":
    print(__doc__)
    print("")

    out_dir = os.getcwd()

    # Setup db
    if os.path.exists(os.path.abspath(db)):
        cprint("IOError: Database already exists.", "red")
        print("")
        raw_input("Press <Enter> to exit. ")
        sys.exit(1)
    print("Making database...")
    conn = dslw.SpatialDB(db, verbose=False)
    cur = conn.cursor()
    print("Done")

    # Process Accela permit data
    # TODO: make a reports folder?
    const_reports = [f for f in os.listdir(out_dir) if f.startswith("all") and
                     f.endswith(".xlsx")]

    print("Processing reports...")
    for rpt in const_reports:
        print("  {}...".format(rpt))
        year = re.findall("\d+", rpt)[0]
        make_reports(rpt)
        csv_rpt = "res{}.csv".format(year)
        dslw.csv2lite(conn, csv_rpt)
        print("Done")


    # Load data
    print("Loading ufda neighborhoods...")
    dslw.addons.ogr2lite(conn, UFDA_NHOODS)
    print("Done")

    #print("Go get a sandwich, this will take some time...")
    print("Loading address points...")
    dslw.addons.ogr2lite(conn, UFDA_ADDRS)
    print("Done")
    
    print("Loading parcels...")
    dslw.addons.ogr2lite(conn, UFDA_PARCELS)
    print("Done")

    # Fix geocodes & addrs
    #print("Repairing permit geocodes...")
    #for rpt in const_reports:
    #    repair_geocodes(conn, csv_rpt.split(".")[0])
    #print("Done")

    print("")
    #status.custom("COMPLETE", "cyan")
    raw_input("Press <Enter> to exit. ")
