#!/usr/bin/env python
# -*- coding: utf-8 -*-

import re
import pandas as pd


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

CITIES = ["missoula", "bonner", "huson"]

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
