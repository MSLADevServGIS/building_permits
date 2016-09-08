# -*- coding: utf-8 -*-
"""
Created on Wed Apr 27 10:18:22 2016

@author: wallyg
"""

import os

import arcpy

arcpy.env.workspace = "in_memory"

# Housing table fields
ho_avg = "HOUSEHOLD_POP_TYPE.HD01_S04"
r_avg = "HOUSEHOLD_POP_TYPE.HD01_S07"


# LOAD DATA INTO MEMORY
resconst_points = os.path.join(r"\\cityfiles\DEVServices\WallyG\projects",
                               r"building_permits\2011-2015",
                               r"Permits_2011-2015.gdb\ResConst")

census_blks = os.path.join(r"H:\DATA\census_db\shapes\ACS_2015\ACS_2015.gdb",
                           r"Shapes_MT\Block_1")

housing = r"H:\DATA\census_db\shapes\ACS_2015\ACS_2015.gdb\HOUSEHOLD_POP_TYPE"


# MAKE FIELD MAPPING FOR dwellings (sum)
def sum_dwellings(poly, points, points_fieldname, out_name="spatial_join"):
    fm = arcpy.FieldMap()
    fms = arcpy.FieldMappings()
    fm.addInputField(points, points_fieldname)
    fm.mergeRule = "SUM"
    fms.addFieldMap(fm)
    fm = arcpy.FieldMap()
    fm.addInputField(poly, "D002")
    fm.mergeRule = "MEAN"
    fms.addFieldMap(fm)
    arcpy.SpatialJoin_analysis(poly, points,
                               "in_memory/{}".format(out_name),
                               "JOIN_ONE_TO_ONE", "KEEP_COMMON", fms)
    return

# =============================================================================
# LOAD DATA

# Load Permit points into memory
arcpy.FeatureClassToFeatureClass_conversion(resconst_points, "in_memory",
                                            "ResConst_mem")

# Load Census Blocks and join with housing data
# Dunno what this even does...
#arcpy.MakeFeatureLayer_management(census_blks, "CensusBlks_lyr")
# Copy CensusBlks to memory
arcpy.FeatureClassToFeatureClass_conversion(census_blks, "in_memory",
                                            "CensusBlks_mem")
# Perminantly join housing data to the memory-stored census blocks
arcpy.JoinField_management("CensusBlks_mem", "GEOID", housing,
                           "GEOID", "KEEP_COMMON")

# What the hell are the housing fields called now????
[f.name for f in arcpy.ListFields("CensusBlks_mem")]

# Calc avg household size
search_cur = arcpy.da.SearchCursor("CensusBlks_lyr", [ho_avg, r_avg])



# add field to ResConst_mem
arcpy.AddField_management("ResConst_mem", "AvgHSize", "FLOAT")
# use update cursor to
update_cur = arcpy.da.UpdateCursor("ResConst_mem", ["dwellings", "AvgHSize"])

