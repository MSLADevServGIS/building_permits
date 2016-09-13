#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
data.py -- Data definitions.
Author: Garin Wally; Aug/Sept 2016

This script defines the location of data used by the permits db / UFDA project.
Executing this script does nothing.
"""

import os
import re
from glob import glob

import yaml


# =============================================================================
# DATA

FEATURES_DB = os.path.realpath("../data/permit_features.sqlite")
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
