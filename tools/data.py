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


# =============================================================================
# DATA

FEATURES_DB = os.path.realpath("../data/permit_features.sqlite")

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

# Include derrived data
OTHER_FEATURES = [
    "condos_dis"
    ]
