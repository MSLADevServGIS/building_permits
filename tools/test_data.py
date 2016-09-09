
import os
import unittest

import dslw

from aside import handle_ex

import data

DB = r"\\cityfiles\DEVServices\WallyG\projects\building_permits\tools\testing.sqlite"
GDB = r"\\cityfiles\DEVServices\WallyG\projects\building_permits\data\permit_features.gdb"

if os.path.exists(DB):
    os.remove(DB)

dslw.SpatialDB(DB)

import arcpy

'''
features = data.ALL_FEATURES.keys()
for feature in features:
    arcpy.FeatureClassToFeatureClass_conversion(
        data.ALL_FEATURES[feature][0], DB, data.ALL_FEATURES[feature][2])
'''

class TestFc2Fc(unittest.TestCase):
    def setUp(self):
        self.features = data.ALL_FEATURES.keys()
        self.conn = dslw.SpatialDB(DB)
        '''
        self.features = data.ALL_FEATURES.keys()
        for feature in self.features:
            arcpy.FeatureClassToFeatureClass_conversion(
                os.path.join(GDB, feature), DB, feature)
            assert feature in self.conn.get_tables()
        '''
        self.cur = self.conn.cursor()
    '''
    def tearDown(self):
        self.cur.execute("DROP TABLE {};".format(OUT))
        self.cur.close()
    '''
    def test_src_gdb(self):
        gdb_fcs = arcpy.ListFeatureClasses(GDB)
        self.assertTrue(sorted(gdb_fcs) == sorted(self.features))

    def test_fc2fc(self):
        for f in self.features:
            valid = "SELECT DISTINCT IsValidReason(Shape) FROM {}".format(f)
            self.assertTrue(f in self.conn.get_tables())
            self.assertEqual(self.cur.execute(valid).fetchone()[0],
                             "Valid Geometry")
            srid = "SELECT DISTINCT SRID(Shape) FROM {}".format(f)
            self.assertEqual(self.cur.execute(srid).fetchone()[0], 0)
    '''
    def test_select(self):
        assert self.cur.execute("SELECT * FROM geometry_columns").fetchall()
    '''
    def test_insert_after(self):
        self.cur.execute("CREATE TABLE names (first, last)")
        self.cur.execute("INSERT INTO names VALUES ('garin', 'wally');")
        assert self.cur.execute("SELECT * FROM names").fetchall()
    '''
    def test_fix(self):
        dslw.utils.normalize_table(self.conn, OUT, "Shape", 102700)
        info = self.cur.execute(
            "PRAGMA table_info('{}')".format(OUT)).fetchall()
        cols = [c[1] for c in info]
        self.assertTrue(all([c.islower() for c in cols]))
        self.assertTrue("geometry" in cols)
        valid = "SELECT DISTINCT IsValidReason(geometry) FROM {}".format(OUT)
        self.assertEqual(self.cur.execute(valid).fetchone()[0],
                         "Valid Geometry")
        srid = "SELECT DISTINCT SRID(geometry) FROM {}".format(OUT)
        self.assertEqual(self.cur.execute(srid).fetchone()[0],
                         102700)            
    '''
