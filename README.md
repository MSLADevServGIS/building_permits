# Building Permits Documentation
### Get all new construction permits from Accela and create the permit spatial database

1.  Log into Accela (or use this [link](http://cpdbprod/ReportServer/Pages/ReportViewer.aspx?%2fLand%2fStatistics%2fNew+Construction+Report&rs:Command=Render) and skip to #4)  
2.  On the lower left find the "Reports Box" and expand "Land Statistics"  
3.  Click the AllConstruction report  
4.  Enter the start and end date and click "Submit"  
    a.  Download permits for one year at a time  
    b.  There are no permits in Accela for years before October 2012-ish (switch from PermitsPlus)  
5.  Click the save icon and save as "Excel" (".xlsx" file extension)  
6.  Save to `building_permits/data/accela_data` and name the file `all_<year>.xlsx`  
7.  Double-click the "make_permit_db.py" script to create the "permits.sqlite" database.  
    a.  If double-clicking doesn't work, open cmd.exe and enter: `python I:/<path/to/make_permits_db.py>`  



# Requirements
1. dslw -- Python package for easily dealing with SpatiaLite developed by Garin Wally  
    a.  Installed (more info coming soon)  
	b.  A system environment called `SPATIALITE_SECURITY` set to `relaxed` (Not exactly required yet)  
2. specific file structure  
3. data  