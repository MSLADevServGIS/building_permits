# Building Permits Documentation

## Spatial Data Prep
1. Get the County's structure data
  a. Download it from [here](https://shared.missoulacounty.us/index.php/s/7WZBWx8yXkab256)  
  b. Manually reproject it from meters to ft (102700: MT State Plane FIPS Ft)   
  c. I've been saving it to  `\\cityfiles\DEVServices\ArcExplorer\Data\Structures\Address.gdb\AddressStructurePoint_ft`  
2. Check the data paths inside the `data.py` script  
3. Run the data.py script and wait a long, long time...  

## Permits
### City
1.  Log into Accela (or use this [link](http://cpdbprod/ReportServer/Pages/ReportViewer.aspx?%2fLand%2fStatistics%2fNew+Construction+Report&rs:Command=Render) and skip to #4)  
2.  On the lower left find the "Reports Box" and expand "Land Statistics"  
3.  Click the AllConstruction report  
4.  Enter the start and end date and click "Submit"  
    a.  Download permits for one year at a time  
    b.  There are no permits in Accela for years before October 2012-ish (switch from PermitsPlus)  
5.  Click the save icon and save as "Excel" (".xlsx" file extension)  
6.  Save to `building_permits/data/city_permits/raw` and name the file `city_<year>.xlsx`  

### County
1.  Email Deborah Evison at devison@missoulacounty.us and request the residential building permits for <year> in .xlsx format  
2.  Hopefully it comes in the layout and format expected by the script (.xlsx)  
  a. 2015 didn't cause they swiched to Odyssey  
  b. If it doesn't, oh boy, manual processing! ...  
3.  Save to building_permits/data/county_permits/raw as `county_<year>.xlsx`  

### Process 'em

Double-click the "make_permit_db.py" script to create the "permits.sqlite" database.  
  a. If double-clicking doesn't work, open cmd.exe and enter: `python I:/<path/to/make_permits_db.py>`  



# Requirements
1. dslw -- Python package for easily dealing with SpatiaLite developed by Garin Wally  
    a.  Go to the [git repo](https://github.com/WindfallLabs/dslw) and download it as zip, put the `dslw` folder in Python's `site-packages` folder.  
	b.  A system environment called `SPATIALITE_SECURITY` set to `relaxed`  
2. The specific file structure of the [bulding_permits](https://github.com/MSLADevServGIS/building_permits) project  
3. Correct data paths set in the `data.py` script  


# Data Dictionary
What are all these files?  

---
#### `*.sqlite`:
a [SQLite database file](https://www.sqlite.org/about.html); can be read and queried with:  
    1. dslw_shell.py - Shortcut  
    2. QGIS  
    3. ArcGIS  

__NOTE__: the GIS programs change some datatypes (e.g. geocode becomes a LONG INT)  


__permits.sqlite__ contains the pre- and post- processed building permits for the City of Missoula  
__permit\_features.sqlite__ contains the collected spatial data from the sources specified in `data\_sources.yaml`  (coming soon)  

---
#### `*.py`:
a Python file; text file that can be executed via cmd.exe and (sometimes) double-clicking.  

---
#### `*.md`:
a [Markdown](https://daringfireball.net/projects/markdown/) file; simple-to-write plain text that gets rendered to HTML  

---
#### `.git`, `.gitignore`, other git files:  
Git and GitHub are used to version and store open source software on the web; necessary, but can be ignored by users.  

# TODO:
move the list of data sources from `data.py` to a `.yaml` file  
