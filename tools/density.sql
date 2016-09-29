/* Density (duac) calculations
Inputs:
{0}: res<year> table -- input permit table
{1}: new density<year> table -- final permit product for mapping/analysis
{2}: new th_dev<year> table -- townhouse development

Outputs:
Creates the density<year> table(s).
Creates the th_dev<year> table(s).
*/

-- Calc duac including total dwellings and total area of parcels intersecting multi-point permits
-- E.g. 1500 S 14TH ST (2014) 62 duac
CREATE TABLE density{1} (
  permit_number TEXT PRIMARY KEY,
  geocode TEXT,
  address TEXT,
  sum_dwellings INTEGER,
  acres REAL,
  duac REAL,
  condo_proj TEXT,
  geometry MULTIPOINT);

INSERT INTO density{1} SELECT * FROM (
  SELECT 
	permit_number,
    u.geocode AS geocode, 
    p.address AS address,
	sum_dwellings, 
    SUM(Area(u.geometry))/43560.0 AS acres, 
    FLOOR(sum_dwellings/(SUM(Area(u.geometry))/43560.0)) as duac,
	c.name AS condo_proj,
	p.geometry AS geometry
  FROM (
      SELECT DISTINCT permit_number, address, 
        SUM(DISTINCT dwellings) AS sum_dwellings, 
	    -- Dissolve points
	    ST_Multi(ST_Collect(geometry)) AS geometry 
      FROM {0}   
      GROUP BY permit_number) AS p
  JOIN ufda_parcels u ON Intersects(p.geometry, u.geometry) 
  LEFT JOIN condos_dis c ON Intersects(p.geometry, c.geometry) 
  GROUP BY p.permit_number
  ORDER BY p.address);
SELECT RecoverGeometryColumn('{1}', 'geometry', 2256, 'MULTIPOINT', 2);


-- Make a table to track townhome (th) / condo development activity
CREATE TABLE th_dev{1} (
  name TEXT,
  sum_dwellings INTEGER,
  acres REAL,
  proj_duac REAL);

INSERT INTO th_dev{1} SELECT * FROM (
  SELECT c.name AS name,  
    SUM(sum_dwellings) as sum_dwellings, 
    SUM(Area(c.geometry))/43560.0 AS acres, 
    FLOOR(SUM(sum_dwellings)/(SUM(Area(c.geometry))/43560.0)) AS proj_duac  
  FROM {1} d 
  JOIN condos_dis c 
  ON Intersects(d.geometry, c.geometry) 
  GROUP BY c.name);


-- Change the incorrectly calculated townhome/condo duacs
UPDATE density{1}  
SET duac = (
    SELECT proj_duac
	FROM th_dev{1} t
	JOIN density{1} d ON t.name = d.condo_proj)
WHERE condo_proj IS NOT NULL;

-- Taking the FLOOR of 0.x results in 0.0, when really it should be 1.0
UPDATE density{1} 
SET duac = 1.0 
WHERE duac = 0.0;

UPDATE th_dev{1} 
SET proj_duac = 1.0 
WHERE proj_duac = 0.0;


/*-- 2013
CREATE TABLE duacs13_temp AS 
	SELECT p.address AS address, p.geocode AS geocode, SUM(p.dwellings) AS dwellings, 
	    z.BASE AS zoning, ST_Multi(ST_Collect(p.geometry)) AS geometry 
	  FROM res2013_dis p 
	  JOIN ufda_zoning z 
	  ON Intersects(p.geometry, z.geometry) 
	  GROUP BY p.geocode 
	  HAVING SUM(p.dwellings) > 1 AND SUM(p.dwellings) < 100 
	  ORDER BY SUM(p.dwellings) DESC;
	
CREATE TABLE duacs2013 AS 
	SELECT p.*, Area(u.geometry)/43560 AS acres, FLOOR(dwellings/(Area(u.geometry)/43560)) AS duac 
	FROM duacs13_temp p 
	JOIN parcels_dis u 
	ON Intersects(p.geometry, u.geometry) 
	ORDER BY duac DESC;

SELECT RecoverGeometryColumn('duacs2013', 'geometry', 2256, 'MULTIPOINT', 'XY');

DROP TABLE duacs13_temp;


-- 2014
CREATE TABLE duacs14_temp AS 
	SELECT p.address AS address, p.geocode AS geocode, SUM(p.dwellings) AS dwellings, 
	    z.BASE AS zoning, ST_Multi(ST_Collect(p.geometry)) AS geometry 
	  FROM res2014_dis p 
	  JOIN ufda_zoning z 
	  ON Intersects(p.geometry, z.geometry) 
	  GROUP BY p.geocode 
	  HAVING SUM(p.dwellings) > 1 AND SUM(p.dwellings) < 100 
	  ORDER BY SUM(p.dwellings) DESC;
	
CREATE TABLE duacs2014 AS 
	SELECT p.*, Area(u.geometry)/43560 AS acres, FLOOR(dwellings/(Area(u.geometry)/43560)) AS duac 
	FROM duacs14_temp p 
	JOIN parcels_dis u 
	ON Intersects(p.geometry, u.geometry) 
	ORDER BY duac DESC;

SELECT RecoverGeometryColumn('duacs2014', 'geometry', 2256, 'MULTIPOINT', 'XY');

DROP TABLE duacs14_temp;


-- 2015 
CREATE TABLE duacs15_temp AS 
	SELECT p.address AS address, p.geocode AS geocode, SUM(p.dwellings) AS dwellings, 
	    z.BASE AS zoning, ST_Multi(ST_Collect(p.geometry)) AS geometry 
	  FROM res2015_dis p 
	  JOIN ufda_zoning z 
	  ON Intersects(p.geometry, z.geometry) 
	  GROUP BY p.geocode 
	  HAVING SUM(p.dwellings) > 1 AND SUM(p.dwellings) < 100 
	  ORDER BY SUM(p.dwellings) DESC;
	
CREATE TABLE duacs2015 AS 
	SELECT p.*, Area(u.geometry)/43560 AS acres, FLOOR(dwellings/(Area(u.geometry)/43560)) AS duac 
	FROM duacs15_temp p 
	JOIN parcels_dis u 
	ON Intersects(p.geometry, u.geometry) 
	ORDER BY duac DESC;

SELECT RecoverGeometryColumn('duacs2015', 'geometry', 2256, 'MULTIPOINT', 'XY');

DROP TABLE duacs15_temp;
*/

/*
-------------------------------------------------------------
SELECT u.name AS name, p.address AS address, SUM(sum_dwellings) as sum_dwellings,  
  SUM(Area(u.geometry))/43560.0 AS acres, 
  FLOOR(SUM(sum_dwellings)/(SUM(Area(u.geometry))/43560.0)) as duac 
FROM (
  SELECT DISTINCT permit_number, address, 
    SUM(DISTINCT dwellings) AS sum_dwellings, 
	-- Dissolve points
	ST_Multi(ST_Collect(geometry)) AS geometry 
  FROM city_res2014 
  GROUP BY permit_number) AS p
JOIN condos_dis u ON Intersects(p.geometry, u.geometry) 
GROUP BY u.name;
*/