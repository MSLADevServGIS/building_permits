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






-- Density (duac)
-- Calc duac including total dwellings and total area of parcels intersecting multi-point permits
-- E.g. 1500 S 14TH ST (2014) 62 duac
CREATE TABLE density_2014 AS
SELECT u.geocode AS geocode, p.address AS address, sum_dwellings, 
  p.geometry AS geometry,
  SUM(Area(u.geometry))/43560.0 AS acres, 
  FLOOR(sum_dwellings/(SUM(Area(u.geometry))/43560.0)) as duac 
FROM (
  SELECT DISTINCT permit_number, address, 
    SUM(DISTINCT dwellings) AS sum_dwellings, 
	-- Dissolve points
	ST_Multi(ST_Collect(geometry)) AS geometry 
  FROM city_res2014 -- {0}
  GROUP BY permit_number) AS p
JOIN ufda_parcels u ON Intersects(p.geometry, u.geometry) 
GROUP BY p.permit_number
ORDER BY p.address;
SELECT RecoverGeometryColumn('density_2014', 'geometry', 2256, 'MULTIPOINT', 2);

/*
-------------------------------------------------------------


-- Density by Townhome/Condo Project
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