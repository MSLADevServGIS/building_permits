-- Ask about Mullan 4100
CREATE TABLE res2013_dis AS SELECT permit_number, geocode, address, 
dwellings, ST_Multi(ST_Collect(geometry)) AS geometry FROM res2013 GROUP BY  permit_number;
SELECT RecoverGeometryColumn('res2013_dis', 'geometry', 2256, 'MULTIPOINT', 'XY');

--2013
CREATE VIEW dev_density2013 AS 
SELECT p.address, p.permit_type, z.BASE, p.dwellings, ROUND(Area(u.geometry)/43560, 4) as sqft, 
  FLOOR(p.dwellings/(Area(u.geometry)/43560)) as duac 
FROM res2013 p
  JOIN parcels_dis u --ufda_parcels -> parcels_dis
    ON Intersects(p.geometry, u.geometry) 
  JOIN ufda_zoning z 
    ON Intersects(p.geometry, z.geometry) 
WHERE p.permit_type NOT LIKE 'BNSF%' AND p.dwellings > 1 
ORDER BY duac DESC;


--2014
CREATE VIEW dev_density2014 AS 
SELECT p.address, p.permit_type, z.BASE, p.dwellings, ROUND(Area(u.geometry)/43560, 4) as sqft, 
  FLOOR(p.dwellings/(Area(u.geometry)/43560)) as duac 
FROM res2014 p
  JOIN ufda_parcels u 
    ON Intersects(p.geometry, u.geometry) 
  JOIN ufda_zoning z 
    ON Intersects(p.geometry, z.geometry) 
WHERE p.permit_type NOT LIKE 'BNSF%' AND p.dwellings > 1 
ORDER BY duac DESC;


--2015
CREATE VIEW dev_density2015 AS 
SELECT p.address, p.permit_type, z.BASE, p.dwellings, COUNT(u.geometry) as num_parcels, ROUND(Area(u.geometry)/43560, 4) as sqft, 
  FLOOR(p.dwellings/(Area(u.geometry)/43560)) as duac 
FROM res2015 p
  JOIN ufda_parcels u 
    ON Intersects(p.geometry, u.geometry) 
  JOIN ufda_zoning z 
    ON Intersects(p.geometry, z.geometry) 
WHERE p.permit_type NOT LIKE 'BNSF%' AND p.dwellings > 1 
ORDER BY duac DESC;