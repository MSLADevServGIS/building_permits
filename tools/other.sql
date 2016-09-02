-- All multidwelling development
SELECT DISTINCT SUM(DISTINCT dwellings) AS units, address 
FROM city_res2013 
GROUP BY permit_number, geocode 
HAVING units >= 3

UNION 

SELECT DISTINCT SUM(DISTINCT dwellings) AS units, address 
FROM city_res2014 
GROUP BY permit_number, geocode 
HAVING units >= 3

UNION 

SELECT DISTINCT SUM(DISTINCT dwellings) AS units, address 
FROM city_res2015 
GROUP BY permit_number, geocode 
HAVING units >= 3

ORDER BY units DESC;
--

--------------------------------
-- All multidwelling development
SELECT DISTINCT SUM(DISTINCT p.dwellings) AS units, p.address, AREA(u.geometry)/43560.0 AS acres 
FROM city_res2013 p 
JOIN ufda_parcels u 
GROUP BY p.permit_number, p.geocode 
HAVING units >= 3;

UNION 

SELECT DISTINCT SUM(DISTINCT dwellings) AS units, address 
FROM city_res2014 
GROUP BY permit_number, geocode 
HAVING units >= 3

UNION 

SELECT DISTINCT SUM(DISTINCT dwellings) AS units, address 
FROM city_res2015 
GROUP BY permit_number, geocode 
HAVING units >= 3

ORDER BY units DESC;
--

