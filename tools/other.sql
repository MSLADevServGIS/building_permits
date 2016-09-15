-- All Multidwelling Development by Nhood
CREATE TABLE all_multidev (nhood_name TEXT, sum_dwellings INTEGER);
-- Populate table
INSERT INTO all_multidev
  SELECT name, sum(sum_dwellings) AS sum_dwellings FROM (

    -- Copy/paste this for individual projects
    SELECT d.address AS address, sum(d.sum_dwellings) AS sum_dwellings, n.name AS name 
    FROM density2014 d 
    JOIN council_dists n ON Intersects(d.geometry, n.geometry) 
    WHERE d.sum_dwellings >= 3 
	GROUP BY d.address

    UNION 

    SELECT d.address AS address, sum(d.sum_dwellings) AS sum_dwellings, n.name AS name 
    FROM density2015 d 
    JOIN council_dists n ON Intersects(d.geometry, n.geometry) 
    WHERE d.sum_dwellings >= 3 
	GROUP BY d.address

    UNION 

    SELECT d.address AS address, sum(d.sum_dwellings) AS sum_dwellings, n.name AS name 
    FROM density2016 d 
    JOIN council_dists n ON Intersects(d.geometry, n.geometry) 
    WHERE d.sum_dwellings >= 3 
	GROUP BY d.address

    ORDER BY n.name
) 
  GROUP BY name;