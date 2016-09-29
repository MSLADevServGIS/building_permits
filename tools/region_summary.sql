/* Region Summary Report
Inputs:
{0}: year
{1}: aggregation feature
{2}: aggregation feature's name field

Outputs:
Creates the region summary tables (_rs<year>) for the input year's single family, duplex, and multi
family units by input aggregation field.
*/

/* Analysis for Mike -- Model Validation
-- All Multidwelling Development by Nhood
CREATE TABLE all_multidev (nhood_name TEXT, sum_dwellings INTEGER);
-- Populate table
INSERT INTO all_multidev
  SELECT name, sum(sum_dwellings) AS sum_dwellings FROM (

    -- Copy/paste this for individual projects
    SELECT d.address AS address, SUM(d.sum_dwellings) AS sum_dwellings, n.name AS name 
    FROM density2014 d 
    JOIN council_dists n ON Intersects(d.geometry, n.geometry) 
    WHERE d.sum_dwellings >= 3 
	GROUP BY d.address

    UNION 

    SELECT d.address AS address, SUM(d.sum_dwellings) AS sum_dwellings, n.name AS name 
    FROM density2015 d 
    JOIN council_dists n ON Intersects(d.geometry, n.geometry) 
    WHERE d.sum_dwellings >= 3 
	GROUP BY d.address

    UNION 

    SELECT d.address AS address, SUM(d.sum_dwellings) AS sum_dwellings, n.name AS name 
    FROM density2016 d 
    JOIN council_dists n ON Intersects(d.geometry, n.geometry) 
    WHERE d.sum_dwellings >= 3 
	GROUP BY d.address

    ORDER BY n.name
) 
  GROUP BY name;
*/

-- Recreated the anlaysis for Mike to be more useful/modular
CREATE TABLE multi_rs{0} (nhood_name TEXT, sum_dwellings INTEGER);
-- Populate table
INSERT INTO multi_rs{0}
  SELECT name, sum(sum_dwellings) AS sum_dwellings FROM (
    SELECT d.address AS address, SUM(d.sum_dwellings) AS sum_dwellings, n.{2} AS name 
    FROM density{0} d 
    JOIN {1} n ON Intersects(d.geometry, n.geometry) 
    WHERE d.sum_dwellings >= 3 
	GROUP BY d.address
	ORDER BY n.{2}
	)
  GROUP BY name;

/* All multidev for a series of years
SELECT nhood_name, SUM(sum_dwellings) FROM (
  SELECT * FROM multidev2014
  UNION
  SELECT * FROM multidev2015
  UNION
  SELECT * FROM multidev2016
  )
  GROUP BY nhood_name;
*/

-- Same thing, but for single family residences (sfr)
CREATE TABLE sfr_rs{0} (nhood_name TEXT, sum_dwellings INTEGER);

INSERT INTO sfr_rs{0}
  SELECT name, SUM(sum_dwellings) AS sum_dwellings FROM (

    SELECT d.address AS address, SUM(d.sum_dwellings) AS sum_dwellings, n.{2} AS name 
    FROM density{0} d
    JOIN {1} n ON Intersects(d.geometry, n.geometry) 
    WHERE d.sum_dwellings = 1 
	GROUP BY d.address
	ORDER BY n.{2}
    ) 
  GROUP BY name;


-- And for duplexes
CREATE TABLE duplex_rs{0} (nhood_name TEXT, sum_dwellings INTEGER);

INSERT INTO duplex_rs{0}
  SELECT name, SUM(sum_dwellings) AS sum_dwellings FROM (

    SELECT d.address AS address, SUM(d.sum_dwellings) AS sum_dwellings, n.{2} AS name 
    FROM density{0} d 
    JOIN {1} n ON Intersects(d.geometry, n.geometry) 
    WHERE d.sum_dwellings = 2 
	GROUP BY d.address
	ORDER BY n.{2}
    ) 
  GROUP BY name;
