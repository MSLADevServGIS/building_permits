/* spatialize.sql: spatializes permits by various methods.
Author: Garin Wally; Aug 2016

This script gives the non-spatial permit table points derrived from either
the ufda_addr features or the ufda_parcels 'PointsOnSurface' (the Centroid isn't
always confined within the parcel).

Any permit that is not spatialized, i.e.
SELECT * FROM <permit_table> WHERE geometry IS NULL;
will need to be delt with manually. Bummer, I know.
*/

-- Join on full address (exact match)
UPDATE
	{0} -- res2015
SET
	notes = 'fulladdr',
	geometry = (
		SELECT ST_Multi(geometry)
		FROM ufda_addrs a
		WHERE a.fulladdress={0}.address)
WHERE geometry IS NULL;


-- Join on address geocode
/* This unintentionally joins permits with all addresses on a parcel giving the permit more geometry
than it truely deserves, but it should be fine because the points are all within the parcel.
*/
UPDATE
	{0} -- res2015
SET
	notes = 'a.parcelid',
	geometry = (
		SELECT ST_Multi(geometry)
		FROM ufda_addrs a
		WHERE a.parcelid={0}.geocode)
WHERE geometry IS NULL;


-- Join on parcel geocode
UPDATE
	{0} -- res2015
SET
	notes = 'geocode',
	geometry = (
		SELECT ST_Multi(PointOnSurface(geometry))
		FROM ufda_parcels u
		WHERE u.parcelid={0}.geocode)
WHERE geometry IS NULL;


-- Spatialize the townhome/condos separately (where permit geocode doesn't end in '000')
-- Then update the permit table from the new townhome/condo table
CREATE TABLE {1} AS
	SELECT p.address AS address, p.geocode AS geocode, u.parcelid AS parcelid, 
		ST_Multi(PointOnSurface(u.geometry)) AS geometry
	FROM res2014 p
	JOIN parcels_dis u
		ON u.parcelid LIKE SUBSTR(p.geocode, 0, LENGTH(p.geocode)-3) || '%'
	WHERE SUBSTR(p.geocode, -4) <> '0000';
SELECT RecoverGeometryColumn('{1}', 'geometry', 2256, 'MULTIPOINT', 'XY');

-- Update the permit table
UPDATE
	{0}
SET
	notes = 'townhome/condo',
	geometry = (
		SELECT geometry
		FROM {1}
		WHERE geocode = res2014.geocode)
WHERE geometry IS NULL;


-- Cleanup
-- DELETE any null geometries that exist where the permit number has already been mapped
-- Example: 2250 RAYMOND AVE merged their parcel so the second doesn't exist
DELETE FROM {0}
WHERE geometry IS NULL
	AND permit_number IN (
		SELECT permit_number FROM {0});

	

	
	
/* MISC left-over code I don't just want to throw away
UPDATE
	{0} -- res2015
SET
	notes = 'townhome/condo',
	geometry = (
		SELECT ST_Multi(PointOnSurface(u.geometry))
		FROM {0} p, parcels_dis u
		WHERE p.geometry IS NULL
			AND u.parcelid LIKE SUBSTR(p.geocode, 0, LENGTH(p.geocode)-2) || '%'
			AND SUBSTR(p.geocode, -3) <> '000')
WHERE geometry IS NULL;
*/
/*	
UPDATE
	res2014
SET
	notes = 'townhome/condo',
	geometry = (SELECT geometry FROM (
		SELECT DISTINCT p.address, p.geocode, u.parcelid, ST_Multi(PointOnSurface(u.geometry)) AS geometry
		FROM res2014 p
		JOIN parcels_dis u
			ON u.parcelid LIKE SUBSTR(p.geocode, 0, LENGTH(p.geocode)-3) || '%'
		WHERE SUBSTR(p.geocode, -4) <> '0000'
		))
WHERE geometry IS NULL;
*/

