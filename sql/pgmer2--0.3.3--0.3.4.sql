DROP FUNCTION "mr_for_beacons_global";
CREATE FUNCTION "mr_reset"() RETURNS TEXT STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'mr_zerorec_wrapper'; 
