DROP FUNCTION IF EXISTS "mr_for_beacons_global";
DROP FUNCTION IF EXISTS "mr_scores_superposition";
DROP FUNCTION IF EXISTS "mr_scores";
DROP FUNCTION IF EXISTS "mr_nodes";
DROP FUNCTION IF EXISTS "mr_graph";
DROP FUNCTION IF EXISTS "mr_reset";

CREATE  FUNCTION "mr_scores_superposition"(
  "ego" TEXT, /* core::option::Option<&str> */
  "start_with" TEXT, /* core::option::Option<alloc::string::String> */
  "score_lt" double precision, /* core::option::Option<f64> */
  "score_lte" double precision, /* core::option::Option<f64> */
  "score_gt" double precision, /* core::option::Option<f64> */
  "score_gte" double precision, /* core::option::Option<f64> */
  "index" INT, /* core::option::Option<i32> */
  "count" INT /* core::option::Option<i32> */
) RETURNS TABLE (
  "ego" TEXT,  /* alloc::string::String */
  "target" TEXT,  /* alloc::string::String */
  "score" double precision  /* f64 */
)
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_scores_superposition_wrapper';

CREATE  FUNCTION "mr_scores"(
  "ego" TEXT, /* core::option::Option<&str> */
  "hide_personal" bool, /* core::option::Option<bool> */
  "context_" TEXT, /* core::option::Option<&str> */
  "start_with" TEXT, /* core::option::Option<alloc::string::String> */
  "score_lt" double precision, /* core::option::Option<f64> */
  "score_lte" double precision, /* core::option::Option<f64> */
  "score_gt" double precision, /* core::option::Option<f64> */
  "score_gte" double precision, /* core::option::Option<f64> */
  "index" INT, /* core::option::Option<i32> */
  "count" INT /* core::option::Option<i32> */
) RETURNS TABLE (
  "ego" TEXT,  /* alloc::string::String */
  "target" TEXT,  /* alloc::string::String */
  "score" double precision  /* f64 */
)
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_scores_wrapper';

CREATE  FUNCTION "mr_nodes"(
  "ego_" TEXT, /* core::option::Option<&str> */
  "focus_" TEXT, /* core::option::Option<&str> */
  "context_" TEXT, /* core::option::Option<&str> */
  "positive_only_" bool, /* core::option::Option<bool> */
  "limit_" INT, /* core::option::Option<i32> */
  "index_" INT, /* core::option::Option<i32> */
  "count_" INT /* core::option::Option<i32> */
) RETURNS TABLE (
  "node" TEXT,  /* alloc::string::String */
  "weight" double precision  /* f64 */
)
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_nodes_wrapper';

CREATE  FUNCTION "mr_graph"(
  "ego_" TEXT, /* core::option::Option<&str> */
  "focus_" TEXT, /* core::option::Option<&str> */
  "context_" TEXT, /* core::option::Option<&str> */
  "positive_only_" bool, /* core::option::Option<bool> */
  "limit_" INT, /* core::option::Option<i32> */
  "index_" INT, /* core::option::Option<i32> */
  "count_" INT /* core::option::Option<i32> */
) RETURNS TABLE (
  "ego" TEXT,  /* alloc::string::String */
  "target" TEXT,  /* alloc::string::String */
  "score" double precision  /* f64 */
)
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_graph_wrapper';

CREATE FUNCTION "mr_reset"() RETURNS TEXT STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'mr_reset_wrapper'; 
