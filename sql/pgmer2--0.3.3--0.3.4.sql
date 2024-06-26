DROP FUNCTION "mr_for_beacons_global";
DROP FUNCTOPN "mr_scores_superposition";
DROP FUNCTION "mr_scores";
DROP FUNCTION "mr_nodes";
DROP FUNCTION "mr_graph";

CREATE FUNCTION "mr_reset"() RETURNS TEXT STRICT LANGUAGE c AS 'MODULE_PATHNAME', 'mr_reset_wrapper'; 

CREATE FUNCTION "mr_scores_superposition"(
  "ego" TEXT, /* &str */
  "start_with" TEXT, /* core::option::Option<alloc::string::String> */
  "score_lt" double precision, /* core::option::Option<f64> */
  "score_lte" double precision, /* core::option::Option<f64> */
  "score_gt" double precision, /* core::option::Option<f64> */
  "score_gte" double precision, /* core::option::Option<f64> */
  "index" INT, /* core::option::Option<i32> */
  "limit" INT /* core::option::Option<i32> */
) RETURNS TABLE (
  "ego" TEXT,  /* alloc::string::String */
  "target" TEXT,  /* alloc::string::String */
  "score" double precision  /* f64 */
)
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_scores_superposition_wrapper';

CREATE  FUNCTION "mr_scores"(
  "ego" TEXT, /* &str */
  "hide_personal" bool, /* bool */
  "context" TEXT, /* &str */
  "start_with" TEXT, /* core::option::Option<alloc::string::String> */
  "score_lt" double precision, /* core::option::Option<f64> */
  "score_lte" double precision, /* core::option::Option<f64> */
  "score_gt" double precision, /* core::option::Option<f64> */
  "score_gte" double precision, /* core::option::Option<f64> */
  "index" INT, /* core::option::Option<i32> */
  "limit" INT /* core::option::Option<i32> */
) RETURNS TABLE (
  "ego" TEXT,  /* alloc::string::String */
  "target" TEXT,  /* alloc::string::String */
  "score" double precision  /* f64 */
)
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_scores_wrapper';

CREATE  FUNCTION "mr_nodes"(
  "ego" TEXT, /* &str */
  "focus" TEXT, /* &str */
  "context" TEXT, /* &str */
  "positive_only" bool, /* bool */
  "index" INT, /* core::option::Option<i32> */
  "limit" INT /* core::option::Option<i32> */
) RETURNS TABLE (
  "node" TEXT,  /* alloc::string::String */
  "weight" double precision  /* f64 */
)
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_nodes_wrapper';

CREATE  FUNCTION "mr_graph"(
  "ego" TEXT, /* &str */
  "focus" TEXT, /* &str */
  "context" TEXT, /* &str */
  "positive_only" bool, /* bool */
  "index" INT, /* core::option::Option<i32> */
  "limit" INT /* core::option::Option<i32> */
) RETURNS TABLE (
  "ego" TEXT,  /* alloc::string::String */
  "target" TEXT,  /* alloc::string::String */
  "score" double precision  /* f64 */
)
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_graph_wrapper';
