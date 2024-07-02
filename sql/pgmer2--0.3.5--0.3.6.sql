DROP FUNCTION IF EXISTS mr_for_beacons_global;

DROP FUNCTION IF EXISTS mr_service_url;
DROP FUNCTION IF EXISTS mr_connector;
DROP FUNCTION IF EXISTS mr_service_wrapped;
DROP FUNCTION IF EXISTS mr_service;
DROP FUNCTION IF EXISTS mr_node_score_superposition;
DROP FUNCTION IF EXISTS mr_node_score;
DROP FUNCTION IF EXISTS mr_node_score_linear_sum;
DROP FUNCTION IF EXISTS mr_scores_superposition;
DROP FUNCTION IF EXISTS mr_scores;
DROP FUNCTION IF EXISTS mr_scores_linear_sum;
DROP FUNCTION IF EXISTS mr_score_linear_sum;
DROP FUNCTION IF EXISTS mr_graph;
DROP FUNCTION IF EXISTS mr_nodes;
DROP FUNCTION IF EXISTS mr_nodelist;
DROP FUNCTION IF EXISTS mr_edgelist;
DROP FUNCTION IF EXISTS mr_connected;
DROP FUNCTION IF EXISTS mr_put_edge;
DROP FUNCTION IF EXISTS mr_delete_edge;
DROP FUNCTION IF EXISTS mr_delete_node;
DROP FUNCTION IF EXISTS mr_reset;
DROP FUNCTION IF EXISTS mr_zerorec;

DROP VIEW IF EXISTS mr_t_edge;
DROP VIEW IF EXISTS mr_t_link;
DROP VIEW IF EXISTS mr_t_node;

CREATE VIEW mr_t_edge AS SELECT ''::text AS ego,    '' ::text             AS target, (0)::double precision AS score;
CREATE VIEW mr_t_link AS SELECT ''::text AS source, '' ::text             AS target;
CREATE VIEW mr_t_node AS SELECT ''::text AS name,   (0)::double precision AS weight;


-- src/lib.rs:548
-- pgmer2::mr_zerorec
CREATE  FUNCTION "mr_zerorec"() RETURNS TEXT /* core::result::Result<alloc::string::String, alloc::boxed::Box<dyn core::error::Error>> */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_zerorec_wrapper';

-- src/lib.rs:184
-- pgmer2::mr_service_url
CREATE  FUNCTION "mr_service_url"() RETURNS TEXT /* &str */
IMMUTABLE STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_service_url_wrapper';

-- src/lib.rs:194
-- pgmer2::mr_service
CREATE  FUNCTION "mr_service"() RETURNS TEXT /* alloc::string::String */
IMMUTABLE STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_service_wrapper';

-- src/lib.rs:295
-- pgmer2::mr_scores_superposition
CREATE  FUNCTION "mr_scores_superposition"(
	"ego" TEXT, /* core::option::Option<&str> */
	"start_with" TEXT, /* core::option::Option<&str> */
	"score_lt" double precision, /* core::option::Option<f64> */
	"score_lte" double precision, /* core::option::Option<f64> */
	"score_gt" double precision, /* core::option::Option<f64> */
	"score_gte" double precision, /* core::option::Option<f64> */
	"index" INT, /* core::option::Option<i32> */
	"count" INT /* core::option::Option<i32> */
) RETURNS SETOF mr_t_edge /* pgrx::heap_tuple::PgHeapTuple<pgrx::pgbox::AllocatedByRust> */
IMMUTABLE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_scores_superposition_wrapper';

-- src/lib.rs:353
-- pgmer2::mr_scores_linear_sum
CREATE  FUNCTION "mr_scores_linear_sum"(
	"src" TEXT /* core::option::Option<&str> */
) RETURNS SETOF mr_t_edge /* pgrx::heap_tuple::PgHeapTuple<pgrx::pgbox::AllocatedByRust> */
IMMUTABLE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_scores_linear_sum_wrapper';

-- src/lib.rs:322
-- pgmer2::mr_scores
CREATE  FUNCTION "mr_scores"(
	"ego" TEXT, /* core::option::Option<&str> */
	"hide_personal" bool, /* core::option::Option<bool> */
	"context" TEXT, /* core::option::Option<&str> */
	"start_with" TEXT, /* core::option::Option<&str> */
	"score_lt" double precision, /* core::option::Option<f64> */
	"score_lte" double precision, /* core::option::Option<f64> */
	"score_gt" double precision, /* core::option::Option<f64> */
	"score_gte" double precision, /* core::option::Option<f64> */
	"index" INT, /* core::option::Option<i32> */
	"count" INT /* core::option::Option<i32> */
) RETURNS SETOF mr_t_edge /* pgrx::heap_tuple::PgHeapTuple<pgrx::pgbox::AllocatedByRust> */
IMMUTABLE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_scores_wrapper';

-- src/lib.rs:366
-- pgmer2::mr_score_linear_sum
CREATE  FUNCTION "mr_score_linear_sum"(
	"src" TEXT, /* core::option::Option<&str> */
	"dest" TEXT /* core::option::Option<&str> */
) RETURNS SETOF mr_t_edge /* pgrx::heap_tuple::PgHeapTuple<pgrx::pgbox::AllocatedByRust> */
IMMUTABLE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_score_linear_sum_wrapper';

-- src/lib.rs:537
-- pgmer2::mr_reset
CREATE  FUNCTION "mr_reset"() RETURNS TEXT /* core::result::Result<alloc::string::String, alloc::boxed::Box<dyn core::error::Error>> */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_reset_wrapper';

-- src/lib.rs:489
-- pgmer2::mr_put_edge
CREATE  FUNCTION "mr_put_edge"(
	"src" TEXT, /* core::option::Option<&str> */
	"dest" TEXT, /* core::option::Option<&str> */
	"weight" double precision, /* core::option::Option<f64> */
	"context" TEXT /* core::option::Option<&str> */
) RETURNS SETOF mr_t_edge /* pgrx::heap_tuple::PgHeapTuple<pgrx::pgbox::AllocatedByRust> */
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_put_edge_wrapper';

-- src/lib.rs:405
-- pgmer2::mr_nodes
CREATE  FUNCTION "mr_nodes"(
	"ego" TEXT, /* core::option::Option<&str> */
	"focus" TEXT, /* core::option::Option<&str> */
	"context" TEXT, /* core::option::Option<&str> */
	"positive_only" bool, /* core::option::Option<bool> */
	"index" INT, /* core::option::Option<i32> */
	"count" INT /* core::option::Option<i32> */
) RETURNS SETOF mr_t_node /* pgrx::heap_tuple::PgHeapTuple<pgrx::pgbox::AllocatedByRust> */
IMMUTABLE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_nodes_wrapper';

-- src/lib.rs:431
-- pgmer2::mr_nodelist
CREATE  FUNCTION "mr_nodelist"(
	"context" TEXT /* core::option::Option<&str> */
) RETURNS SETOF TEXT /* alloc::string::String */
IMMUTABLE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_nodelist_wrapper';

-- src/lib.rs:202
-- pgmer2::mr_node_score_superposition
CREATE  FUNCTION "mr_node_score_superposition"(
	"ego" TEXT, /* core::option::Option<&str> */
	"target" TEXT /* core::option::Option<&str> */
) RETURNS SETOF mr_t_edge /* pgrx::heap_tuple::PgHeapTuple<pgrx::pgbox::AllocatedByRust> */
IMMUTABLE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_node_score_superposition_wrapper';

-- src/lib.rs:235
-- pgmer2::mr_node_score_linear_sum
CREATE  FUNCTION "mr_node_score_linear_sum"(
	"ego" TEXT, /* core::option::Option<&str> */
	"target" TEXT /* core::option::Option<&str> */
) RETURNS SETOF mr_t_edge /* pgrx::heap_tuple::PgHeapTuple<pgrx::pgbox::AllocatedByRust> */
IMMUTABLE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_node_score_linear_sum_wrapper';

-- src/lib.rs:217
-- pgmer2::mr_node_score
CREATE  FUNCTION "mr_node_score"(
	"ego" TEXT, /* core::option::Option<&str> */
	"target" TEXT, /* core::option::Option<&str> */
	"context" TEXT /* core::option::Option<&str> */
) RETURNS SETOF mr_t_edge /* pgrx::heap_tuple::PgHeapTuple<pgrx::pgbox::AllocatedByRust> */
IMMUTABLE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_node_score_wrapper';

-- src/lib.rs:381
-- pgmer2::mr_graph
CREATE  FUNCTION "mr_graph"(
	"ego" TEXT, /* core::option::Option<&str> */
	"focus" TEXT, /* core::option::Option<&str> */
	"context" TEXT, /* core::option::Option<&str> */
	"positive_only" bool, /* core::option::Option<bool> */
	"index" INT, /* core::option::Option<i32> */
	"count" INT /* core::option::Option<i32> */
) RETURNS SETOF mr_t_edge /* pgrx::heap_tuple::PgHeapTuple<pgrx::pgbox::AllocatedByRust> */
IMMUTABLE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_graph_wrapper';

-- src/lib.rs:451
-- pgmer2::mr_edgelist
CREATE  FUNCTION "mr_edgelist"(
	"context" TEXT /* core::option::Option<&str> */
) RETURNS SETOF mr_t_edge /* pgrx::heap_tuple::PgHeapTuple<pgrx::pgbox::AllocatedByRust> */
IMMUTABLE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_edgelist_wrapper';

-- src/lib.rs:524
-- pgmer2::mr_delete_node
CREATE  FUNCTION "mr_delete_node"(
	"ego" TEXT, /* core::option::Option<&str> */
	"context" TEXT /* core::option::Option<&str> */
) RETURNS TEXT /* core::result::Result<&str, alloc::boxed::Box<dyn core::error::Error>> */
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_delete_node_wrapper';

-- src/lib.rs:509
-- pgmer2::mr_delete_edge
CREATE  FUNCTION "mr_delete_edge"(
	"ego" TEXT, /* core::option::Option<&str> */
	"target" TEXT, /* core::option::Option<&str> */
	"context" TEXT /* core::option::Option<&str> */
) RETURNS TEXT /* core::result::Result<&str, alloc::boxed::Box<dyn core::error::Error>> */
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_delete_edge_wrapper';

-- src/lib.rs:189
-- pgmer2::mr_connector
CREATE  FUNCTION "mr_connector"() RETURNS TEXT /* &str */
IMMUTABLE STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_connector_wrapper';

-- src/lib.rs:467
-- pgmer2::mr_connected
CREATE  FUNCTION "mr_connected"(
	"ego" TEXT, /* core::option::Option<&str> */
	"context" TEXT /* core::option::Option<&str> */
) RETURNS SETOF mr_t_link /* pgrx::heap_tuple::PgHeapTuple<pgrx::pgbox::AllocatedByRust> */
IMMUTABLE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'mr_connected_wrapper';
