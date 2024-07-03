use lazy_static::lazy_static;
use nng::*;
use nng::options::{Options, RecvTimeout};
use std::time::Duration;
use pgrx::*;
use pgrx::iter::SetOfIterator;
use pgrx::prelude::PgHeapTuple;
use serde::de::Deserialize;
use std::env::var;
use std::error::Error;
use core::result::Result;

#[cfg(any(test, feature = "pg_test"))]
pub mod testing;

pg_module_magic!();

lazy_static! {
  static ref SERVICE_URL : String =
    var("MERITRANK_SERVICE_URL").unwrap_or("tcp://127.0.0.1:10234".to_string());

  static ref RECV_TIMEOUT_MSEC : u64 =
    var("MERITRANK_RECV_TIMEOUT_MSEC")
      .ok()
      .and_then(|s| s.parse::<u64>().ok())
      .unwrap_or(10000);
}

const VERSION : &str = match option_env!("CARGO_PKG_VERSION") {
  Some(x) => x,
  None    => "dev"
};

//  ================================================================
//
//    SQL
//
//  ================================================================

extension_sql!(r#"
DROP FUNCTION IF EXISTS mr_for_beacons_global;
DROP FUNCTION IF EXISTS mr_score_linear_sum;
DROP FUNCTION IF EXISTS mr_nodes;
DROP VIEW     IF EXISTS mr_t_node;

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
DROP FUNCTION IF EXISTS mr_graph;
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
DROP VIEW IF EXISTS mr_t_stats;

CREATE VIEW mr_t_edge  AS SELECT ''::text AS ego,    '' ::text             AS target, (0)::double precision AS score          WHERE false;
CREATE VIEW mr_t_link  AS SELECT ''::text AS source, '' ::text             AS target                                          WHERE false;
CREATE VIEW mr_t_stats AS SELECT ''::text AS name,   (0)::double precision AS score,  (0)::double precision AS score_reversed WHERE false;
"#,
  name      = "bootstrap_raw",
  bootstrap,
  creates   = [Type(mr_t_edge), Type(mr_t_link), Type(mr_t_stats)],
);

//  ================================================================
//
//    Utils
//
//  ================================================================


fn request_raw(payload : Vec<u8>, timeout_msec : Option<u64>) -> Result<Message, Box<dyn Error + 'static>> {
  let client = Socket::new(Protocol::Req0)?;
  match timeout_msec {
    Some(t) => client.set_opt::<RecvTimeout>(Some(Duration::from_millis(t)))?,
    _       => {}
  }
  client.dial(&SERVICE_URL)?;
  client
    .send(Message::from(payload.as_slice()))
    .map_err(|(_, err)| err)?;
  return Ok(client.recv()?);
}

fn request<T: for<'a> Deserialize<'a>>(
  payload    : Vec<u8>,
  timeout_msec : Option<u64>,
) -> Result<Vec<T>, Box<dyn Error + 'static>> {
  let msg = request_raw(payload, timeout_msec)?;
  let slice : &[u8] = msg.as_slice();
  rmp_serde::from_slice(slice).or_else(|_| {
    let err: String = rmp_serde::from_slice(slice)?;
    Err(Box::from(format!("Server error: {}", err)))
  })
}

fn contexted_payload(
  context : &str,
  payload : Vec<u8>
) -> Result<Vec<u8>, Box<dyn Error + 'static>> {
  let q : (&str, &str, Vec<u8>) = ("context", context, payload);
  Ok(rmp_serde::to_vec(&q)?)
}

fn service_wrapped() -> Result<String, Box<dyn Error + 'static>> {
  let payload  = rmp_serde::to_vec(&"ver")?;
  let response = request_raw(payload, Some(*RECV_TIMEOUT_MSEC))?;
  let s        = rmp_serde::from_slice(response.as_slice())?;
  return Ok(s);
}

fn make_setof_edge(response : &Vec<(String, String, f64)>) -> Result<
  SetOfIterator<'static, pgrx::composite_type!('static, "mr_t_edge")>,
  Box<dyn Error + 'static>,
> {
  let tuples : Vec<PgHeapTuple<'_, AllocatedByRust>> =
    response
      .iter()
      .map(|(ego, target, score)| {
        let mut edge = PgHeapTuple::new_composite_type("mr_t_edge").unwrap();
        edge.set_by_name("ego",    ego.as_str()).unwrap();
        edge.set_by_name("target", target.as_str()).unwrap();
        edge.set_by_name("score",  *score).unwrap();
        return edge;
      })
      .collect();
  return Ok(SetOfIterator::new(tuples));
}

fn make_setof_link(response : &Vec<(String, String)>) -> Result<
  SetOfIterator<'static, pgrx::composite_type!('static, "mr_t_link")>,
  Box<dyn Error + 'static>,
> {
  let tuples : Vec<PgHeapTuple<'_, AllocatedByRust>> =
    response
      .iter()
      .map(|(ego, target)| {
        let mut edge = PgHeapTuple::new_composite_type("mr_t_link").unwrap();
        edge.set_by_name("source",    ego.as_str()).unwrap();
        edge.set_by_name("target", target.as_str()).unwrap();
        return edge;
      })
      .collect();
  return Ok(SetOfIterator::new(tuples));
}

fn make_setof_stats(response : &Vec<(String, f64, f64)>) -> Result<
  SetOfIterator<'static, pgrx::composite_type!('static, "mr_t_stats")>,
  Box<dyn Error + 'static>,
> {
  let tuples : Vec<PgHeapTuple<'_, AllocatedByRust>> =
    response
      .iter()
      .map(|(name, score, score_reversed)| {
        let mut stats = PgHeapTuple::new_composite_type("mr_t_stats").unwrap();
        stats.set_by_name("name",           name.as_str()).unwrap();
        stats.set_by_name("score",          *score).unwrap();
        stats.set_by_name("score_reversed", *score_reversed).unwrap();
        return stats;
      })
      .collect();
  return Ok(SetOfIterator::new(tuples));
}

//  ================================================================
//
//    Immutable functions
//
//  ================================================================

#[pg_extern(immutable)]
fn mr_service_url() -> &'static str {
  &SERVICE_URL
}

#[pg_extern(immutable)]
fn mr_connector() ->  &'static str {
  VERSION
}

#[pg_extern(immutable)]
fn mr_service() -> String {
  match service_wrapped() {
    Err(e) => format!("{}", e),
    Ok(s)  => s
  }
}

#[pg_extern(immutable)]
fn mr_node_score_superposition(
  ego    : Option<&str>,
  target : Option<&str>,
) -> Result<
  SetOfIterator<'static, pgrx::composite_type!('static, "mr_t_edge")>,
  Box<dyn Error + 'static>,
> {
  let ego      = ego.expect("ego should not be null");
  let target   = target.expect("target should not be null");
  let payload  = rmp_serde::to_vec(&((("src", "=", ego), ("dest", "=", target)), ()))?;
  let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
  return make_setof_edge(&response);
}

#[pg_extern(immutable)]
fn mr_node_score(
  ego     : Option<&str>,
  target  : Option<&str>,
  context : default!(Option<&str>, "''"),
) -> Result<
  SetOfIterator<'static, pgrx::composite_type!('static, "mr_t_edge")>,
  Box<dyn Error + 'static>,
> {
  let ego      = ego.expect("ego should not be null");
  let target   = target.expect("target should not be null");
  let context  = context.unwrap_or("");
  let payload  = rmp_serde::to_vec(&((("src", "=", ego), ("dest", "=", target)), ()))?;
  let payload  = if context.is_empty() { payload } else { contexted_payload(context, payload)? };
  let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
  return make_setof_edge(&response);
}

#[pg_extern(immutable)]
fn mr_node_score_linear_sum(
  ego    : Option<&str>,
  target : Option<&str>,
) -> Result<
  SetOfIterator<'static, pgrx::composite_type!('static, "mr_t_edge")>,
  Box<dyn Error + 'static>,
> {
  let ego      = ego.expect("ego should not be null");
  let target   = target.expect("target should not be null");
  let payload  = rmp_serde::to_vec(&((("src", "=", ego), ("dest", "=", target)), (), "null"))?;
  let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
  return make_setof_edge(&response);
}

fn scores_payload(
  ego            : Option<&str>,
  hide_personal  : Option<bool>,
  node_kind      : Option<&str>,
  score_lt       : Option<f64>,
  score_lte      : Option<f64>,
  score_gt       : Option<f64>,
  score_gte      : Option<f64>,
  index          : Option<i32>,
  count          : Option<i32>
) -> Result<
  Vec<u8>,
  Box<dyn Error + 'static>,
> {
  let ego           = ego.expect("ego should not be null");
  let hide_personal = hide_personal.unwrap_or(false);
  let index         = index.unwrap_or(0) as u32;
  let count         = count.unwrap_or(i32::MAX) as u32;
  let (lcmp, lt) = match (score_lt, score_lte) {
    (Some(lt), None) => ("<", lt),
    (None, Some(lte)) => ("<=", lte),
    (None, None) => ("<", f64::MIN),
    _ => return Err(Box::from("either lt or lte allowed!"))
  };
  let (gcmp, gt) = match (score_gt, score_gte) {
    (Some(gt), None) => (">", gt),
    (None, Some(gte)) => (">=", gte),
    (None, None) => (">", f64::MAX),
    _ => return Err(Box::from("either gt or gte allowed!"))
  };
  let k = node_kind.unwrap_or("".into());
  let q = ((
        ("src", "=", ego),
        ("node_kind", k),
        ("hide_personal", hide_personal),
        ("score", gcmp, gt),
        ("score", lcmp, lt),
        ("index", index),
        ("count", count)
       ),
       ());
  rmp_serde::to_vec(&q)
    .map_err(|e| e.into())
}

#[pg_extern(immutable)]
fn mr_scores_superposition(
  ego        : Option<&str>,
  start_with : default!(Option<&str>, "''"),
  score_lt   : default!(Option<f64>,  "null"),
  score_lte  : default!(Option<f64>,  "null"),
  score_gt   : default!(Option<f64>,  "null"),
  score_gte  : default!(Option<f64>,  "null"),
  index      : default!(Option<i32>,  "null"),
  count      : default!(Option<i32>,  "null")
) -> Result<
  SetOfIterator<'static, pgrx::composite_type!('static, "mr_t_edge")>,
  Box<dyn Error + 'static>,
> {
  let payload = scores_payload(
    ego,
    Some(false),
    start_with,
    score_lt, score_lte,
    score_gt, score_gte,
    index,
    count
  )?;
  let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
  return make_setof_edge(&response);
}

#[pg_extern(immutable)]
fn mr_scores(
  ego           : Option<&str>,
  hide_personal : default!(Option<bool>, "false"),
  context       : default!(Option<&str>, "''"),
  start_with    : default!(Option<&str>, "''"),
  score_lt      : default!(Option<f64>,  "null"),
  score_lte     : default!(Option<f64>,  "null"),
  score_gt      : default!(Option<f64>,  "null"),
  score_gte     : default!(Option<f64>,  "null"),
  index         : default!(Option<i32>,  "null"),
  count         : default!(Option<i32>,  "null")
) -> Result<
  SetOfIterator<'static, pgrx::composite_type!('static, "mr_t_edge")>,
  Box<dyn Error + 'static>,
> {
  let context = context.unwrap_or("");
  let payload = scores_payload(
    ego,
    hide_personal,
    start_with,
    score_lt, score_lte,
    score_gt, score_gte,
    index,
    count
  )?;
  let payload  = if context.is_empty() { payload } else { contexted_payload(context, payload)? };
  let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
  return make_setof_edge(&response);
}

#[pg_extern(immutable)]
fn mr_scores_linear_sum(
  src : Option<&str>
) -> Result<
  SetOfIterator<'static, pgrx::composite_type!('static, "mr_t_edge")>,
  Box<dyn Error + 'static>,
> {
  let src      = src.expect("src should not be null");
  let payload  = rmp_serde::to_vec(&((("src", "=", src), ), (), "null"))?;
  let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
  return make_setof_edge(&response);
}

#[pg_extern(immutable)]
fn mr_graph(
  ego           : Option<&str>,
  focus         : Option<&str>,
  context       : default!(Option<&str>, "''"),
  positive_only : default!(Option<bool>, "false"),
  index         : default!(Option<i32>,  "null"),
  count         : default!(Option<i32>,  "null")
) -> Result<
  SetOfIterator<'static, pgrx::composite_type!('static, "mr_t_edge")>,
  Box<dyn Error + 'static>,
> {
  let ego           = ego.expect("ego should not be null");
  let focus         = focus.expect("focus should not be null");
  let context       = context.unwrap_or("");
  let positive_only = positive_only.unwrap_or(false);
  let index         = index.unwrap_or(0) as u32;
  let count         = count.unwrap_or(i32::MAX) as u32;
  let payload       = rmp_serde::to_vec(&(((ego, "gravity", focus), positive_only, index, count), ()))?;
  let payload       = if context.is_empty() { payload } else { contexted_payload(context, payload)? };
  let response      = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
  return make_setof_edge(&response);
}

#[pg_extern(immutable)]
fn mr_nodelist(
  context : default!(Option<&str>, "''")
) -> Result<
  SetOfIterator<'static, String>,
  Box<dyn Error + 'static>,
> {
  let context  = context.unwrap_or("");
  let payload  = rmp_serde::to_vec(&("nodes", ()))?;
  let payload  = if context.is_empty() { payload } else { contexted_payload(context, payload)? };
  let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;

  let strings : Vec<String> =
    response
      .iter()
      .map(|x : &(String,)| x.0.clone())
      .collect();
  return Ok(SetOfIterator::new(strings));
}

#[pg_extern(immutable)]
fn mr_edgelist(
  context : default!(Option<&str>, "''")
) -> Result<
  SetOfIterator<'static, pgrx::composite_type!('static, "mr_t_edge")>,
  Box<dyn Error + 'static>,
> {
  let context  = context.unwrap_or("");
  let payload  = rmp_serde::to_vec(&("edges", ()))?;
  let payload  = if context.is_empty() { payload } else { contexted_payload(context, payload)? };
  let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
  return make_setof_edge(&response);
}

#[pg_extern(immutable)]
fn mr_connected(
  ego     : Option<&str>,
  context : default!(Option<&str>, "''")
) -> Result<
  SetOfIterator<'static, pgrx::composite_type!('static, "mr_t_link")>,
  Box<dyn Error + 'static>,
> {
  let ego      = ego.expect("ego should not be null");
  let context  = context.unwrap_or("");
  let payload  = rmp_serde::to_vec(&(((ego, "connected"), ), ()))?;
  let payload  = if context.is_empty() { payload } else { contexted_payload(context, payload)? };
  let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
  return make_setof_link(&response);
}

#[pg_extern(immutable)]
fn mr_users_stats(
  ego     : Option<&str>,
  context : default!(Option<&str>, "''")
) -> Result<
  SetOfIterator<'static, pgrx::composite_type!('static, "mr_t_stats")>,
  Box<dyn Error + 'static>,
> {
  let ego      = ego.expect("ego should not be null");
  let context  = context.unwrap_or("");
  let payload  = rmp_serde::to_vec(&("users_stats", ego, ()))?;
  let payload  = if context.is_empty() { payload } else { contexted_payload(context, payload)? };
  let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
  return make_setof_stats(&response);
}

//  ================================================================
//
//    Mutable functions
//
//  ================================================================

#[pg_extern]
fn mr_put_edge(
  src     : Option<&str>,
  dest    : Option<&str>,
  weight  : Option<f64>,
  context : default!(Option<&str>, "''")
) -> Result<
  SetOfIterator<'static, pgrx::composite_type!('static, "mr_t_edge")>,
  Box<dyn Error>,
> {
  let src      = src.expect("src should not be null");
  let dest     = dest.expect("dest should not be null");
  let weight   = weight.expect("weight should not be null");
  let context  = context.unwrap_or("");
  let payload  = rmp_serde::to_vec(&(((src, dest, weight), ), ()))?;
  let payload  = if context.is_empty() { payload } else { contexted_payload(context, payload)? };
  let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
  return make_setof_edge(&response);
}

#[pg_extern]
fn mr_delete_edge(
  ego     : Option<&str>,
  target  : Option<&str>,
  context : default!(Option<&str>, "''")
) -> Result<&'static str, Box<dyn Error + 'static>> {
  let ego         = ego.expect("ego should not be null");
  let target      = target.expect("target should not be null");
  let context     = context.unwrap_or("");
  let payload     = rmp_serde::to_vec(&((("src", "delete", ego), ("dest", "delete", target)), ()))?;
  let payload     = if context.is_empty() { payload } else { contexted_payload(context, payload)? };
  let _ : Vec<()> = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
  return Ok("Ok");
}

#[pg_extern]
fn mr_delete_node(
  ego     : Option<&str>,
  context : default!(Option<&str>, "''")
) -> Result<&'static str, Box<dyn Error + 'static>> {
  let ego         = ego.expect("ego should not be null");
  let context     = context.unwrap_or("");
  let payload     = rmp_serde::to_vec(&((("src", "delete", ego), ), ()))?;
  let payload     = if context.is_empty() { payload } else { contexted_payload(context, payload)? };
  let _ : Vec<()> = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
  return Ok("Ok");
}

#[pg_extern]
fn mr_reset() -> Result<
  String,
  Box<dyn Error + 'static>,
> {
  let payload  = rmp_serde::to_vec(&(("reset"), ()))?;
  let response = request_raw(payload, None)?;
  let s    = rmp_serde::from_slice(response.as_slice())?;
  return Ok(s);
} 

#[pg_extern]
fn mr_zerorec() -> Result<
  String,
  Box<dyn Error + 'static>,
> {
  let payload  = rmp_serde::to_vec(&(("zerorec"), ()))?;
  let response = request_raw(payload, None)?;
  let s    = rmp_serde::from_slice(response.as_slice())?;
  return Ok(s);
}

//  ================================================================
//
//    Tests
//
//  ================================================================

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
  use pgrx::prelude::*;
  use super::testing::*;

  fn unpack_edge(x : &PgHeapTuple<'static, pgrx::AllocatedByRust>) -> (String, String, f64) {
    return (
      x.get_by_name("ego").unwrap().unwrap(),
      x.get_by_name("target").unwrap().unwrap(),
      x.get_by_name("score").unwrap().unwrap()
    );
  }

  fn collect_edges(i : SetOfIterator<'_, PgHeapTuple<'static, pgrx::AllocatedByRust>>) -> Vec<(String, String, f64)> {
    i.map(|x| unpack_edge(&x)).collect()
  }

  #[pg_test]
  fn zerorec_graph() {
    let _ = crate::mr_reset().unwrap();

    put_testing_edges();

    let _ = crate::mr_zerorec().unwrap();

    let res = crate::mr_graph(
      Some("Uadeb43da4abb"),
      Some("U000000000000"),
      None,
      Some(false),
      None,
      None
    ).unwrap();

    let n = res.count();

    assert!(n > 25 && n < 60);
  }

  #[pg_test]
  fn zerorec_graph_positive_only() {
    let _ = crate::mr_reset().unwrap();

    put_testing_edges();

    let _ = crate::mr_zerorec().unwrap();

    let res = crate::mr_graph(
      Some("Uadeb43da4abb"),
      Some("U000000000000"),
      None,
      Some(true),
      None,
      None
    ).unwrap();

    let n = res.count();

    assert!(n > 25 && n < 60);
  }

  #[pg_test]
  fn service() {
    let ver = crate::mr_service();

    //  check if ver is in form "X.Y.Z"
    assert_eq!(ver.split(".").map(|x|
      x.parse::<i32>().unwrap()
    ).count(), 3);
  }

  #[pg_test]
  fn edge_uncontexted() {
    let _ = crate::mr_reset().unwrap();

    let res = crate::mr_put_edge(Some("U1"), Some("U2"), Some(1.0), None).unwrap();

    let n = res.map(|x| {
      let (ego, target, score) = unpack_edge(&x);
      assert_eq!(ego,    "U1");
      assert_eq!(target, "U2");
      assert_eq!(score,  1.0);
    }).count();

    assert_eq!(n, 1);
  }

  #[pg_test]
  fn edge_contexted() {
    let _ = crate::mr_reset().unwrap();

    let res = crate::mr_put_edge(Some("U1"), Some("U2"), Some(1.0), Some("X")).unwrap();
    
    let n = res.map(|x| {
      let (ego, target, score) = unpack_edge(&x);
      assert_eq!(ego,    "U1");
      assert_eq!(target, "U2");
      assert_eq!(score,   1.0);
    }).count();

    assert_eq!(n, 1);
  }

  #[pg_test]
  fn null_context_is_sum() {
    let _ = crate::mr_reset().unwrap();

    let _ = crate::mr_put_edge(Some("U1"), Some("U2"), Some(1.0), Some("X")).unwrap();
    let _ = crate::mr_put_edge(Some("U1"), Some("U2"), Some(2.0), Some("Y")).unwrap();

    let res = crate::mr_edgelist(None).unwrap();

    let n = res.map(|x| {
      let (ego, target, score) = unpack_edge(&x);
      assert_eq!(ego,    "U1");
      assert_eq!(target, "U2");
      assert_eq!(score,  3.0);
    }).count();

    assert_eq!(n, 1);
  }

  #[pg_test]
  fn delete_contexted_edge() {
    let _ = crate::mr_reset().unwrap();

    let _ = crate::mr_put_edge(Some("U1"), Some("U2"), Some(1.0), Some("X")).unwrap();
    let _ = crate::mr_put_edge(Some("U1"), Some("U2"), Some(2.0), Some("Y")).unwrap();
    let _ = crate::mr_delete_edge(Some("U1"), Some("U2"), Some("X")).unwrap();

    //  We should still have "Y" edge.
    let res = crate::mr_edgelist(None).unwrap();

    let n = res.map(|x| {
      let (ego, target, score) = unpack_edge(&x);
      assert_eq!(ego,    "U1");
      assert_eq!(target, "U2");
      assert_eq!(score,  2.0);
    }).count();

    assert_eq!(n, 1);
  }

  #[pg_test]
  fn null_context_invariant() {
    let _ = crate::mr_reset().unwrap();

    let _ = crate::mr_put_edge(Some("U1"), Some("U2"), Some(1.0), Some("X")).unwrap();
    let _ = crate::mr_put_edge(Some("U1"), Some("U2"), Some(2.0), Some("Y")).unwrap();

    //  Delete and put back again.
    let _ = crate::mr_delete_edge(Some("U1"), Some("U2"), Some("X"));
    let _ = crate::mr_put_edge(Some("U1"), Some("U2"), Some(1.0), Some("X"));

    let res = crate::mr_edgelist(None).unwrap();

    let n = res.map(|x| {
      let (ego, target, score) = unpack_edge(&x);
      assert_eq!(ego,    "U1");
      assert_eq!(target, "U2");
      assert_eq!(score,  3.0);
    }).count();

    assert_eq!(n, 1);
  }

  #[pg_test]
  fn node_score_superposition() {
    let _ = crate::mr_reset().unwrap();

    let _ = crate::mr_put_edge(Some("U1"), Some("U2"), Some(2.0), None).unwrap();
    let _ = crate::mr_put_edge(Some("U1"), Some("U3"), Some(1.0), None).unwrap();
    let _ = crate::mr_put_edge(Some("U3"), Some("U2"), Some(3.0), None).unwrap();

    let res = crate::mr_node_score_superposition(Some("U1"), Some("U2")).unwrap();

    let n = res.map(|x| {
      let (ego, target, score) = unpack_edge(&x);
      assert_eq!(ego,    "U1");
      assert_eq!(target, "U2");
      assert!(score > 0.33);
      assert!(score < 0.43);
    }).count();

    assert_eq!(n, 1);
  }

  #[pg_test]
  fn node_score() {
    let _ = crate::mr_reset().unwrap();

    let _ = crate::mr_put_edge(Some("U1"), Some("U2"), Some(2.0), Some("X")).unwrap();
    let _ = crate::mr_put_edge(Some("U1"), Some("U3"), Some(1.0), Some("X")).unwrap();
    let _ = crate::mr_put_edge(Some("U3"), Some("U2"), Some(3.0), Some("X")).unwrap();

    let res = crate::mr_node_score(Some("U1"), Some("U2"), Some("X")).unwrap();

    let n = res.map(|x| {
      let (ego, target, score) = unpack_edge(&x);
      assert_eq!(ego, "U1");
      assert_eq!(target, "U2");
      assert!(score > 0.33);
      assert!(score < 0.43);
    }).count();

    assert_eq!(n, 1);
  }

  #[pg_test]
  fn node_score_linear_sum() {
    let _ = crate::mr_reset().unwrap();

    let _ = crate::mr_put_edge(Some("U1"), Some("U2"), Some(2.0), Some("X")).unwrap();
    let _ = crate::mr_put_edge(Some("U1"), Some("U3"), Some(1.0), Some("X")).unwrap();
    let _ = crate::mr_put_edge(Some("U3"), Some("U2"), Some(3.0), Some("X")).unwrap();

    let res = crate::mr_node_score_linear_sum(Some("U1"), Some("U2")).unwrap();

    let n = res.map(|x| {
      let (ego, target, score) = unpack_edge(&x);
      assert_eq!(ego,    "U1");
      assert_eq!(target, "U2");
      assert!(score > 0.33);
      assert!(score < 0.43);
    }).count();

    assert_eq!(n, 1);
  }

  #[pg_test]
  fn scores_superposition() {
    let _ = crate::mr_reset().unwrap();

    let _ = crate::mr_put_edge(Some("U1"), Some("U2"), Some(2.0), Some("X")).unwrap();
    let _ = crate::mr_put_edge(Some("U1"), Some("U3"), Some(1.0), Some("X")).unwrap();
    let _ = crate::mr_put_edge(Some("U2"), Some("U3"), Some(3.0), Some("X")).unwrap();

    let res = collect_edges(crate::mr_scores_superposition(
      Some("U1"),
      Some("U"),
      Some(10.0), None,
      Some(0.0), None,
      None, None
    ).unwrap());

    assert_eq!(res.len(), 3);

    assert_eq!(res[0].0, "U1");
    assert_eq!(res[0].1, "U1");
    assert!(res[0].2 > 0.2);
    assert!(res[0].2 < 0.5);

    assert_eq!(res[1].0, "U1");
    assert_eq!(res[1].1, "U3");
    assert!(res[1].2 > 0.2);
    assert!(res[1].2 < 0.5);

    assert_eq!(res[2].0, "U1");
    assert_eq!(res[2].1, "U2");
    assert!(res[2].2 > 0.1);
    assert!(res[2].2 < 0.4);
  }

  #[pg_test]
  fn scores() {
    let _ = crate::mr_reset().unwrap();

    let _ = crate::mr_put_edge(Some("U1"), Some("U2"), Some(2.0), Some("X")).unwrap();
    let _ = crate::mr_put_edge(Some("U1"), Some("U3"), Some(1.0), Some("X")).unwrap();
    let _ = crate::mr_put_edge(Some("U2"), Some("U3"), Some(3.0), Some("X")).unwrap();

    let res = collect_edges(crate::mr_scores(
      Some("U1"),
      Some(false),
      Some("X"),
      Some("U"),
      Some(10.0), None,
      Some(0.0), None,
      None, None
    ).unwrap());

    assert_eq!(res.len(), 3);

    assert_eq!(res[0].0, "U1");
    assert_eq!(res[0].1, "U1");
    assert!(res[0].2 > 0.2);
    assert!(res[0].2 < 0.5);

    assert_eq!(res[1].0, "U1");
    assert_eq!(res[1].1, "U3");
    assert!(res[1].2 > 0.2);
    assert!(res[1].2 < 0.5);

    assert_eq!(res[2].0, "U1");
    assert_eq!(res[2].1, "U2");
    assert!(res[2].2 > 0.1);
    assert!(res[2].2 < 0.4);
  }

  #[pg_test]
  fn scores_linear_sum() {
    let _ = crate::mr_reset().unwrap();

    let _ = crate::mr_put_edge(Some("U1"), Some("U2"), Some(2.0), Some("X")).unwrap();
    let _ = crate::mr_put_edge(Some("U1"), Some("U3"), Some(1.0), Some("X")).unwrap();
    let _ = crate::mr_put_edge(Some("U2"), Some("U3"), Some(3.0), Some("X")).unwrap();

    let res = collect_edges(crate::mr_scores_linear_sum(
      Some("U1"),
    ).unwrap());

    assert_eq!(res.len(), 3);

    assert_eq!(res[0].0, "U1");
    assert_eq!(res[0].1, "U1");
    assert!(res[0].2 > 0.2);
    assert!(res[0].2 < 0.5);

    assert_eq!(res[1].0, "U1");
    assert_eq!(res[1].1, "U3");
    assert!(res[1].2 > 0.2);
    assert!(res[1].2 < 0.5);

    assert_eq!(res[2].0, "U1");
    assert_eq!(res[2].1, "U2");
    assert!(res[2].2 > 0.1);
    assert!(res[2].2 < 0.4);
  }

  #[pg_test]
  fn nodelist() {
    let _ = crate::mr_reset().unwrap();

    let _ = crate::mr_put_edge(Some("U1"), Some("U2"), Some(2.0), None).unwrap();
    let _ = crate::mr_put_edge(Some("U1"), Some("U3"), Some(1.0), None).unwrap();
    let _ = crate::mr_put_edge(Some("U2"), Some("U3"), Some(3.0), None).unwrap();

    let res : Vec<String> = crate::mr_nodelist(None).unwrap().collect();

    assert_eq!(res.len(), 3);

    for x in res {
      assert!(x == "U1" || x == "U2" || x == "U3");
    }
  }

  #[pg_test]
  fn connected() {
    let _ = crate::mr_reset().unwrap();

    let _ = crate::mr_put_edge(Some("U1"), Some("U2"), Some(2.0), None).unwrap();
    let _ = crate::mr_put_edge(Some("U1"), Some("U3"), Some(1.0), None).unwrap();
    let _ = crate::mr_put_edge(Some("U2"), Some("U3"), Some(3.0), None).unwrap();

    let res : Vec<(String, String)> =
      crate::mr_connected(Some("U1"), None).unwrap()
        .map(|x| (
          x.get_by_name("source").unwrap().unwrap(),
          x.get_by_name("target").unwrap().unwrap(),
        ))
        .collect();

    assert_eq!(res.len(), 2);

    for x in res {
      assert_eq!(x.0, "U1");
      assert!(x.1 == "U2" || x.1 == "U3");
    }
  }

  #[pg_test]
  fn users_stats() {
    let _ = crate::mr_reset().unwrap();

    let _ = crate::mr_put_edge(Some("U1"), Some("U2"), Some(3.0), None).unwrap();
    let _ = crate::mr_put_edge(Some("U1"), Some("U3"), Some(1.0), None).unwrap();
    let _ = crate::mr_put_edge(Some("U2"), Some("U1"), Some(2.0), None).unwrap();
    let _ = crate::mr_put_edge(Some("U2"), Some("U3"), Some(4.0), None).unwrap();
    let _ = crate::mr_put_edge(Some("U3"), Some("U1"), Some(3.0), None).unwrap();
    let _ = crate::mr_put_edge(Some("U3"), Some("U2"), Some(2.0), None).unwrap();

    let res : Vec<(String, f64, f64)> =
      crate::mr_users_stats(Some("U1"), None).unwrap()
        .map(|x| (
          x.get_by_name("name").unwrap().unwrap(),
          x.get_by_name("score").unwrap().unwrap(),
          x.get_by_name("score_reversed").unwrap().unwrap(),
        ))
        .collect();

    assert_eq!(res.len(), 3);

    let mut u1 = true;
    let mut u2 = true;
    let mut u3 = true;

    for x in res.iter() {
      match x.0.as_str() {
        "U1" => {
          assert!(res[0].1 > 0.3);
          assert!(res[0].1 < 0.45);
          assert!(res[0].2 > 0.3);
          assert!(res[0].2 < 0.45);
          assert!(u1);
          u1 = false;
        },

        "U2" => {
          assert!(res[1].1 > 0.3);
          assert!(res[1].1 < 0.4);
          assert!(res[1].2 > 0.2);
          assert!(res[1].2 < 0.35);
          assert!(u2);
          u2 = false;
        },

        "U3" => {
          assert!(res[2].1 > 0.2);
          assert!(res[2].1 < 0.35);
          assert!(res[2].2 > 0.25);
          assert!(res[2].2 < 0.35);
          assert!(u3);
          u3 = false;
        },

        _ => {
          assert!(false);
        },
      };
    }
  }
}

#[cfg(test)]
pub mod pg_test {
  pub fn setup(_options: Vec<&str>) {
  }

  pub fn postgresql_conf_options() -> Vec<&'static str> {
    vec![]
  }
}
