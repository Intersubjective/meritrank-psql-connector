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
use meritrank_service::commands::*;

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
-- deprecated
DROP FUNCTION IF EXISTS mr_for_beacons_global;
DROP FUNCTION IF EXISTS mr_score_linear_sum;
DROP FUNCTION IF EXISTS mr_nodes;
DROP FUNCTION IF EXISTS mr_users_stats;
DROP VIEW     IF EXISTS mr_t_node;
DROP VIEW     IF EXISTS mr_t_stats;

CREATE OR REPLACE VIEW mr_t_edge AS SELECT
  '' ::text             AS src,
  '' ::text             AS dst,
  (0)::double precision AS score
  WHERE false;

CREATE OR REPLACE VIEW mr_t_link AS SELECT
  ''::text AS src,
  ''::text AS dst
  WHERE false;

CREATE OR REPLACE VIEW mr_t_mutual_score AS SELECT
  '' ::text             AS src,
  '' ::text             AS dst,
  (0)::double precision AS dst_score,
  (0)::double precision AS src_score
  WHERE false;
"#,
  name      = "bootstrap_raw",
  bootstrap,
  creates   = [Type(mr_t_edge), Type(mr_t_link), Type(mr_t_mutual_score)],
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
  payload      : Vec<u8>,
  timeout_msec : Option<u64>,
) -> Result<T, Box<dyn Error + 'static>> {
  let msg = request_raw(payload, timeout_msec)?;
  let slice : &[u8] = msg.as_slice();
  rmp_serde::from_slice(slice).or_else(|_| {
    let err: String = rmp_serde::from_slice(slice)?;
    Err(Box::from(format!("Server error: {}", err)))
  })
}

fn service_wrapped() -> Result<String, Box<dyn Error + 'static>> {
  let payload  = rmp_serde::to_vec(&(
    CMD_VERSION,
    "",
    rmp_serde::to_vec(&())?
  ))?;

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
        edge.set_by_name("src",    ego.as_str()).unwrap();
        edge.set_by_name("dst", target.as_str()).unwrap();
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
        edge.set_by_name("src",    ego.as_str()).unwrap();
        edge.set_by_name("dst", target.as_str()).unwrap();
        return edge;
      })
      .collect();
  return Ok(SetOfIterator::new(tuples));
}

fn make_setof_mutual_score(src : &str, response : &Vec<(String, f64, f64)>) -> Result<
  SetOfIterator<'static, pgrx::composite_type!('static, "mr_t_mutual_score")>,
  Box<dyn Error + 'static>,
> {
  let tuples : Vec<PgHeapTuple<'_, AllocatedByRust>> =
    response
      .iter()
      .map(|(dst, dst_score, src_score)| {
        let mut score = PgHeapTuple::new_composite_type("mr_t_mutual_score").unwrap();
        score.set_by_name("src",       src).unwrap();
        score.set_by_name("dst",       dst.as_str()).unwrap();
        score.set_by_name("dst_score", *dst_score).unwrap();
        score.set_by_name("src_score", *src_score).unwrap();
        return score;
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
  src : Option<&str>,
  dst : Option<&str>,
) -> Result<
  SetOfIterator<'static, pgrx::composite_type!('static, "mr_t_edge")>,
  Box<dyn Error + 'static>,
> {
  let ego    = src.expect("src should not be null");
  let target = dst.expect("dst should not be null");

  let args = rmp_serde::to_vec(&(
    ego,
    target
  ))?;

  let payload = rmp_serde::to_vec(&(
    CMD_NODE_SCORE,
    "",
    args
  ))?;

  let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
  return make_setof_edge(&response);
}

#[pg_extern(immutable)]
fn mr_node_score(
  src     : Option<&str>,
  dst     : Option<&str>,
  context : default!(Option<&str>, "''"),
) -> Result<
  SetOfIterator<'static, pgrx::composite_type!('static, "mr_t_edge")>,
  Box<dyn Error + 'static>,
> {
  let context  = context.unwrap_or("");
  let ego      = src.expect("src should not be null");
  let target   = dst.expect("dst should not be null");

  let args = rmp_serde::to_vec(&(
    ego,
    target
  ))?;

  let payload  = rmp_serde::to_vec(&(
    CMD_NODE_SCORE,
    context,
    args
  ))?;

  let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
  return make_setof_edge(&response);
}

#[pg_extern(immutable)]
fn mr_node_score_linear_sum(
  src : Option<&str>,
  dst : Option<&str>,
) -> Result<
  SetOfIterator<'static, pgrx::composite_type!('static, "mr_t_edge")>,
  Box<dyn Error + 'static>,
> {
  let ego    = src.expect("src should not be null");
  let target = dst.expect("dst should not be null");

  let args = rmp_serde::to_vec(&(
    ego,
    target
  ))?;

  let payload = rmp_serde::to_vec(&(
    CMD_NODE_SCORE_NULL,
    "",
    args
  ))?;

  let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
  return make_setof_edge(&response);
}

fn scores_payload(
  context       : Option<&str>,
  src           : Option<&str>,
  hide_personal : Option<bool>,
  kind          : Option<&str>,
  lt            : Option<f64>,
  lte           : Option<f64>,
  gt            : Option<f64>,
  gte           : Option<f64>,
  index         : Option<i32>,
  count         : Option<i32>
) -> Result<
  Vec<u8>,
  Box<dyn Error + 'static>,
> {
  let context       = context.unwrap_or("");
  let ego           = src.expect("ego should not be null");
  let hide_personal = hide_personal.unwrap_or(false);
  let k             = kind.unwrap_or("");
  let index         = index.unwrap_or(0) as u32;
  let count         = count.unwrap_or(i32::MAX) as u32;
  if lt.is_some() && lte.is_some() {
    return Err(Box::from("either lt or lte is allowed!"));
  }
  if gt.is_some() && gte.is_some() {
    return Err(Box::from("either gt or gte is allowed!"));
  }

  let args = rmp_serde::to_vec(&(
    ego,
    k,
    hide_personal,
    lt.unwrap_or(lte.unwrap_or(i32::MAX.into())),
    lte.is_some(),
    gt.unwrap_or(gte.unwrap_or(i32::MIN.into())),
    gte.is_some(),
    index,
    count
  ))?;

  let payload = rmp_serde::to_vec(&(
    CMD_SCORES,
    context,
    args
  ));

  payload.map_err(|e| e.into())
}

#[pg_extern(immutable)]
fn mr_scores_superposition(
  src   : Option<&str>,
  kind  : default!(Option<&str>, "''"),
  lt    : default!(Option<f64>,  "null"),
  lte   : default!(Option<f64>,  "null"),
  gt    : default!(Option<f64>,  "null"),
  gte   : default!(Option<f64>,  "null"),
  index : default!(Option<i32>,  "0"),
  count : default!(Option<i32>,  "16")
) -> Result<
  SetOfIterator<'static, pgrx::composite_type!('static, "mr_t_edge")>,
  Box<dyn Error + 'static>,
> {
  let payload = scores_payload(
    None,
    src,
    Some(false),
    kind,
    lt, lte,
    gt, gte,
    index,
    count
  )?;

  let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
  return make_setof_edge(&response);
}

#[pg_extern(immutable)]
fn mr_scores(
  src           : Option<&str>,
  hide_personal : default!(Option<bool>, "false"),
  context       : default!(Option<&str>, "''"),
  kind          : default!(Option<&str>, "''"),
  lt            : default!(Option<f64>,  "null"),
  lte           : default!(Option<f64>,  "null"),
  gt            : default!(Option<f64>,  "null"),
  gte           : default!(Option<f64>,  "null"),
  index         : default!(Option<i32>,  "0"),
  count         : default!(Option<i32>,  "16")
) -> Result<
  SetOfIterator<'static, pgrx::composite_type!('static, "mr_t_edge")>,
  Box<dyn Error + 'static>,
> {
  let payload = scores_payload(
    context,
    src,
    hide_personal,
    kind,
    lt, lte,
    gt, gte,
    index,
    count
  )?;

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
  let src = src.expect("src should not be null");

  let args = rmp_serde::to_vec(&(
    src
  ))?;

  let payload = rmp_serde::to_vec(&(
    CMD_SCORES_NULL,
    "",
    args
  ))?;

  let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
  return make_setof_edge(&response);
}

#[pg_extern(immutable)]
fn mr_graph(
  src           : Option<&str>,
  focus         : Option<&str>,
  context       : default!(Option<&str>, "''"),
  positive_only : default!(Option<bool>, "false"),
  index         : default!(Option<i32>,  "0"),
  count         : default!(Option<i32>,  "16")
) -> Result<
  SetOfIterator<'static, pgrx::composite_type!('static, "mr_t_edge")>,
  Box<dyn Error + 'static>,
> {
  let context       = context.unwrap_or("");
  let ego           = src.expect("src should not be null");
  let focus         = focus.expect("focus should not be null");
  let positive_only = positive_only.unwrap_or(false);
  let index         = index.unwrap_or(0) as u32;
  let count         = count.unwrap_or(i32::MAX) as u32;

  let args = rmp_serde::to_vec(&(
    ego,
    focus,
    positive_only,
    index,
    count
  ))?;

  let payload = rmp_serde::to_vec(&(
    CMD_GRAPH,
    context,
    args
  ))?;

  let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
  return make_setof_edge(&response);
}

#[pg_extern(immutable)]
fn mr_nodelist(
  context : default!(Option<&str>, "''")
) -> Result<
  SetOfIterator<'static, String>,
  Box<dyn Error + 'static>,
> {
  let context = context.unwrap_or("");

  let payload = rmp_serde::to_vec(&(
    CMD_NODE_LIST,
    context,
    rmp_serde::to_vec(&())?
  ))?;

  let response : Vec<_> = request(payload, Some(*RECV_TIMEOUT_MSEC))?;

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
  let context = context.unwrap_or("");

  let payload = rmp_serde::to_vec(&(
    CMD_EDGES,
    context,
    rmp_serde::to_vec(&())?
  ))?;

  let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
  return make_setof_edge(&response);
}

#[pg_extern(immutable)]
fn mr_connected(
  src     : Option<&str>,
  context : default!(Option<&str>, "''")
) -> Result<
  SetOfIterator<'static, pgrx::composite_type!('static, "mr_t_link")>,
  Box<dyn Error + 'static>,
> {
  let context = context.unwrap_or("");
  let ego     = src.expect("src should not be null");

  let args = rmp_serde::to_vec(&(
    ego
  ))?;

  let payload = rmp_serde::to_vec(&(
    CMD_CONNECTED,
    context,
    args
  ))?;

  let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
  return make_setof_link(&response);
}

#[pg_extern(immutable)]
fn mr_mutual_scores(
  src     : Option<&str>,
  context : default!(Option<&str>, "''")
) -> Result<
  SetOfIterator<'static, pgrx::composite_type!('static, "mr_t_mutual_score")>,
  Box<dyn Error + 'static>,
> {
  let ego     = src.expect("src should not be null");
  let context = context.unwrap_or("");

  let args = rmp_serde::to_vec(&(
    ego
  ))?;

  let payload = rmp_serde::to_vec(&(
    CMD_MUTUAL_SCORES,
    context,
    args
  ))?;

  let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
  return make_setof_mutual_score(ego, &response);
}

#[pg_extern(immutable)]
fn mr_sync(
  timeout : default!(Option<i32>, "60000"),
) -> Result<
  &'static str,
  Box<dyn Error + 'static>
> {
  let timeout = if timeout.is_some() { Some(timeout.unwrap() as u64) } else { None };

  let payload = rmp_serde::to_vec(&(
    CMD_SYNC,
    "",
    rmp_serde::to_vec(&())?
  ))?;

  let _ : () = request(payload, timeout)?;
  return Ok("Ok");
}

//  ================================================================
//
//    Mutable functions
//
//  ================================================================

#[pg_extern]
fn mr_log_level(
  log_level : default!(Option<i32>, "1"),
) -> Result<&'static str, Box<dyn Error + 'static>> {
  let log_level = log_level.unwrap_or(0);

  let payload = rmp_serde::to_vec(&(
    CMD_LOG_LEVEL,
    "",
    rmp_serde::to_vec(&(log_level as u32))?
  ))?;

  let _ : () = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
  return Ok("Ok");
}

#[pg_extern]
fn mr_put_edge(
  src     : Option<&str>,
  dst     : Option<&str>,
  weight  : Option<f64>,
  context : default!(Option<&str>, "''")
) -> Result<
  SetOfIterator<'static, pgrx::composite_type!('static, "mr_t_edge")>,
  Box<dyn Error + 'static>,
> {
  let context = context.unwrap_or("");
  let src     = src.expect("src should not be null");
  let dest    = dst.expect("dst should not be null");
  let weight  = weight.expect("weight should not be null");

  let args = rmp_serde::to_vec(&(
    src,
    dest,
    weight
  ))?;

  let payload = rmp_serde::to_vec(&(
    CMD_PUT_EDGE,
    context,
    args
  ))?;

  let _ : () = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
  return make_setof_edge(&vec![(src.to_string(), dest.to_string(), weight)]);
}

#[pg_extern]
fn mr_delete_edge(
  src     : Option<&str>,
  dst     : Option<&str>,
  context : default!(Option<&str>, "''")
) -> Result<&'static str, Box<dyn Error + 'static>> {
  let context = context.unwrap_or("");
  let ego     = src.expect("src should not be null");
  let target  = dst.expect("dst should not be null");

  let args = rmp_serde::to_vec(&(
    ego,
    target
  ))?;

  let payload = rmp_serde::to_vec(&(
    CMD_DELETE_EDGE,
    context,
    args
  ))?;

  let _ : () = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
  return Ok("Ok");
}

#[pg_extern]
fn mr_delete_node(
  src     : Option<&str>,
  context : default!(Option<&str>, "''")
) -> Result<&'static str, Box<dyn Error + 'static>> {
  let context = context.unwrap_or("");
  let ego     = src.expect("src should not be null");

  let args = rmp_serde::to_vec(&(
    ego,
  ))?;

  let payload = rmp_serde::to_vec(&(
    CMD_DELETE_NODE,
    context,
    args
  ))?;

  let _ : () = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
  return Ok("Ok");
}

#[pg_extern]
fn mr_reset() -> Result<
  &'static str,
  Box<dyn Error + 'static>,
> {
  let payload  = rmp_serde::to_vec(&(
    CMD_RESET,
    "",
    rmp_serde::to_vec(&())?
  ))?;

  let _ : () = request(payload, None)?;
  return Ok("Ok");
}

#[pg_extern]
fn mr_zerorec() -> Result<
  &'static str,
  Box<dyn Error + 'static>,
> {
  let payload  = rmp_serde::to_vec(&(
    CMD_RECALCULATE_ZERO,
    "",
    rmp_serde::to_vec(&())?
  ))?;

  let response = request_raw(payload, None)?;
  let _ : () = rmp_serde::from_slice(response.as_slice())?;
  return Ok("Ok");
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
      x.get_by_name("src").unwrap().unwrap(),
      x.get_by_name("dst").unwrap().unwrap(),
      x.get_by_name("score").unwrap().unwrap()
    );
  }

  fn collect_edges(i : SetOfIterator<'_, PgHeapTuple<'static, pgrx::AllocatedByRust>>) -> Vec<(String, String, f64)> {
    i.map(|x| unpack_edge(&x)).collect()
  }

  #[pg_test]
  fn zerorec_graph_all() {
    let _ = crate::mr_reset().unwrap();

    put_testing_edges();

    let _ = crate::mr_zerorec().unwrap();
    let _ = crate::mr_sync(None).unwrap();

    let res = crate::mr_graph(
      Some("Uadeb43da4abb"),
      Some("U000000000000"),
      None,
      Some(false),
      None,
      None
    ).unwrap();

    let n = res.count();

    assert!(n > 25);
    assert!(n < 120);
  }

  #[pg_test]
  fn zerorec_graph_positive_only() {
    let _ = crate::mr_reset().unwrap();

    put_testing_edges();

    let _ = crate::mr_zerorec().unwrap();
    let _ = crate::mr_sync(None).unwrap();

    let res = crate::mr_graph(
      Some("Uadeb43da4abb"),
      Some("U000000000000"),
      None,
      Some(true),
      None,
      None
    ).unwrap();

    let n = res.count();

    assert!(n > 25);
    assert!(n < 120);
  }

  #[pg_test]
  fn zerorec_scores() {
    let _ = crate::mr_reset().unwrap();

    put_testing_edges();

    let _ = crate::mr_zerorec().unwrap();
    let _ = crate::mr_sync(None).unwrap();

    let res = crate::mr_scores(
      Some("Uadeb43da4abb"),
      Some(true),
      Some(""),
      Some("B"),
      None,
      None,
      Some(0.0),
      None,
      Some(0),
      Some(i32::MAX)
    ).unwrap();

    let n = res.count();

    assert!(n > 5);
    assert!(n < 80);
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
    let _ = crate::mr_sync(None).unwrap();

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
    let _ = crate::mr_sync(None).unwrap();

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
    let _ = crate::mr_sync(None).unwrap();

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
    let _ = crate::mr_sync(None).unwrap();

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
    let _ = crate::mr_sync(None).unwrap();

    //  Delete and put back again.
    let _ = crate::mr_delete_edge(Some("U1"), Some("U2"), Some("X"));
    let _ = crate::mr_put_edge(Some("U1"), Some("U2"), Some(1.0), Some("X"));
    let _ = crate::mr_sync(None).unwrap();

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
    let _ = crate::mr_sync(None).unwrap();

    let res = crate::mr_node_score_superposition(Some("U1"), Some("U2")).unwrap();

    let n = res.map(|x| {
      let (ego, target, score) = unpack_edge(&x);
      assert_eq!(ego,    "U1");
      assert_eq!(target, "U2");
      assert!(score > 0.3);
      assert!(score < 0.45);
    }).count();

    assert_eq!(n, 1);
  }

  #[pg_test]
  fn node_score_context() {
    let _ = crate::mr_reset().unwrap();

    let _ = crate::mr_put_edge(Some("U1"), Some("U2"), Some(2.0), Some("X")).unwrap();
    let _ = crate::mr_put_edge(Some("U1"), Some("U3"), Some(1.0), Some("X")).unwrap();
    let _ = crate::mr_put_edge(Some("U3"), Some("U2"), Some(3.0), Some("X")).unwrap();
    let _ = crate::mr_sync(None).unwrap();

    let res = crate::mr_node_score(Some("U1"), Some("U2"), Some("X")).unwrap();

    let n = res.map(|x| {
      let (ego, target, score) = unpack_edge(&x);
      assert_eq!(ego, "U1");
      assert_eq!(target, "U2");
      assert!(score > 0.3);
      assert!(score < 0.45);
    }).count();

    assert_eq!(n, 1);
  }

  #[pg_test]
  fn node_score_linear_sum() {
    let _ = crate::mr_reset().unwrap();

    let _ = crate::mr_put_edge(Some("U1"), Some("U2"), Some(2.0), Some("X")).unwrap();
    let _ = crate::mr_put_edge(Some("U1"), Some("U3"), Some(1.0), Some("X")).unwrap();
    let _ = crate::mr_put_edge(Some("U3"), Some("U2"), Some(3.0), Some("X")).unwrap();
    let _ = crate::mr_sync(None).unwrap();

    let res = crate::mr_node_score_linear_sum(Some("U1"), Some("U2")).unwrap();

    let n = res.map(|x| {
      let (ego, target, score) = unpack_edge(&x);
      assert_eq!(ego,    "U1");
      assert_eq!(target, "U2");
      assert!(score > 0.3);
      assert!(score < 0.45);
    }).count();

    assert_eq!(n, 1);
  }

  #[pg_test]
  fn scores_superposition() {
    let _ = crate::mr_reset().unwrap();

    let _ = crate::mr_put_edge(Some("U1"), Some("U2"), Some(2.0), Some("X")).unwrap();
    let _ = crate::mr_put_edge(Some("U1"), Some("U3"), Some(1.0), Some("X")).unwrap();
    let _ = crate::mr_put_edge(Some("U2"), Some("U3"), Some(3.0), Some("X")).unwrap();
    let _ = crate::mr_sync(None).unwrap();

    let res = collect_edges(crate::mr_scores_superposition(
      Some("U1"),
      Some("U"),
      Some(10.0), None,
      Some(0.0), None,
      None, None
    ).unwrap());

    assert_eq!(res.len(), 3);

    let mut u1 = true;
    let mut u2 = true;
    let mut u3 = true;

    for x in res.iter() {
      assert_eq!(x.0, "U1");

      match x.1.as_str() {
        "U1" => {
          assert_eq!(x.0, "U1");
          assert_eq!(x.1, "U1");
          assert!(x.2 > 0.2);
          assert!(x.2 < 0.5);
          assert!(u1);
          u1 = false;
        },

        "U2" => {
          assert_eq!(x.0, "U1");
          assert_eq!(x.1, "U2");
          assert!(x.2 > 0.1);
          assert!(x.2 < 0.4);
          assert!(u2);
          u2 = false;
        },

        "U3" => {
          assert_eq!(x.0, "U1");
          assert_eq!(x.1, "U3");
          assert!(x.2 > 0.2);
          assert!(x.2 < 0.5);
          assert!(u3);
          u3 = false;
        },

        _ => {
          assert!(false);
        },
      };
    }
  }

  #[pg_test]
  fn scores_null_context() {
    let _ = crate::mr_reset().unwrap();

    let _ = crate::mr_put_edge(Some("U1"), Some("U2"), Some(2.0), Some("")).unwrap();
    let _ = crate::mr_put_edge(Some("U1"), Some("U3"), Some(1.0), Some("")).unwrap();
    let _ = crate::mr_put_edge(Some("U2"), Some("U3"), Some(3.0), Some("")).unwrap();
    let _ = crate::mr_sync(None).unwrap();

    let res = collect_edges(crate::mr_scores(
      Some("U1"),
      Some(false),
      Some(""),
      Some("U"),
      Some(10.0), None,
      Some(0.0), None,
      None, None
    ).unwrap());

    assert_eq!(res.len(), 3);

    for x in res {
      assert_eq!(x.0, "U1");

      match x.1.as_str() {
        "U1" => {
          assert!(x.2 > 0.2);
          assert!(x.2 < 0.5);
        },

        "U2" => {
          assert!(x.2 > 0.1);
          assert!(x.2 < 0.4);
        },

        "U3" => {
          assert!(x.2 > 0.2);
          assert!(x.2 < 0.5);
        },

        _ => assert!(false),
      }
    }
  }

  #[pg_test]
  fn scores_context() {
    let _ = crate::mr_reset().unwrap();

    let _ = crate::mr_put_edge(Some("U1"), Some("U2"), Some(2.0), Some("X")).unwrap();
    let _ = crate::mr_put_edge(Some("U1"), Some("U3"), Some(1.0), Some("X")).unwrap();
    let _ = crate::mr_put_edge(Some("U2"), Some("U3"), Some(3.0), Some("X")).unwrap();
    let _ = crate::mr_sync(None).unwrap();

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

    for x in res {
      assert_eq!(x.0, "U1");

      match x.1.as_str() {
        "U1" => {
          assert!(x.2 > 0.2);
          assert!(x.2 < 0.5);
        },

        "U2" => {
          assert!(x.2 > 0.1);
          assert!(x.2 < 0.4);
        },

        "U3" => {
          assert!(x.2 > 0.2);
          assert!(x.2 < 0.5);
        },

        _ => assert!(false),
      }
    }
  }

  #[pg_test]
  fn scores_defaults() {
    let _ = crate::mr_reset().unwrap();

    let _ = crate::mr_put_edge(Some("U1"), Some("U2"), Some(2.0), Some("X")).unwrap();
    let _ = crate::mr_put_edge(Some("U1"), Some("U3"), Some(1.0), Some("X")).unwrap();
    let _ = crate::mr_put_edge(Some("U2"), Some("U3"), Some(3.0), Some("X")).unwrap();
    let _ = crate::mr_sync(None).unwrap();

    let res = collect_edges(crate::mr_scores(
      Some("U1"),
      Some(false),
      Some("X"),
      Some("U"),
      None, None,
      None, None,
      None, None
    ).unwrap());

    assert_eq!(res.len(), 3);


    for x in res {
      assert_eq!(x.0, "U1");

      match x.1.as_str() {
        "U1" => {
          assert!(x.2 > 0.2);
          assert!(x.2 < 0.5);
        },

        "U2" => {
          assert!(x.2 > 0.1);
          assert!(x.2 < 0.4);
        },

        "U3" => {
          assert!(x.2 > 0.2);
          assert!(x.2 < 0.5);
        },

        _ => assert!(false),
      }
    }
  }

  #[pg_test]
  fn scores_linear_sum() {
    let _ = crate::mr_reset().unwrap();

    let _ = crate::mr_put_edge(Some("U1"), Some("U2"), Some(2.0), Some("X")).unwrap();
    let _ = crate::mr_put_edge(Some("U1"), Some("U3"), Some(1.0), Some("X")).unwrap();
    let _ = crate::mr_put_edge(Some("U2"), Some("U3"), Some(3.0), Some("X")).unwrap();
    let _ = crate::mr_sync(None).unwrap();

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
    let _ = crate::mr_sync(None).unwrap();

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
    let _ = crate::mr_sync(None).unwrap();

    let res : Vec<(String, String)> =
      crate::mr_connected(Some("U1"), None).unwrap()
        .map(|x| (
          x.get_by_name("src").unwrap().unwrap(),
          x.get_by_name("dst").unwrap().unwrap(),
        ))
        .collect();

    assert_eq!(res.len(), 2);

    for x in res {
      assert_eq!(x.0, "U1");
      assert!(x.1 == "U2" || x.1 == "U3");
    }
  }

  #[pg_test]
  fn mutual_scores() {
    let _ = crate::mr_reset().unwrap();

    let _ = crate::mr_put_edge(Some("U1"), Some("U2"), Some(3.0), None).unwrap();
    let _ = crate::mr_put_edge(Some("U1"), Some("U3"), Some(1.0), None).unwrap();
    let _ = crate::mr_put_edge(Some("U2"), Some("U1"), Some(2.0), None).unwrap();
    let _ = crate::mr_put_edge(Some("U2"), Some("U3"), Some(4.0), None).unwrap();
    let _ = crate::mr_put_edge(Some("U3"), Some("U1"), Some(3.0), None).unwrap();
    let _ = crate::mr_put_edge(Some("U3"), Some("U2"), Some(2.0), None).unwrap();
    let _ = crate::mr_sync(None).unwrap();

    let res : Vec<(String, String, f64, f64)> =
      crate::mr_mutual_scores(Some("U1"), None).unwrap()
        .map(|x| (
          x.get_by_name("src").unwrap().unwrap(),
          x.get_by_name("dst").unwrap().unwrap(),
          x.get_by_name("dst_score").unwrap().unwrap(),
          x.get_by_name("src_score").unwrap().unwrap(),
        ))
        .collect();

    assert_eq!(res.len(), 3);

    let mut u1 = true;
    let mut u2 = true;
    let mut u3 = true;

    for x in res.iter() {
      assert_eq!(x.0, "U1");

      match x.1.as_str() {
        "U1" => {
          assert!(res[0].2 > 0.25);
          assert!(res[0].2 < 0.45);
          assert!(res[0].3 > 0.25);
          assert!(res[0].3 < 0.45);
          assert!(u1);
          u1 = false;
        },

        "U2" => {
          assert!(res[1].2 > 0.25);
          assert!(res[1].2 < 0.4);
          assert!(res[1].3 > 0.15);
          assert!(res[1].3 < 0.35);
          assert!(u2);
          u2 = false;
        },

        "U3" => {
          assert!(res[2].2 > 0.15);
          assert!(res[2].2 < 0.35);
          assert!(res[2].3 > 0.2);
          assert!(res[2].3 < 0.35);
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
