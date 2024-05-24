//  TODO
//
//  - Broken functions:
//
//  mr_for_beacons_global
//  ERROR:  IO error while reading marker: failed to fill whole buffer
//

use lazy_static::lazy_static;
use nng::*;
use nng::options::{Options, RecvTimeout};
use std::time::Duration;
use pgrx::iter::TableIterator;
use pgrx::*;
use serde::de::Deserialize;
use std::env::var;
use std::error::Error;
use core::result::Result;

// pgx specific macros
pg_module_magic!();

lazy_static! {
    static ref SERVICE_URL: String =
        var("RUST_SERVICE_URL").unwrap_or("tcp://127.0.0.1:10234".to_string());
}

const VERSION: Option<&str> = option_env!("CARGO_PKG_VERSION");

const RECV_TIMEOUT_MSEC : u64 = 10000;

fn request<T: for<'a> Deserialize<'a>>(
    q: Vec<u8>,
) -> Result<Vec<T>, Box<dyn Error + 'static>> {
    let client = Socket::new(Protocol::Req0)?;
    client.set_opt::<RecvTimeout>(Some(Duration::from_millis(RECV_TIMEOUT_MSEC)))?;
    client.dial(&SERVICE_URL)?;
    client
        .send(Message::from(q.as_slice()))
        .map_err(|(_, err)| err)?;
    let msg: Message = client.recv()?;
    let slice: &[u8] = msg.as_slice();
    rmp_serde::from_slice(slice).or_else(|_| {
        let err: String = rmp_serde::from_slice(slice)?;
        Err(Box::from(format!("Server error: {}", err)))
    })
}

fn contexted_request(
    context: &str
) -> impl Fn(Vec<u8>) -> Result<
    Vec<u8>,
    Box<dyn Error + 'static>,
> + '_ {
    move |payload: Vec<u8>| {
        //let q: (&str, &str, &[u8]) = ("context", context, payload.as_slice()); // why not working?
        let q: (&str, &str, Vec<u8>) = ("context", context, payload);
        match rmp_serde::to_vec(&q) {
            Ok(x)  => Ok(x),
            Err(x) => Err(x.into())
        }
    }
}

///  Information functions
#[pg_extern]
fn mr_service_url() -> &'static str {
    &SERVICE_URL
}

#[pg_extern]
fn mr_connector() ->  &'static str { &VERSION.unwrap_or("unknown") }

fn mr_service0() -> Result<String, Box<dyn Error + 'static>> {
    //  FIXME
    //  Code duplication with `fn request()`
    let q = "ver";
    let client = Socket::new(Protocol::Req0)?;
    client.set_opt::<RecvTimeout>(Some(Duration::from_millis(RECV_TIMEOUT_MSEC)))?;
    client.dial(&SERVICE_URL)?;
    client.send(Message::from(rmp_serde::to_vec(&q)?.as_slice()))
        .map_err(|(_, err)| err)?;
    let msg: Message = client.recv()?;
    let slice: &[u8] = msg.as_slice();

    let s: String = rmp_serde::from_slice(slice)?;
    Ok( s )
}
#[pg_extern]
fn mr_service() -> String {
    match mr_service0() {
        Err(e) => format!("{}", e),
        Ok(s) => s
    }
}

/// Basic functions

fn mr_node_score0(
    ego: &str,
    target: &str,
) -> Result<
    Vec<u8>,
    Box<dyn Error + 'static>,
> {
    let q = ((("src", "=", ego), ("dest", "=", target)), ());
    rmp_serde::to_vec(&q)
        .map_err(|e| e.into())
}

#[pg_extern]
fn mr_node_score_superposition(
    ego: &str,
    target: &str,
) -> Result<
    TableIterator<'static, (name!(ego, String), name!(target, String), name!(score, f64))>,
    Box<dyn Error + 'static>,
> {
    mr_node_score0(ego, target)
        .map(request)?
        .map(TableIterator::new)
}

#[pg_extern]
fn mr_node_score(
    ego: &str,
    target: &str,
    context: &str,
) -> Result<
    TableIterator<'static, (name!(ego, String), name!(target, String), name!(score, f64))>,
    Box<dyn Error + 'static>,
> {
    let payload = mr_node_score0(ego, target);
    let payload = if context.is_empty() { payload } else { payload.map(contexted_request(context)).unwrap() };
    payload
        .map(request)?
        .map(TableIterator::new)
}

#[pg_extern]
fn mr_node_score_linear_sum(
    ego: &str,
    target: &str,
) -> Result<
    TableIterator<'static, (name!(ego, String), name!(target, String), name!(score, f64))>,
    Box<dyn Error + 'static>,
> {
    let q = ((("src", "=", ego), ("dest", "=", target)), (), "null");
    rmp_serde::to_vec(&q)
        .map(request)?
        .map(TableIterator::new)
}


fn mr_scores0(
    ego: &str,
    hide_personal: bool,
    start_with: Option<String>,
    score_lt: Option<f64>,
    score_lte: Option<f64>,
    score_gt: Option<f64>,
    score_gte: Option<f64>,
    limit: Option<i32>
) -> Result<
    Vec<u8>,
    Box<dyn Error + 'static>,
> {
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
    let binding = start_with.unwrap_or(String::new());
    let q = ((
              ("src", "=", ego),
              ("target", "like", binding.as_str()),
              ("hide_personal", hide_personal),
              ("score", gcmp, gt),
              ("score", lcmp, lt),
              ("limit", limit)
             ),
             ());
    rmp_serde::to_vec(&q)
        .map_err(|e| e.into())
}

#[pg_extern]
fn mr_scores_superposition(
    ego: &str,
    start_with: Option<String>,
    score_lt: Option<f64>,
    score_lte: Option<f64>,
    score_gt: Option<f64>,
    score_gte: Option<f64>,
    limit: Option<i32>
) -> Result<
    TableIterator<'static, (name!(ego, String), name!(target, String), name!(score, f64))>,
    Box<dyn Error + 'static>,
> {
    mr_scores0(ego,
               false,
               start_with,
               score_lt, score_lte,
               score_gt, score_gte,
               limit
    )
        .map(request)?
        .map(TableIterator::new)
}

#[pg_extern]
fn mr_scores(
    ego: &str,
    hide_personal: bool,
    context: &str,
    start_with: Option<String>,
    score_lt: Option<f64>,
    score_lte: Option<f64>,
    score_gt: Option<f64>,
    score_gte: Option<f64>,
    limit: Option<i32>
) -> Result<
    TableIterator<'static, (name!(ego, String), name!(target, String), name!(score, f64))>,
    Box<dyn Error + 'static>,
> {
    let payload = mr_scores0(
        ego,
        hide_personal,
        start_with,
        score_lt, score_lte,
        score_gt, score_gte,
        limit
    );
    let payload = if context.is_empty() { payload } else { payload.map(contexted_request(context)).unwrap() };
    payload
        .map(request)?
        .map(TableIterator::new)
}

#[pg_extern]
fn mr_scores_linear_sum(
    src: &str,
) -> Result<
    TableIterator<'static, (name!(ego, String), name!(target, String), name!(score, f64))>,
    Box<dyn Error + 'static>,
> {
    let q = ((("src", "=", src), ), (), "null");
    rmp_serde::to_vec(&q)
        .map(request)?
        .map(TableIterator::new)
}

#[pg_extern]
fn mr_score_linear_sum(
    src: &str,
    dest: &str
) -> Result<
    TableIterator<'static, (name!(ego, String), name!(target, String), name!(score, f64))>,
    Box<dyn Error + 'static>,
> {
    let q = ((("src", "=", src), ("dest", "=", dest)), (), "null");
    rmp_serde::to_vec(&q)
        .map(request)?
        .map(TableIterator::new)
}

/// Modify functions
fn mr_put_edge0(
    src: &str,
    dest: &str,
    weight: f64,
) -> Result<
    Vec<u8>,
    Box<dyn Error>,
> {
    let q = (((src, dest, weight), ), ());
    rmp_serde::to_vec(&q)
        .map_err(|e| e.into())
}

#[pg_extern]
fn mr_put_edge(
    src: &str,
    dest: &str,
    weight: f64,
    context: &str,
) -> Result<
    TableIterator<'static, (name!(ego, String), name!(target, String), name!(score, f64))>,
    Box<dyn Error>,
> {
    let ctx = mr_put_edge0(src, dest, weight);
    let ctx = if context.is_empty() { ctx } else { ctx.map(contexted_request(context))? };
    ctx
        .map(request)?
        .map(TableIterator::new)
}

fn mr_delete_edge0(
    ego: &str,
    target: &str,
) -> Result<Vec<u8>, Box<dyn Error + 'static>> {
    let q = ((("src", "delete", ego), ("dest", "delete", target)), ());
    rmp_serde::to_vec(&q)
        .map_err(|e| e.into())
}

#[pg_extern]
fn mr_delete_edge(
    ego: &str,
    target: &str,
    context: &str,
) -> Result<&'static str, Box<dyn Error + 'static>> {
    let ctx = mr_delete_edge0(ego, target);
    let ctx = if context.is_empty() { ctx } else { ctx.map(contexted_request(context))? };
    ctx
        .map(request)?
        .map(|_: Vec<()>| "Ok")
        .map_err(|e| e.into())
}

fn mr_delete_node0(
    ego: &str,
) -> Result<Vec<u8>, Box<dyn Error + 'static>> {
    let q = ((("src", "delete", ego), ), ());
    rmp_serde::to_vec(&q)
        .map_err(|e| e.into())
}

#[pg_extern]
fn mr_delete_node(
    ego: &str,
    context: &str,
) -> Result<&'static str, Box<dyn Error + 'static>> {
    let ctx = mr_delete_node0(ego);
    let ctx = if context.is_empty() { ctx } else { ctx.map(contexted_request(context))? };
    ctx
        .map(request)?
        .map(|_: Vec<()>| "Ok")
        .map_err(|e| e.into())
}

/// Gravity functions
fn mr_graph0(
    ego: &str,
    focus: &str,
    positive_only: bool,
    limit: Option<i32>
) -> Result<
    Vec<u8>,
    Box<dyn Error + 'static>,
> {
    let q = (((ego, "gravity", focus), positive_only, limit), ());
    rmp_serde::to_vec(&q)
        .map_err(|e| e.into())
}

#[pg_extern]
fn mr_graph(
    ego: &str,
    focus: &str,
    context: &str,
    positive_only: bool,
    limit: Option<i32>
) -> Result<
    TableIterator<'static, (name!(ego, String), name!(target, String), name!(score, f64))>,
    Box<dyn Error + 'static>,
> {
    let ctx = mr_graph0(ego, focus, positive_only, limit);
    let ctx = if context.is_empty() { ctx } else { ctx.map(contexted_request(context))? };
    ctx
        .map(request)?
        .map(TableIterator::new)
}

fn mr_nodes0(
    ego: &str,
    focus: &str,
    positive_only: bool,
    limit: Option<i32>
) -> Result<
    Vec<u8>,
    Box<dyn Error + 'static>,
> {
    let q = (((ego, "gravity_nodes", focus), positive_only, limit), ());
    rmp_serde::to_vec(&q)
        .map_err(|e| e.into())
}

#[pg_extern]
fn mr_nodes(
    ego: &str,
    focus: &str,
    context: &str,
    positive_only: bool,
    limit: Option<i32>
) -> Result<
    TableIterator<'static, (name!(node, String), name!(weight, f64))>,
    Box<dyn Error + 'static>,
> {
    let ctx = mr_nodes0(ego, focus, positive_only, limit);
    let ctx = if context.is_empty() { ctx } else { ctx.map(contexted_request(context))? };
    ctx
        .map(request)?
        .map(TableIterator::new)
}

#[pg_extern]
fn mr_for_beacons_global() -> Result<
    TableIterator<'static, (name!(ego, String), name!(target, String), name!(score, f64))>,
    Box<dyn Error + 'static>,
> {
    let q = ("for_beacons_global", ());
    rmp_serde::to_vec(&q)
        .map(request)?
        .map(TableIterator::new)
}

/// list functions

fn mr_nodelist0() -> Result<
    Vec<u8>,
    Box<dyn Error + 'static>,
> {
    let q = ("nodes", ());
    rmp_serde::to_vec(&q)
        .map_err(|e| e.into())
}

#[pg_extern]
fn mr_nodelist(context: &str) -> Result<
    TableIterator<'static, (name!(id, String), )>,
    Box<dyn Error + 'static>,
> {
    let ctx = mr_nodelist0();
    let ctx = if context.is_empty() { ctx } else { ctx.map(contexted_request(context)).unwrap() };
    ctx
        .map(request::<String>)?
        .map(|v| v.into_iter().map(|s| (s,))) // wrap to single-element tuple
        .map(TableIterator::new)
}

fn mr_edgelist0() -> Result<
    Vec<u8>,
    Box<dyn Error + 'static>,
> {
    let q = ("edges", ());
    rmp_serde::to_vec(&q)
        .map_err(|e| e.into())
}

#[pg_extern]
fn mr_edgelist(
    context: &str
) -> Result<
    TableIterator<'static, (name!(source, String), name!(target, String), name!(weight, f64))>,
    Box<dyn Error + 'static>,
> {
    let ctx = mr_edgelist0();
    let ctx = if context.is_empty() { ctx } else { ctx.map(contexted_request(context))? };
    ctx
        .map(request)?
        .map(TableIterator::new)
}

// connected nodes
fn mr_connected0(
    ego: &str
) -> Result<
    Vec<u8>,
    Box<dyn Error + 'static>,
> {
    let q = (((ego, "connected"), ), ());
    rmp_serde::to_vec(&q)
        .map_err(|e| e.into())
}

#[pg_extern]
fn mr_connected(
    ego: &str,
    context: &str,
) -> Result<
    TableIterator<'static, (name!(source, String), name!(target, String))>,
    Box<dyn Error + 'static>,
> {
    let ctx = mr_connected0(ego);
    let ctx = if context.is_empty() { ctx } else { ctx.map(contexted_request(context))? };
    ctx
        .map(request)?
        .map(TableIterator::new)
}

//
//  Testing
//

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_service() {
        println!("Rust service URL: {}", super::SERVICE_URL.as_str());

        let ver = crate::mr_service();

        //  check of ver is in form "X.Y.Z"
        assert_eq!(ver.split(".").map(|x|
            x.parse::<u32>().unwrap()
        ).count(), 3);
    }

    #[pg_test]
    fn test_edge_uncontexted() {
        let res = crate::mr_put_edge("U1", "U2", 1.0, "").unwrap();

        let n = res.map(|x| {
            assert_eq!(x.0, "U1");
            assert_eq!(x.1, "U2");
            assert_eq!(x.2, 1.0);
        }).count();

        assert_eq!(n, 1);

        let _ = crate::mr_delete_edge("U1", "U2", "");
    }

    #[pg_test]
    fn test_edge_contexted() {
        let res = crate::mr_put_edge("U1", "U2", 1.0, "X").unwrap();
        
        let n = res.map(|x| {
            assert_eq!(x.0, "U1");
            assert_eq!(x.1, "U2");
            assert_eq!(x.2, 1.0);
        }).count();

        assert_eq!(n, 1);

        let _ = crate::mr_delete_edge("U1", "U2", "X");
    }

    #[pg_test]
    fn test_null_context_is_sum() {
        let _ = crate::mr_put_edge("U1", "U2", 1.0, "X");
        let _ = crate::mr_put_edge("U1", "U2", 2.0, "Y");

        let res = crate::mr_edgelist("").unwrap();

        let n = res.map(|x| {
            assert_eq!(x.0, "U1");
            assert_eq!(x.1, "U2");
            assert_eq!(x.2, 3.0);
        }).count();

        assert_eq!(n, 1);

        let _ = crate::mr_delete_edge("U1", "U2", "X");
        let _ = crate::mr_delete_edge("U1", "U2", "Y");
    }


    #[pg_test]
    fn test_delete_contexted_edge() {
        let _ = crate::mr_put_edge("U1", "U2", 1.0, "X");
        let _ = crate::mr_put_edge("U1", "U2", 2.0, "Y");
        let _ = crate::mr_delete_edge("U1", "U2", "X");

        //  We should still have "Y" edge.
        let res = crate::mr_edgelist("").unwrap();

        let n = res.map(|x| {
            assert_eq!(x.0, "U1");
            assert_eq!(x.1, "U2");
            assert_eq!(x.2, 2.0);
        }).count();

        assert_eq!(n, 1);

        let _ = crate::mr_delete_edge("U1", "U2", "Y");
    }

    #[pg_test]
    fn test_null_context_invariant() {
        let _ = crate::mr_put_edge("U1", "U2", 1.0, "X");
        let _ = crate::mr_put_edge("U1", "U2", 2.0, "Y");

        //  Delete and put back again.
        let _ = crate::mr_delete_edge("U1", "U2", "X");
        let _ = crate::mr_put_edge("U1", "U2", 1.0, "X");

        let res = crate::mr_edgelist("").unwrap();

        let n = res.map(|x| {
            assert_eq!(x.0, "U1");
            assert_eq!(x.1, "U2");
            assert_eq!(x.2, 3.0);
        }).count();

        assert_eq!(n, 1);

        let _ = crate::mr_delete_edge("U1", "U2", "X");
        let _ = crate::mr_delete_edge("U1", "U2", "Y");
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
