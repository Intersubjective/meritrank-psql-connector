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

#[cfg(any(test, feature = "pg_test"))]
pub mod testing;

// pgx specific macros
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

const VERSION : Option<&str> = option_env!("CARGO_PKG_VERSION");

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
) -> Result<Vec<T>, Box<dyn Error + 'static>> {
    let msg           = request_raw(payload, timeout_msec)?;
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

///  Information functions
#[pg_extern]
fn mr_service_url() -> &'static str {
    &SERVICE_URL
}

#[pg_extern]
fn mr_connector() ->  &'static str { &VERSION.unwrap_or("unknown") }

fn mr_service_wrapped() -> Result<String, Box<dyn Error + 'static>> {
    let payload  = rmp_serde::to_vec(&"ver")?;
    let response = request_raw(payload, Some(*RECV_TIMEOUT_MSEC))?;
    let s        = rmp_serde::from_slice(response.as_slice())?;
    return Ok(s);
}

#[pg_extern]
fn mr_service() -> String {
    match mr_service_wrapped() {
        Err(e) => format!("{}", e),
        Ok(s)  => s
    }
}

/// Basic functions

#[pg_extern]
fn mr_node_score_superposition(
    ego: &str,
    target: &str,
) -> Result<
    TableIterator<'static, (name!(ego, String), name!(target, String), name!(score, f64))>,
    Box<dyn Error + 'static>,
> {
    let payload  = rmp_serde::to_vec(&((("src", "=", ego), ("dest", "=", target)), ()))?;
    let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
    return Ok(TableIterator::new(response));
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
    let payload  = rmp_serde::to_vec(&((("src", "=", ego), ("dest", "=", target)), ()))?;
    let payload  = if context.is_empty() { payload } else { contexted_payload(context, payload)? };
    let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
    return Ok(TableIterator::new(response));
}

#[pg_extern]
fn mr_node_score_linear_sum(
    ego: &str,
    target: &str,
) -> Result<
    TableIterator<'static, (name!(ego, String), name!(target, String), name!(score, f64))>,
    Box<dyn Error + 'static>,
> {
    let payload  = rmp_serde::to_vec(&((("src", "=", ego), ("dest", "=", target)), (), "null"))?;
    let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
    return Ok(TableIterator::new(response));
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
    let payload = mr_scores0(
        ego,
        false,
        start_with,
        score_lt, score_lte,
        score_gt, score_gte,
        limit
    )?;
    let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
    return Ok(TableIterator::new(response));
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
    )?;
    let payload  = if context.is_empty() { payload } else { contexted_payload(context, payload)? };
    let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
    return Ok(TableIterator::new(response));
}

#[pg_extern]
fn mr_scores_linear_sum(
    src: &str,
) -> Result<
    TableIterator<'static, (name!(ego, String), name!(target, String), name!(score, f64))>,
    Box<dyn Error + 'static>,
> {
    let payload  = rmp_serde::to_vec(&((("src", "=", src), ), (), "null"))?;
    let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
    return Ok(TableIterator::new(response));
}

#[pg_extern]
fn mr_score_linear_sum(
    src: &str,
    dest: &str
) -> Result<
    TableIterator<'static, (name!(ego, String), name!(target, String), name!(score, f64))>,
    Box<dyn Error + 'static>,
> {
    let payload = rmp_serde::to_vec(&((("src", "=", src), ("dest", "=", dest)), (), "null"))?;
    let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
    return Ok(TableIterator::new(response));
}

/// Modify functions

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
    let payload  = rmp_serde::to_vec(&(((src, dest, weight), ), ()))?;
    let payload  = if context.is_empty() { payload } else { contexted_payload(context, payload)? };
    let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
    return Ok(TableIterator::new(response));
}

#[pg_extern]
fn mr_delete_edge(
    ego: &str,
    target: &str,
    context: &str,
) -> Result<&'static str, Box<dyn Error + 'static>> {
    let payload     = rmp_serde::to_vec(&((("src", "delete", ego), ("dest", "delete", target)), ()))?;
    let payload     = if context.is_empty() { payload } else { contexted_payload(context, payload)? };
    let _ : Vec<()> = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
    return Ok("Ok");
}

#[pg_extern]
fn mr_delete_node(
    ego: &str,
    context: &str,
) -> Result<&'static str, Box<dyn Error + 'static>> {
    let payload     = rmp_serde::to_vec(&((("src", "delete", ego), ), ()))?;
    let payload     = if context.is_empty() { payload } else { contexted_payload(context, payload)? };
    let _ : Vec<()> = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
    return Ok("Ok");
}

/// Gravity functions

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
    let payload  = rmp_serde::to_vec(&(((ego, "gravity", focus), positive_only, limit), ()))?;
    let payload  = if context.is_empty() { payload } else { contexted_payload(context, payload)? };
    let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
    return Ok(TableIterator::new(response));
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
    let payload  = rmp_serde::to_vec(&(((ego, "gravity_nodes", focus), positive_only, limit), ()))?;
    let payload  = if context.is_empty() { payload } else { contexted_payload(context, payload)? };
    let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
    return Ok(TableIterator::new(response));
}

/// list functions

#[pg_extern]
fn mr_nodelist(context: &str) -> Result<
    TableIterator<'static, (name!(id, String), )>,
    Box<dyn Error + 'static>,
> {
    let payload  = rmp_serde::to_vec(&("nodes", ()))?;
    let payload  = if context.is_empty() { payload } else { contexted_payload(context, payload)? };
    let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
    return Ok(TableIterator::new(response));
}

#[pg_extern]
fn mr_edgelist(
    context: &str
) -> Result<
    TableIterator<'static, (name!(source, String), name!(target, String), name!(weight, f64))>,
    Box<dyn Error + 'static>,
> {
    let payload  = rmp_serde::to_vec(&("edges", ()))?;
    let payload  = if context.is_empty() { payload } else { contexted_payload(context, payload)? };
    let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
    return Ok(TableIterator::new(response));
}

// connected nodes

#[pg_extern]
fn mr_connected(
    ego: &str,
    context: &str,
) -> Result<
    TableIterator<'static, (name!(source, String), name!(target, String))>,
    Box<dyn Error + 'static>,
> {
    let payload  = rmp_serde::to_vec(&(((ego, "connected"), ), ()))?;
    let payload  = if context.is_empty() { payload } else { contexted_payload(context, payload)? };
    let response = request(payload, Some(*RECV_TIMEOUT_MSEC))?;
    return Ok(TableIterator::new(response));
}

#[pg_extern]
fn mr_reset() -> Result<
    String,
    Box<dyn Error + 'static>,
> {
    let payload  = rmp_serde::to_vec(&(("reset"), ()))?;
    let response = request_raw(payload, None)?;
    let s        = rmp_serde::from_slice(response.as_slice())?;
    return Ok(s);
} 

#[pg_extern]
fn mr_zerorec() -> Result<
    String,
    Box<dyn Error + 'static>,
> {
    let payload  = rmp_serde::to_vec(&(("zerorec"), ()))?;
    let response = request_raw(payload, None)?;
    let s        = rmp_serde::from_slice(response.as_slice())?;
    return Ok(s);
}

//
//  Testing
//

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;
    use super::testing::*;

    #[pg_test]
    fn zerorec_graph() {
        put_testing_edges();

        let _ = crate::mr_zerorec().unwrap();

        let res = crate::mr_graph("Uadeb43da4abb", "U000000000000", "", false, Some(10000)).unwrap();

        let n = res.count();

        assert!(n > 25 && n < 60);

        let _ = crate::mr_reset().unwrap();
    }

    #[pg_test]
    fn zerorec_graph_positive_only() {
        put_testing_edges();

        let _ = crate::mr_zerorec().unwrap();

        let res = crate::mr_graph("Uadeb43da4abb", "U000000000000", "", true, Some(10000)).unwrap();
        let n = res.count();

        assert!(n > 25 && n < 60);

        let _ = crate::mr_reset().unwrap();
    }

    #[pg_test]
    fn service() {
        let ver = crate::mr_service();

        //  check if ver is in form "X.Y.Z"
        assert_eq!(ver.split(".").map(|x|
            x.parse::<u32>().unwrap()
        ).count(), 3);
    }

    #[pg_test]
    fn edge_uncontexted() {
        let res = crate::mr_put_edge("U1", "U2", 1.0, "").unwrap();

        let n = res.map(|x| {
            assert_eq!(x.0, "U1");
            assert_eq!(x.1, "U2");
            assert_eq!(x.2, 1.0);
        }).count();

        assert_eq!(n, 1);

        let _ = crate::mr_reset().unwrap();
    }

    #[pg_test]
    fn edge_contexted() {
        let res = crate::mr_put_edge("U1", "U2", 1.0, "X").unwrap();
        
        let n = res.map(|x| {
            assert_eq!(x.0, "U1");
            assert_eq!(x.1, "U2");
            assert_eq!(x.2, 1.0);
        }).count();

        assert_eq!(n, 1);
        let _ = crate::mr_reset().unwrap();
    }

    #[pg_test]
    fn null_context_is_sum() {
        let _ = crate::mr_put_edge("U1", "U2", 1.0, "X").unwrap();
        let _ = crate::mr_put_edge("U1", "U2", 2.0, "Y").unwrap();

        let res = crate::mr_edgelist("").unwrap();

        let n = res.map(|x| {
            assert_eq!(x.0, "U1");
            assert_eq!(x.1, "U2");
            assert_eq!(x.2, 3.0);
        }).count();

        assert_eq!(n, 1);

        let _ = crate::mr_reset().unwrap();
    }


    #[pg_test]
    fn delete_contexted_edge() {
        let _ = crate::mr_put_edge("U1", "U2", 1.0, "X").unwrap();
        let _ = crate::mr_put_edge("U1", "U2", 2.0, "Y").unwrap();
        let _ = crate::mr_delete_edge("U1", "U2", "X").unwrap();

        //  We should still have "Y" edge.
        let res = crate::mr_edgelist("").unwrap();

        let n = res.map(|x| {
            assert_eq!(x.0, "U1");
            assert_eq!(x.1, "U2");
            assert_eq!(x.2, 2.0);
        }).count();

        assert_eq!(n, 1);

        let _ = crate::mr_reset().unwrap();
    }

    #[pg_test]
    fn null_context_invariant() {
        let _ = crate::mr_put_edge("U1", "U2", 1.0, "X").unwrap();
        let _ = crate::mr_put_edge("U1", "U2", 2.0, "Y").unwrap();

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

        let _ = crate::mr_reset().unwrap();
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
