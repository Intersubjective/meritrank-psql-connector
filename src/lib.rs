use lazy_static::lazy_static;
use nng::*;
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

fn request<T: for<'a> Deserialize<'a>>(
    q: Vec<u8>,
) -> Result<Vec<T>, Box<dyn Error + 'static>> {
    let client = Socket::new(Protocol::Req0)?;
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

fn contexted_request<T: for<'a> Deserialize<'a>>(
    context: &str
) -> impl Fn(Vec<u8>) -> Result<
    Vec<T>,
    Box<dyn Error + 'static>,
> + '_ {
    move |payload: Vec<u8>| {
        //let q: (&str, &str, &[u8]) = ("context", context, payload.as_slice()); // why not working?
        let q: (&str, &str, Vec<u8>) = ("context", context, payload);
        rmp_serde::to_vec( & q).map(request)?
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
    let ctx = mr_node_score0(ego, target);
    let ctx = if context.is_empty() { ctx } else { ctx.map(contexted_request(context)).unwrap() };
    ctx
        .map(request)?
        .map(TableIterator::new)
}

//#[pg_extern]
fn mr_node_score1(
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


fn mr_scores00(
    ego: &str
) -> Result<
    Vec<u8>,
    Box<dyn Error + 'static>,
> {
    let q = ((("src", "=", ego),
              //("target", "like", ""),
              ),
             ());
    rmp_serde::to_vec(&q)
        .map_err(|e| e.into())
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

//#[pg_extern]
fn mr_scores1(
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
    let ctx = mr_scores0(
        ego,
        hide_personal,
        start_with,
        score_lt, score_lte,
        score_gt, score_gte,
        limit
    );
    let ctx = if context.is_empty() { ctx } else { ctx.map(contexted_request(context)).unwrap() };
    ctx
        .map(request)?
        .map(TableIterator::new)
}

//#[pg_extern]
fn mr_scores_simple1(
    ego: &str
) -> Result<
    TableIterator<'static, (name!(ego, String), name!(target, String), name!(score, f64))>,
    Box<dyn Error + 'static>,
> {
    mr_scores00(ego)
        .map(request)?
        .map(TableIterator::new)
}

//#[pg_extern]
fn mr_scores_simple(
    ego: &str,
    context: &str,
) -> Result<
    TableIterator<'static, (name!(ego, String), name!(target, String), name!(score, f64))>,
    Box<dyn Error + 'static>,
> {
    mr_scores( ego, false,  context, None, None, None, None, None, None)
}

/*
#[pg_extern]
fn mr_scores_linear_sum(
    ego: &str,
    target_like: &str,  // = ""
    score_gt: f64,      // = f64::MIN
    score_gte: bool,
    limit: Option<i32>
) -> Result<
    TableIterator<'static, (name!(ego, String), name!(target, String), name!(score, f64))>,
    Box<dyn Error + 'static>,
> {
    let cmp = if score_gte { ">=" } else { ">" };
    let q = ((("src", "=", ego), ("target", "like", target_like), ("score", cmp, score_gt), ("limit", limit)), (), "null");
    rmp_serde::to_vec(&q)
        .map(request)?
        .map(TableIterator::new)
}
*/

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

//#[pg_extern]
fn mr_put_edge1(
    src: &str,
    dest: &str,
    weight: f64,
) -> Result<
    TableIterator<'static, (name!(ego, String), name!(target, String), name!(score, f64))>,
    Box<dyn Error>,
> {
    mr_put_edge0(src, dest, weight)
        .map(request)?
        .map( TableIterator::new )
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
//#[pg_extern]
fn mr_delete_edge1(
    ego: &str,
    target: &str,
) -> Result<&'static str, Box<dyn Error + 'static>> {
    mr_delete_edge0(ego, target)
        .map(request)?
        .map(|_: Vec<()>| "Ok")
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

//#[pg_extern]
fn mr_delete_node1(
    ego: &str,
) -> Result<&'static str, Box<dyn Error + 'static>> {
    mr_delete_node0(ego)
        .map( request )?
        .map(|_: Vec<()>| "Ok")
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

//#[pg_extern]
fn mr_graph1(
    ego: &str,
    focus: &str,
) -> Result<
    TableIterator<'static, (name!(ego, String), name!(target, String), name!(score, f64))>,
    Box<dyn Error + 'static>,
> {
    mr_graph0(ego, focus, true, Some(3))
        .map(request)?
        .map(TableIterator::new)
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

//#[pg_extern]
fn mr_nodes1(
    ego: &str,
    focus: &str,
) -> Result<
    TableIterator<'static, (name!(node, String), name!(weight, f64))>,
    Box<dyn Error + 'static>,
> {
    mr_nodes0(ego, focus, false, Some(i32::MAX))
        .map(request)?
        .map(TableIterator::new)
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

//#[pg_extern]
fn mr_nodelist1() -> Result<
    TableIterator<'static, (name!(id, String), )>,
    Box<dyn Error + 'static>,
> {
    mr_nodelist0()
        .map(request::<String>)?
        .map(|v| v.into_iter().map(|s| (s,))) // wrap to single-element tuple
        .map(TableIterator::new)
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

//#[pg_extern]
fn mr_edgelist1() -> Result<
    TableIterator<'static, (name!(source, String), name!(target, String), name!(weight, f64))>,
    Box<dyn Error + 'static>,
> {
    mr_edgelist0()
        .map(request)?
        .map(TableIterator::new)
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
//#[pg_extern]
fn mr_connected1(
    ego: &str
) -> Result<
    TableIterator<'static, (name!(source, String), name!(target, String))>,
    Box<dyn Error + 'static>,
> {
    mr_connected0(ego)
        .map(request)?
        .map(TableIterator::new)
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
        //  FIXME
        //  Hardcoded service version
        assert_eq!(crate::mr_service().as_str(), "0.2.2");
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
