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

#[pg_extern]
fn mr_service_url() -> &'static str {
    &SERVICE_URL
}

#[pg_extern]
fn mr_connector() ->  &'static str { &VERSION.unwrap_or("unknown") }

#[pg_extern]
fn mr_service() -> &'static str {
    let q = "ver";
    let client = Socket::new(Protocol::Req0)
        .unwrap();
    client.dial(&SERVICE_URL)
        .unwrap();
    client
        .send(Message::from(q.as_bytes()))
        .unwrap();
    let msg: Message = client.recv()
        .unwrap();
    let slice: &[u8] = msg.as_slice();

    let s: &str = std::str::from_utf8(slice).unwrap();
    s.to_string().leak() // Rust 1.72.0
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
    context: &str,
    ego: &str,
    target: &str,
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
    let (gcmp, gt) = match (score_gt, score_gte) {
        (Some(gt), None) => (">", gt),
        (None, Some(gte)) => (">=", gte),
        (None, None) => (">", f64::MIN),
        _ => return Err(Box::from("either gt or gte allowed!"))
    };
    let (lcmp, lt) = match (score_lt, score_lte) {
        (Some(lt), None) => ("<", lt),
        (None, Some(lte)) => ("<=", lte),
        (None, None) => ("<", f64::MAX),
        _ => return Err(Box::from("either lt or lte allowed!"))
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
    context: &str,
    ego: &str,
    hide_personal: bool,
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
    let ctx = mr_scores0(ego,
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
    mr_scores00(ego) //mr_scores0(ego, None, None, None, None, None, None)
        .map(request)?
        .map(TableIterator::new)
}

//#[pg_extern]
fn mr_scores_simple(
    context: &str,
    ego: &str
) -> Result<
    TableIterator<'static, (name!(ego, String), name!(target, String), name!(score, f64))>,
    Box<dyn Error + 'static>,
> {
    mr_scores(context,  ego, false,  None, None, None, None, None, None)
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

fn mr_edge0(
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
fn mr_edge1(
    src: &str,
    dest: &str,
    weight: f64,
) -> Result<
    TableIterator<'static, (name!(ego, String), name!(target, String), name!(score, f64))>,
    Box<dyn Error>,
> {
    mr_edge0(src, dest, weight)
        .map(request)?
        .map( TableIterator::new )
}

#[pg_extern]
fn mr_edge(
    context: &str,
    src: &str,
    dest: &str,
    weight: f64,
) -> Result<
    TableIterator<'static, (name!(ego, String), name!(target, String), name!(score, f64))>,
    Box<dyn Error>,
> {
    let ctx = mr_edge0(src, dest, weight);
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
    context: &str,
    ego: &str,
    target: &str,
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
    context: &str,
    ego: &str,
) -> Result<&'static str, Box<dyn Error + 'static>> {
    let ctx = mr_delete_node0(ego);
    let ctx = if context.is_empty() { ctx } else { ctx.map(contexted_request(context))? };
    ctx
        .map(request)?
        .map(|_: Vec<()>| "Ok")
        .map_err(|e| e.into())
}

fn mr_gravity_graph0(
    ego: &str,
    focus: &str,
) -> Result<
    Vec<u8>,
    Box<dyn Error + 'static>,
> {
    let q = (((ego, "gravity", focus), ), ());
    rmp_serde::to_vec(&q)
        .map_err(|e| e.into())
}

//#[pg_extern]
fn mr_gravity_graph1(
    ego: &str,
    focus: &str,
) -> Result<
    TableIterator<'static, (name!(ego, String), name!(target, String), name!(score, f64))>,
    Box<dyn Error + 'static>,
> {
    mr_gravity_graph0(ego, focus)
        .map(request)?
        .map(TableIterator::new)
}

#[pg_extern]
fn mr_gravity_graph(
    context: &str,
    ego: &str,
    focus: &str,
) -> Result<
    TableIterator<'static, (name!(ego, String), name!(target, String), name!(score, f64))>,
    Box<dyn Error + 'static>,
> {
    let ctx = mr_gravity_graph0(ego, focus);
    let ctx = if context.is_empty() { ctx } else { ctx.map(contexted_request(context))? };
    ctx
        .map(request)?
        .map(TableIterator::new)
}

fn mr_gravity_nodes0(
    ego: &str,
    focus: &str,
) -> Result<
    Vec<u8>,
    Box<dyn Error + 'static>,
> {
    let q = (((ego, "gravity_nodes", focus), ), ());
    rmp_serde::to_vec(&q)
        .map_err(|e| e.into())
}

//#[pg_extern]
fn mr_gravity_nodes1(
    ego: &str,
    focus: &str,
) -> Result<
    TableIterator<'static, (name!(node, String), name!(weight, f64))>,
    Box<dyn Error + 'static>,
> {
    mr_gravity_nodes0(ego, focus)
        .map(request)?
        .map(TableIterator::new)
}

#[pg_extern]
fn mr_gravity_nodes(
    context: &str,
    ego: &str,
    focus: &str,
) -> Result<
    TableIterator<'static, (name!(node, String), name!(weight, f64))>,
    Box<dyn Error + 'static>,
> {
    let ctx = mr_gravity_nodes0(ego, focus);
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

fn mr_nodes0() -> Result<
    Vec<u8>,
    Box<dyn Error + 'static>,
> {
    let q = ("nodes", ());
    rmp_serde::to_vec(&q)
        .map_err(|e| e.into())
}


//#[pg_extern]
fn mr_nodes1() -> Result<
    TableIterator<'static, (name!(id, String), )>,
    Box<dyn Error + 'static>,
> {
    mr_nodes0()
        .map(request::<String>)?
        .map(|v| v.into_iter().map(|s| (s,))) // wrap to single-element tuple
        .map(TableIterator::new)
}

#[pg_extern]
fn mr_nodes(context: &str) -> Result<
    TableIterator<'static, (name!(id, String), )>,
    Box<dyn Error + 'static>,
> {
    let ctx = mr_nodes0();
    let ctx = if context.is_empty() { ctx } else { ctx.map(contexted_request(context)).unwrap() };
    ctx
        .map(request::<String>)?
        .map(|v| v.into_iter().map(|s| (s,))) // wrap to single-element tuple
        .map(TableIterator::new)
}

fn mr_edges0() -> Result<
    Vec<u8>,
    Box<dyn Error + 'static>,
> {
    let q = ("edges", ());
    rmp_serde::to_vec(&q)
        .map_err(|e| e.into())
}

//#[pg_extern]
fn mr_edges1() -> Result<
    TableIterator<'static, (name!(source, String), name!(target, String), name!(weight, f64))>,
    Box<dyn Error + 'static>,
> {
    mr_edges0()
        .map(request)?
        .map(TableIterator::new)
}

#[pg_extern]
fn mr_edges(
    context: &str
) -> Result<
    TableIterator<'static, (name!(source, String), name!(target, String), name!(weight, f64))>,
    Box<dyn Error + 'static>,
> {
    let ctx = mr_edges0();
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
    context: &str,
    ego: &str
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
