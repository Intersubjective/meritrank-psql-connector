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

fn request<T: for<'a> Deserialize<'a>>(
    req: Vec<u8>,
) -> Result<Vec<T>, Box<dyn Error + 'static>> {
    let client = Socket::new(Protocol::Req0)?;
    client.dial(&SERVICE_URL)?;
    client
        .send(Message::from(req.as_slice()))
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

fn contexted_request<T: for<'a> Deserialize<'a>>(
    context: &str) -> impl Fn(Vec<u8>) -> Result<
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
    ego: &'static str,
    target: &'static str,
) -> Result<
    Vec<u8>,
    Box<dyn Error + 'static>,
> {
    let q = ((("src", "=", ego), ("dest", "=", target)), ());
    rmp_serde::to_vec(&q)
        .map_err(|e| e.into())
}

#[pg_extern]
fn mr_node_score(
    ego: &'static str,
    target: &'static str,
) -> Result<
    TableIterator<'static, (name!(node, String), name!(ego, String), name!(score, f64))>,
    Box<dyn Error + 'static>,
> {
    mr_node_score0(ego, target)
        .map(request)?
        .map(TableIterator::new)
}

#[pg_extern]
fn mr_node_score1(
    context: &'static str,
    ego: &'static str,
    target: &'static str,
) -> Result<
    TableIterator<'static, (name!(node, String), name!(ego, String), name!(score, f64))>,
    Box<dyn Error + 'static>,
> {
    mr_node_score0(ego, target)
        .map(contexted_request(context))?
        .map(TableIterator::new)
}

fn mr_scores0(
    ego: &'static str,
) -> Result<
    Vec<u8>,
    Box<dyn Error + 'static>,
> {
    let q = ((("src", "=", ego), ), ());
    rmp_serde::to_vec(&q)
        .map_err(|e| e.into())
}

#[pg_extern]
fn mr_scores(
    ego: &'static str,
) -> Result<
    TableIterator<'static, (name!(node, String), name!(ego, String), name!(score, f64))>,
    Box<dyn Error + 'static>,
> {
    mr_scores0(ego)
        .map(request)?
        .map(TableIterator::new)
}

#[pg_extern]
fn mr_scores1(
    context: &'static str,
    ego: &'static str,
) -> Result<
    TableIterator<'static, (name!(node, String), name!(ego, String), name!(score, f64))>,
    Box<dyn Error + 'static>,
> {
    mr_scores0(ego)
        .map(contexted_request(context))?
        .map(TableIterator::new)
}

fn mr_edge0(
    src: &'static str,
    dest: &'static str,
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
fn mr_edge(
    src: &'static str,
    dest: &'static str,
    weight: f64,
) -> Result<
    TableIterator<'static, (name!(node, String), name!(ego, String), name!(score, f64))>,
    Box<dyn Error>,
> {
    mr_edge0(src, dest, weight)
        .map(request)?
        .map( TableIterator::new )
}

#[pg_extern]
fn mr_edge1(
    context: &'static str,
    src: &'static str,
    dest: &'static str,
    weight: f64,
) -> Result<
    TableIterator<'static, (name!(node, String), name!(ego, String), name!(score, f64))>,
    Box<dyn Error>,
> {
    mr_edge0(src, dest, weight)
        .map(contexted_request(context))?
        .map(TableIterator::new)
}

fn mr_delete_edge0(
    ego: &'static str,
    target: &'static str,
) -> Result<Vec<u8>, Box<dyn Error + 'static>> {
    let q = ((("src", "delete", ego), ("dest", "delete", target)), ());
    rmp_serde::to_vec(&q)
        .map_err(|e| e.into())
}
#[pg_extern]
fn mr_delete_edge(
    ego: &'static str,
    target: &'static str,
) -> Result<&'static str, Box<dyn Error + 'static>> {
    mr_delete_edge0(ego, target)
        .map(request)?
        .map(|_: Vec<()>| "Ok")
}

#[pg_extern]
fn mr_delete_edge1(
    context: &str,
    ego: &'static str,
    target: &'static str,
) -> Result<&'static str, Box<dyn Error + 'static>> {
    mr_delete_edge0(ego, target)
        .map(contexted_request(context))?
        .map(|_: Vec<()>| "Ok")
        .map_err(|e| e.into())
}

fn mr_delete_node0(
    ego: &'static str,
) -> Result<Vec<u8>, Box<dyn Error + 'static>> {
    let q = ((("src", "delete", ego), ), ());
    rmp_serde::to_vec(&q)
        .map_err(|e| e.into())
}

#[pg_extern]
fn mr_delete_node(
    ego: &'static str,
) -> Result<&'static str, Box<dyn Error + 'static>> {
    mr_delete_node0(ego)
        .map( request )?
        .map(|_: Vec<()>| "Ok")
        .map_err(|e| e.into())
}

#[pg_extern]
fn mr_delete_node1(
    context: &str,
    ego: &'static str,
) -> Result<&'static str, Box<dyn Error + 'static>> {
    mr_delete_node0(ego)
        .map(contexted_request(context))?
        .map(|_: Vec<()>| "Ok")
        .map_err(|e| e.into())
}

fn mr_gravity_graph0(
    ego: &'static str,
    focus: &'static str,
) -> Result<
    Vec<u8>,
    Box<dyn Error + 'static>,
> {
    let q = (((ego, "gravity", focus), ), ());
    rmp_serde::to_vec(&q)
        .map_err(|e| e.into())
}

#[pg_extern]
fn mr_gravity_graph(
    ego: &'static str,
    focus: &'static str,
) -> Result<
    TableIterator<'static, (name!(node, String), name!(ego, String), name!(score, f64))>,
    Box<dyn Error + 'static>,
> {
    mr_gravity_graph0(ego, focus)
        .map(request)?
        .map(TableIterator::new)
}

#[pg_extern]
fn mr_gravity_graph1(
    context: &str,
    ego: &'static str,
    focus: &'static str,
) -> Result<
    TableIterator<'static, (name!(node, String), name!(ego, String), name!(score, f64))>,
    Box<dyn Error + 'static>,
> {
    mr_gravity_graph0(ego, focus)
        .map(contexted_request(context))?
        .map(TableIterator::new)
}

fn mr_gravity_nodes0(
    ego: &'static str,
    focus: &'static str,
) -> Result<
    Vec<u8>,
    Box<dyn Error + 'static>,
> {
    let q = (((ego, "gravity_nodes", focus), ), ());
    rmp_serde::to_vec(&q)
        .map_err(|e| e.into())
}

#[pg_extern]
fn mr_gravity_nodes(
    ego: &'static str,
    focus: &'static str,
) -> Result<
    TableIterator<'static, (name!(node, String), name!(weight, f64))>,
    Box<dyn Error + 'static>,
> {
    mr_gravity_nodes0(ego, focus)
        .map(request)?
        .map(TableIterator::new)
}

#[pg_extern]
fn mr_gravity_nodes1(
    context: &str,
    ego: &'static str,
    focus: &'static str,
) -> Result<
    TableIterator<'static, (name!(node, String), name!(weight, f64))>,
    Box<dyn Error + 'static>,
> {
    mr_gravity_nodes0(ego, focus)
        .map(contexted_request(context))?
        .map(request)?
        .map(TableIterator::new)
}

#[pg_extern]
fn mr_for_beacons_global() -> Result<
    TableIterator<'static, (name!(ego, String), name!(dest, String), name!(score, f64))>,
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


#[pg_extern]
fn mr_nodes() -> Result<
    TableIterator<'static, (name!(id, String), )>,
    Box<dyn Error + 'static>,
> {
    mr_nodes0()
        .map(request::<String>)?
        .map(|v| v.into_iter().map(|s| (s,))) // wrap to single-element tuple
        .map(TableIterator::new)
}

#[pg_extern]
fn mr_nodes1(context: &str) -> Result<
    TableIterator<'static, (name!(id, String), )>,
    Box<dyn Error + 'static>,
> {
    mr_nodes0()
        .map(contexted_request(context))?
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

#[pg_extern]
fn mr_edges() -> Result<
    TableIterator<'static, (name!(source, String), name!(target, String), name!(weight, f64))>,
    Box<dyn Error + 'static>,
> {
    mr_edges0()
        .map(request)?
        .map(TableIterator::new)
}

#[pg_extern]
fn mr_edges1(context: &str) -> Result<
    TableIterator<'static, (name!(source, String), name!(target, String), name!(weight, f64))>,
    Box<dyn Error + 'static>,
> {
    mr_edges0()
        .map(contexted_request(context))?
        .map(request)?
        .map(TableIterator::new)
}

// connected nodes
fn mr_connected0(ego: &'static str) -> Result<
    Vec<u8>,
    Box<dyn Error + 'static>,
> {
    let q = (((ego, "connected"), ), ());
    rmp_serde::to_vec(&q)
        .map_err(|e| e.into())
}
#[pg_extern]
fn mr_connected(ego: &'static str) -> Result<
    TableIterator<'static, (name!(src, String), name!(dest, String))>,
    Box<dyn Error + 'static>,
> {
    mr_connected0(ego)
        .map(request)?
        .map(TableIterator::new)
}

#[pg_extern]
fn mr_connected1(
    context: &str,
    ego: &'static str
) -> Result<
    TableIterator<'static, (name!(src, String), name!(dest, String))>,
    Box<dyn Error + 'static>,
> {
    mr_connected0(ego)
        .map(contexted_request(context))?
        .map(TableIterator::new)
}
