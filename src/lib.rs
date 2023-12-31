use lazy_static::lazy_static;
use nng::*;
use pgrx::iter::TableIterator;
use pgrx::*;
use serde::de::Deserialize;
use std::env::var;

// pgx specific macros
pg_module_magic!();

lazy_static! {
    static ref SERVICE_URL: String =
        var("RUST_SERVICE_URL").unwrap_or("tcp://127.0.0.1:10234".to_string());
}

fn request<T: for<'a> Deserialize<'a>>(
    req: &Vec<u8>,
) -> core::result::Result<Vec<T>, Box<dyn std::error::Error + 'static>> {
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

#[pg_extern]
fn mr_node_score(
    ego: &'static str,
    target: &'static str,
) -> core::result::Result<
    TableIterator<'static, (name!(node, String), name!(ego, String), name!(score, f64))>,
    Box<dyn std::error::Error + 'static>,
> {
    let rq = ((("src", "=", ego), ("dest", "=", target)), ());
    let req = rmp_serde::to_vec(&rq)?;
    let res = request(&req);
    res.map(|v| TableIterator::new(v))
}

#[pg_extern]
fn mr_scores(
    ego: &'static str,
) -> core::result::Result<
    TableIterator<'static, (name!(node, String), name!(ego, String), name!(score, f64))>,
    Box<dyn std::error::Error + 'static>,
> {
    let rq = ((("src", "=", ego), ), ());
    let req = rmp_serde::to_vec(&rq)?;
    let res = request(&req);
    res.map(|v| TableIterator::new(v))
}

#[pg_extern]
fn mr_edge(
    src: &'static str,
    dest: &'static str,
    weight: f64,
) -> core::result::Result<
    TableIterator<'static, (name!(node, String), name!(ego, String), name!(score, f64))>,
    Box<dyn std::error::Error>,
> {
    let rq = (((src, dest, weight), ), ());
    let req = rmp_serde::to_vec(&rq)?;
    let res = request(&req);
    res.map(|v| TableIterator::new(v))
}

#[pg_extern]
fn mr_delete_edge(
    ego: &'static str,
    target: &'static str,
) -> core::result::Result<&'static str, Box<dyn std::error::Error + 'static>> {
    let rq = ((("src", "delete", ego), ("dest", "delete", target)), ());
    let req = rmp_serde::to_vec(&rq)?;
    let _res: Vec<()> = request(&req)?;
    Ok("Ok")
}

#[pg_extern]
fn mr_delete_node(
    ego: &'static str,
) -> core::result::Result<&'static str, Box<dyn std::error::Error + 'static>> {
    let rq = ((("src", "delete", ego), ), ());
    let req = rmp_serde::to_vec(&rq)?;
    let _res: Vec<()> = request(&req)?;
    Ok("Ok")
}

#[pg_extern]
fn mr_gravity_graph(
    ego: &'static str,
    focus: &'static str,
) -> core::result::Result<
    TableIterator<'static, (name!(node, String), name!(ego, String), name!(score, f64))>,
    Box<dyn std::error::Error + 'static>,
> {
    let rq = (((ego, "gravity", focus), ), ());
    let req = rmp_serde::to_vec(&rq)?;
    let res = request(&req);
    res.map(|v| TableIterator::new(v))
}

#[pg_extern]
fn mr_gravity_nodes(
    ego: &'static str,
    focus: &'static str,
) -> core::result::Result<
    TableIterator<'static, (name!(node, String), name!(weight, f64))>,
    Box<dyn std::error::Error + 'static>,
> {
    let rq = (((ego, "gravity_nodes", focus), ), ());
    let req = rmp_serde::to_vec(&rq)?;
    let res = request(&req);
    res.map(|v| TableIterator::new(v))
}

#[pg_extern]
fn mr_for_beacons_global() -> core::result::Result<
    TableIterator<'static, (name!(ego, String), name!(dest, String), name!(score, f64))>,
    Box<dyn std::error::Error + 'static>,
> {
    let rq = ("for_beacons_global", ());
    let req = rmp_serde::to_vec(&rq)?;
    let res = request(&req);
    res.map(|v| TableIterator::new(v))
}

#[pg_extern]
fn mr_nodes() -> core::result::Result<
    TableIterator<'static, (name!(id, String), )>,
    Box<dyn std::error::Error + 'static>,
> {
    let rq = ("nodes", ());
    let req = rmp_serde::to_vec(&rq)?;
    let res = request::<String>(&req)
        .map(|v| v.into_iter().map(|s| (s,))); // wrap to single-element tuple
    res.map(|v| TableIterator::new(v))
}

#[pg_extern]
fn mr_edges() -> core::result::Result<
    TableIterator<'static, (name!(source, String), name!(target, String), name!(weight, f64))>,
    Box<dyn std::error::Error + 'static>,
> {
    let rq = ("edges", ());
    let req = rmp_serde::to_vec(&rq)?;
    let res = request(&req);
    res.map(|v| TableIterator::new(v))
}

// connected nodes
#[pg_extern]
fn mr_connected(ego: &'static str) -> core::result::Result<
    TableIterator<'static, (name!(src, String), name!(dest, String))>,
    Box<dyn std::error::Error + 'static>,
> {
    let rq = (((ego, "connected"), ), ());
    let req = rmp_serde::to_vec(&rq)?;
    let res = request(&req);
    res.map(|v| TableIterator::new(v))
}