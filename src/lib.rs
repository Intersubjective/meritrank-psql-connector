use nng::*;
use pgrx::iter::TableIterator;
use pgrx::*;
use serde::de::Deserialize;

// pgx specific macros
pg_module_magic!();

const SERVICE_URL: &str = "tcp://127.0.0.1:10234";

fn request<T: for<'a> Deserialize<'a>>(
    req: &Vec<u8>,
) -> core::result::Result<Vec<T>, Box<dyn std::error::Error + 'static>> {
    let client = Socket::new(Protocol::Req0)?;
    client.dial(SERVICE_URL)?;
    client
        .send(Message::from(req.as_slice()))
        .map_err(|(_, err)| err)?;
    let msg: Message = client.recv()?;
    let slice: &[u8] = msg.as_slice();
    rmp_serde::from_slice(slice)
        .or_else(|_| {
            let err: String = rmp_serde::from_slice(slice)?;
            Err(Box::from(format!("Server error: {}", err)))
        })
}

#[pg_extern]
fn mr_service_url() -> &'static str {
    SERVICE_URL
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
) -> core::result::Result<
    &'static str,
    Box<dyn std::error::Error + 'static>,
> {
    let rq = ((("src", "delete", ego), ("dest", "delete", target)), ());
    let req = rmp_serde::to_vec(&rq)?;
    let _res: Vec<()> = request(&req)?;
    Ok("Ok")
}

#[pg_extern]
fn mr_delete_node(
    ego: &'static str,
) -> core::result::Result<
    &'static str,
    Box<dyn std::error::Error + 'static>,
> {
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
    let rq = (((ego, "gravity", focus),), ());
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
    let rq = (((ego, "gravity_nodes", focus),), ());
    let req = rmp_serde::to_vec(&rq)?;
    let res = request(&req);
    res.map(|v| TableIterator::new(v))
}
