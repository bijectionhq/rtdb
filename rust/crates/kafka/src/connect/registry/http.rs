use std::{collections::BTreeMap, fmt, future::Future, io, net::IpAddr, pin::Pin, sync::Arc, task::{Context, Poll}, time::Duration};

use bytes::Bytes;
use futures::{Stream, future::poll_fn};
use http_body_util::Full;
use hyper::{body::{Body, Incoming}, client::conn::http1, header::{HeaderMap, HeaderValue, AUTHORIZATION, CONNECTION, CONTENT_LENGTH, COOKIE, HOST, LOCATION}, Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use openssl::ssl::{Ssl, SslContext};
use rtdb_columnar::MemoryReservation;
use tokio::{io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf}, net::TcpStream, time::{Sleep, timeout}};
use tokio_openssl::SslStream;
use url::{Host, Position, Url};

const HEADER_LIMIT: usize = 384 * 1024;
mod resolve;

mod body;
pub(super) use body::{BodyInput, RawResponse};

#[derive(Clone)]
pub(crate) struct Client(Arc<Configuration>);

struct Configuration {
    // SSL_CTX cleanup uses native keys after its ex-data callbacks have run; its library-context owner must drop last.
    tls: SslContext,
    _owner: Arc<crate::tls::NativeTls>,
    verify_names: bool,
    proxy: Option<(String, u16)>,
    connect_timeout: Option<Duration>,
    read_timeout: Option<Duration>,
    charge: Arc<MemoryReservation>,
}

impl Client {
    pub(crate) fn new(tls: SslContext, owner: Arc<crate::tls::NativeTls>, verify_names: bool, options: &BTreeMap<String, String>, charge: Arc<MemoryReservation>) -> rtdb_types::Result<Self> {
        let mut configuration = Configuration { tls, _owner: owner, verify_names, proxy: None, connect_timeout: None, read_timeout: None, charge };
        let duration = |name| -> rtdb_types::Result<Option<Duration>> {
            let millis = options.get(name).map(|value| value.parse::<i32>().map_err(|_| super::super::error("invalid-plugin-config", "Registry timeout must be a nonnegative Java integer"))).transpose()?.unwrap_or(60_000);
            if millis < 0 { return Err(super::super::error("invalid-plugin-config", "Registry timeout must be nonnegative milliseconds")); }
            Ok((millis > 0).then(|| Duration::from_millis(millis as u64)))
        };
        let port = options.get("proxy.port").map(|port| port.parse::<i32>().map_err(|_| super::super::error("invalid-plugin-config", "Registry proxy port is invalid"))).transpose()?;
        let proxy = match (options.get("proxy.host"), port) {
            (Some(host), Some(port)) => {
                if host.is_empty() || port <= 0 { None } else {
                    let port = u16::try_from(port).map_err(|_| super::super::error("invalid-plugin-config", "Registry proxy port is invalid"))?;
                    let authority = match host.parse::<IpAddr>() { Ok(IpAddr::V6(address)) => format!("[{address}]"), _ => host.clone() };
                    let parsed = Url::parse(&format!("http://{authority}:{port}")).map_err(|_| super::super::error("invalid-plugin-config", "Registry proxy host is invalid"))?;
                    if parsed.host().is_none() || !parsed.username().is_empty() || parsed.password().is_some() || parsed.path() != "/" || parsed.query().is_some() || parsed.fragment().is_some() {
                        return Err(super::super::error("invalid-plugin-config", "Registry proxy host is invalid"));
                    }
                    Some((host_name(&parsed).map_err(|_| super::super::error("invalid-plugin-config", "Registry proxy host is invalid"))?, port))
                }
            }
            _ => None,
        };
        configuration.proxy = proxy;
        configuration.connect_timeout = duration("http.connect.timeout.ms")?;
        configuration.read_timeout = duration("http.read.timeout.ms")?;
        Ok(Self(Arc::new(configuration)))
    }

    pub(super) async fn request(&self, mut method: Method, mut url: Url, mut headers: HeaderMap, body: Vec<u8>) -> Result<Response, Error> {
        let mut body = Bytes::from(body);
        let mut proxy = self.0.proxy.clone();
        for _ in 0..20 {
            let response = self.request_once(method.clone(), &url, &headers, body.clone(), proxy.as_ref()).await?;
            if !matches!(response.status.as_u16(), 300..=303 | 305 | 307) { return Ok(response); }
            let Some(location) = &response.location else { return Ok(response); };
            let mut redirected = url.join(location).map_err(|_| Error("Invalid HTTP redirect"))?;
            if redirected.scheme() != url.scheme() { return Ok(response); }
            redirected.set_username("").map_err(|_| Error("Invalid HTTP redirect"))?;
            redirected.set_password(None).map_err(|_| Error("Invalid HTTP redirect"))?;
            if response.status.as_u16() == 305 {
                proxy = Some((host_name(&redirected)?, redirected.port_or_known_default().ok_or(Error("Invalid HTTP redirect proxy"))?));
            } else {
                if method == Method::POST && response.status.as_u16() != 307 {
                    method = Method::GET;
                    body = Bytes::new();
                    headers.clear();
                } else if url.host() != redirected.host() || url.port_or_known_default() != redirected.port_or_known_default() {
                    headers.remove(AUTHORIZATION);
                    headers.remove(COOKIE);
                    headers.remove("cookie2");
                }
                url = redirected;
            }
        }
        Err(Error("HTTP redirects exceeded their limit"))
    }

    pub(crate) async fn request_once(&self, method: Method, url: &Url, headers: &HeaderMap, body: Bytes, proxy: Option<&(String, u16)>) -> Result<Response, Error> {
        if !matches!(url.scheme(), "http" | "https") || !url.username().is_empty() || url.password().is_some() { return Err(Error("Invalid HTTP endpoint")); }
        let bound = headers.iter().try_fold(url.as_str().len().checked_add(body.len()).ok_or(Error("HTTP allocation overflow"))?, |size, (name, value)| size.checked_add(name.as_str().len()).and_then(|size| size.checked_add(value.len())).ok_or(Error("HTTP allocation overflow")))?;
        let bound = bound.checked_mul(8).and_then(|size| size.checked_add(16 * HEADER_LIMIT)).ok_or(Error("HTTP allocation overflow"))?;
        let charge = self.0.charge.budget().reserve(bound).map_err(|_| Error("HTTP memory limit exceeded"))?;
        let transport = self.connect(url, proxy).await?;
        let mut headers = headers.clone();
        let authority = &url[Position::BeforeHost..Position::AfterPort];
        headers.insert(HOST, HeaderValue::from_str(authority).map_err(|_| Error("Invalid HTTP authority"))?);
        headers.insert(CONNECTION, HeaderValue::from_static("close"));
        let target = if proxy.is_some() && url.scheme() == "http" { &url[..Position::AfterQuery] } else { &url[Position::BeforePath..Position::AfterQuery] };
        let mut request = Request::builder().method(method).uri(target).body(Full::new(body)).map_err(|_| Error("Invalid HTTP request"))?;
        *request.headers_mut() = headers;
        let transport = HttpTransport::new(transport);
        let (mut sender, connection) = http1::Builder::new().max_buf_size(HEADER_LIMIT).max_headers(HEADER_LIMIT / 32).allow_obsolete_multiline_headers_in_responses(true).handshake(TokioIo::new(transport)).await.map_err(|_| Error("HTTP connection initialization failed"))?;
        let mut connection = Some(Box::pin(connection));
        let mut response = Box::pin(sender.send_request(request));
        let response = poll_fn(|cx| {
            if let Poll::Ready(result) = response.as_mut().poll(cx) { return Poll::Ready(result.map_err(|_| Error("HTTP request failed"))); }
            if let Err(error) = drive(&mut connection, cx) { return Poll::Ready(Err(error)); }
            response.as_mut().poll(cx).map(|result| result.map_err(|_| Error("HTTP request failed")))
        }).await?;
        let (parts, body) = response.into_parts();
        Ok(Response { body, connection, status: parts.status, content_length: parts.headers.get(CONTENT_LENGTH).and_then(|value| value.to_str().ok()).and_then(|value| value.parse().ok()), location: parts.headers.get(LOCATION).map(|value| value.as_bytes().iter().map(|byte| char::from(*byte)).collect()), _configuration: Arc::clone(&self.0), _charge: charge })
    }

    async fn connect(&self, url: &Url, proxy: Option<&(String, u16)>) -> Result<Transport, Error> {
        let host = host_name(url)?;
        let port = url.port_or_known_default().ok_or(Error("Invalid HTTP port"))?;
        let (peer, peer_port) = proxy.map(|(host, port)| (host.as_str(), *port)).unwrap_or((&host, port));
        let address = resolve::resolve(peer, peer_port, self.0.charge.budget()).await?;
        let connect = TcpStream::connect(address);
        let socket = match self.0.connect_timeout {
            Some(duration) => timeout(duration, connect).await.map_err(|_| Error("HTTP connection timed out"))?,
            None => connect.await,
        }.map_err(|_| Error("HTTP connection failed"))?;
        let mut socket = ReadTimeout { socket, duration: self.0.read_timeout, deadline: None };
        if url.scheme() != "https" { return Ok(Transport::Plain(socket)); }
        if proxy.is_some() { tunnel(&mut socket, &host, port).await?; }
        let mut ssl = Ssl::new(&self.0.tls).map_err(|_| Error("TLS initialization failed"))?;
        let address = host.parse::<IpAddr>().ok();
        if address.is_none() { ssl.set_hostname(&host).map_err(|_| Error("Invalid TLS server name"))?; }
        if self.0.verify_names {
            match address {
                Some(address) => ssl.param_mut().set_ip(address).map_err(|_| Error("Invalid TLS server address"))?,
                None => ssl.param_mut().set_host(&host).map_err(|_| Error("Invalid TLS server name"))?,
            }
        }
        let mut socket = SslStream::new(ssl, socket).map_err(|_| Error("TLS initialization failed"))?;
        Pin::new(&mut socket).connect().await.map_err(|_| Error("TLS handshake failed"))?;
        Ok(Transport::Tls(socket))
    }
}

fn host_name(url: &Url) -> Result<String, Error> {
    match url.host() { Some(Host::Domain(host)) => Ok(host.to_owned()), Some(Host::Ipv4(host)) => Ok(host.to_string()), Some(Host::Ipv6(host)) => Ok(host.to_string()), None => Err(Error("HTTP host is absent")) }
}

async fn tunnel(socket: &mut ReadTimeout, host: &str, port: u16) -> Result<(), Error> {
    let authority = if host.contains(':') { format!("[{host}]:{port}") } else { format!("{host}:{port}") };
    socket.write_all(format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n\r\n").as_bytes()).await.map_err(|_| Error("HTTP proxy connection failed"))?;
    let mut parsed = 0;
    loop {
        let mut headers = HeaderNormalizer::new();
        loop {
            let byte = socket.read_u8().await.map_err(|_| Error("HTTP proxy response failed"))?;
            if headers.push(byte)? { break; }
            parsed += 1;
            if parsed == 65_536 { tokio::task::yield_now().await; parsed = 0; }
        }
        if headers.informational() { continue; }
        if headers.status != Some(200) { return Err(Error("HTTP proxy rejected CONNECT")); }
        return Ok(());
    }
}

fn trim_end(mut value: &[u8]) -> &[u8] {
    while value.last().is_some_and(|byte| *byte <= b' ') { value = &value[..value.len() - 1]; }
    value
}

#[derive(Clone, Copy)]
enum HeaderState { First, Record, AfterCr, AfterLf, AfterCrLf, AfterBlankCr }

struct HeaderNormalizer {
    record: Vec<u8>,
    output: Vec<u8>,
    size: usize,
    records: usize,
    key_end: Option<usize>,
    in_key: bool,
    state: HeaderState,
    status: Option<u16>,
}

impl HeaderNormalizer {
    fn new() -> Self {
        Self { record: Vec::new(), output: Vec::new(), size: 0, records: 0, key_end: None, in_key: false, state: HeaderState::First, status: None }
    }

    // Temurin 25 MessageHeader.mergeHeader admits each unfolded record before trimming it.
    fn push(&mut self, byte: u8) -> Result<bool, Error> {
        match self.state {
            HeaderState::First => {
                if matches!(byte, b'\r' | b'\n') { return Err(Error("HTTP response status is absent")); }
                self.start(byte)?;
            }
            HeaderState::Record => match byte {
                b'\r' => self.state = HeaderState::AfterCr,
                b'\n' => self.state = HeaderState::AfterLf,
                b':' => {
                    if self.in_key { self.key_end = Some(self.record.len()); }
                    self.in_key = false;
                    self.append(byte)?;
                }
                b' ' | b'\t' => { self.in_key = false; self.append(b' ')?; }
                _ => self.append(byte)?,
            },
            HeaderState::AfterCr if byte == b'\n' => self.state = HeaderState::AfterCrLf,
            HeaderState::AfterCrLf if byte == b'\r' => self.state = HeaderState::AfterBlankCr,
            HeaderState::AfterCr | HeaderState::AfterLf | HeaderState::AfterCrLf | HeaderState::AfterBlankCr => {
                if matches!(byte, b'\r' | b'\n') {
                    self.finish()?;
                    self.output.extend_from_slice(b"\r\n");
                    return Ok(true);
                }
                if byte > b' ' { self.finish()?; self.start(byte)?; }
                else { self.append(b' ')?; self.state = HeaderState::Record; }
            }
        }
        Ok(false)
    }

    fn start(&mut self, byte: u8) -> Result<(), Error> {
        self.key_end = None;
        self.in_key = byte > b' ';
        self.state = HeaderState::Record;
        self.append(byte)
    }

    fn append(&mut self, byte: u8) -> Result<(), Error> {
        if self.size + 32 + self.record.len() >= HEADER_LIMIT { return Err(Error("HTTP headers exceed their byte limit")); }
        self.record.push(byte);
        Ok(())
    }

    fn finish(&mut self) -> Result<(), Error> {
        let record = trim_end(&self.record);
        let (name, mut value) = match self.key_end {
            Some(colon) => (&record[..colon], &record[(colon + 1).min(record.len())..]),
            None => (&[][..], record),
        };
        if self.key_end.is_some() { while value.first().is_some_and(|byte| *byte <= b' ') { value = &value[1..]; } }
        self.size += 32 + name.len() + value.len();
        if self.size > HEADER_LIMIT { return Err(Error("HTTP headers exceed their byte limit")); }
        if self.records == 0 {
            let mut fields = record.split(|byte| *byte == b' ').filter(|field| !field.is_empty());
            if !matches!(fields.next(), Some(b"HTTP/1.0" | b"HTTP/1.1")) { return Err(Error("Invalid HTTP response version")); }
            self.status = fields.next().and_then(|code| std::str::from_utf8(code).ok()).and_then(|code| code.parse().ok());
            if self.status == Some(101) { return Err(Error("Unexpected HTTP protocol upgrade")); }
            self.output.extend_from_slice(record);
        } else {
            self.output.extend_from_slice(name);
            if self.key_end.is_some() { self.output.push(b':'); }
            self.output.extend_from_slice(value);
        }
        self.output.extend_from_slice(b"\r\n");
        self.record.clear();
        self.records += 1;
        Ok(())
    }

    fn informational(&self) -> bool { self.status.is_some_and(|status| (100..200).contains(&status)) }
}

type Connection = http1::Connection<TokioIo<HttpTransport>, Full<Bytes>>;

fn drive(connection: &mut Option<Pin<Box<Connection>>>, cx: &mut Context<'_>) -> Result<(), Error> {
    if let Some(driver) = connection {
        if let Poll::Ready(result) = driver.as_mut().poll(cx) {
            *connection = None;
            result.map_err(|_| Error("HTTP connection failed"))?;
        }
    }
    Ok(())
}

pub(crate) struct Response {
    body: Incoming,
    connection: Option<Pin<Box<Connection>>>,
    status: StatusCode,
    content_length: Option<u64>,
    location: Option<String>,
    _configuration: Arc<Configuration>,
    _charge: Arc<MemoryReservation>,
}

impl Response {
    pub(crate) fn status(&self) -> StatusCode { self.status }
    pub(crate) fn content_length(&self) -> Option<u64> { self.content_length }
    pub(super) fn bytes_stream(self) -> Self { self }
}

impl Stream for Response {
    type Item = Result<Bytes, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            match Pin::new(&mut this.body).poll_frame(cx) {
                Poll::Ready(Some(Ok(frame))) => if let Ok(bytes) = frame.into_data() { return Poll::Ready(Some(Ok(bytes))); },
                Poll::Ready(Some(Err(_))) => return Poll::Ready(Some(Err(Error("HTTP response failed")))),
                Poll::Ready(None) => { this.connection = None; return Poll::Ready(None); }
                Poll::Pending => {
                    if let Err(error) = drive(&mut this.connection, cx) { return Poll::Ready(Some(Err(error))); }
                    return match Pin::new(&mut this.body).poll_frame(cx) {
                        Poll::Ready(Some(Ok(frame))) => match frame.into_data() { Ok(bytes) => Poll::Ready(Some(Ok(bytes))), Err(_) => { cx.waker().wake_by_ref(); Poll::Pending } },
                        Poll::Ready(Some(Err(_))) => Poll::Ready(Some(Err(Error("HTTP response failed")))),
                        Poll::Ready(None) => { this.connection = None; Poll::Ready(None) }
                        Poll::Pending => Poll::Pending,
                    };
                }
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct Error(&'static str);
impl Error { pub(super) fn without_url(self) -> Self { self } }
impl fmt::Display for Error { fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result { formatter.write_str(self.0) } }
impl std::error::Error for Error {}

struct ReadTimeout { socket: TcpStream, duration: Option<Duration>, deadline: Option<Pin<Box<Sleep>>> }

impl AsyncRead for ReadTimeout {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buffer: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match Pin::new(&mut this.socket).poll_read(cx, buffer) {
            Poll::Ready(result) => { this.deadline = None; Poll::Ready(result) }
            Poll::Pending => {
                if let Some(duration) = this.duration {
                    let deadline = this.deadline.get_or_insert_with(|| Box::pin(tokio::time::sleep(duration)));
                    if deadline.as_mut().poll(cx).is_ready() { return Poll::Ready(Err(io::Error::new(io::ErrorKind::TimedOut, "HTTP read timed out"))); }
                }
                Poll::Pending
            }
        }
    }
}

impl AsyncWrite for ReadTimeout {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> { Pin::new(&mut self.get_mut().socket).poll_write(cx, bytes) }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> { Pin::new(&mut self.get_mut().socket).poll_flush(cx) }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> { Pin::new(&mut self.get_mut().socket).poll_shutdown(cx) }
}

enum Transport { Plain(ReadTimeout), Tls(SslStream<ReadTimeout>) }
impl AsyncRead for Transport {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buffer: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() { Self::Plain(socket) => Pin::new(socket).poll_read(cx, buffer), Self::Tls(socket) => Pin::new(socket).poll_read(cx, buffer) }
    }
}
impl AsyncWrite for Transport {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> { match self.get_mut() { Self::Plain(socket) => Pin::new(socket).poll_write(cx, bytes), Self::Tls(socket) => Pin::new(socket).poll_write(cx, bytes) } }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> { match self.get_mut() { Self::Plain(socket) => Pin::new(socket).poll_flush(cx), Self::Tls(socket) => Pin::new(socket).poll_flush(cx) } }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> { match self.get_mut() { Self::Plain(socket) => Pin::new(socket).poll_shutdown(cx), Self::Tls(socket) => Pin::new(socket).poll_shutdown(cx) } }
}

enum HttpInput {
    Headers(HeaderNormalizer),
    Normalized { bytes: Vec<u8>, position: usize, informational: bool },
    Body,
}

struct HttpTransport {
    transport: Transport,
    state: HttpInput,
    input: [u8; 8192],
    start: usize,
    end: usize,
}

impl HttpTransport {
    fn new(transport: Transport) -> Self {
        Self { transport, state: HttpInput::Headers(HeaderNormalizer::new()), input: [0; 8192], start: 0, end: 0 }
    }
}

impl AsyncRead for HttpTransport {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buffer: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buffer.remaining() == 0 { return Poll::Ready(Ok(())); }
        let mut parsed = 0;
        loop {
            match &mut this.state {
                HttpInput::Normalized { bytes, position, informational } => {
                    let length = buffer.remaining().min(bytes.len() - *position);
                    buffer.put_slice(&bytes[*position..*position + length]);
                    *position += length;
                    if *position == bytes.len() { this.state = if *informational { HttpInput::Headers(HeaderNormalizer::new()) } else { HttpInput::Body }; }
                    return Poll::Ready(Ok(()));
                }
                HttpInput::Body => {
                    if this.start == this.end { return Pin::new(&mut this.transport).poll_read(cx, buffer); }
                    let length = buffer.remaining().min(this.end - this.start);
                    buffer.put_slice(&this.input[this.start..this.start + length]);
                    this.start += length;
                    return Poll::Ready(Ok(()));
                }
                HttpInput::Headers(headers) => {
                    if parsed == 65_536 { cx.waker().wake_by_ref(); return Poll::Pending; }
                    if this.start == this.end {
                        let mut input = ReadBuf::new(&mut this.input);
                        match Pin::new(&mut this.transport).poll_read(cx, &mut input) {
                            Poll::Ready(Ok(())) => {
                                this.start = 0; this.end = input.filled().len();
                                if this.end == 0 { return Poll::Ready(Err(io::Error::new(io::ErrorKind::UnexpectedEof, "Incomplete HTTP headers"))); }
                            }
                            result => return result,
                        }
                    }
                    let complete = headers.push(this.input[this.start]).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
                    this.start += 1; parsed += 1;
                    if complete {
                        let informational = headers.informational();
                        let bytes = std::mem::take(&mut headers.output);
                        this.state = HttpInput::Normalized { bytes, position: 0, informational };
                    }
                }
            }
        }
    }
}
impl AsyncWrite for HttpTransport {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, bytes: &[u8]) -> Poll<io::Result<usize>> { Pin::new(&mut self.get_mut().transport).poll_write(cx, bytes) }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> { Pin::new(&mut self.get_mut().transport).poll_flush(cx) }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> { Pin::new(&mut self.get_mut().transport).poll_shutdown(cx) }
}

#[cfg(test)]
mod tests;
