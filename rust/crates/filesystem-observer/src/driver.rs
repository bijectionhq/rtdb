use std::{ffi::CString, fs::{self, File, OpenOptions}, io::{self, Read, Write}, net::{TcpListener, TcpStream}, os::unix::{ffi::OsStrExt, fs::MetadataExt}, path::Path, sync::{Arc, atomic::{AtomicBool, Ordering}}, thread, time::{Duration, Instant}};
use fuser::{MountOption, Session};
use serde_json::{Value,json};
use sha2::{Digest,Sha256};
use crate::{BOUNDARY,Config,Measurements,filesystem::Backing};

/// Returns the running Linux executable's lowercase SHA-256, reading `/proc/self/exe` in 64 KiB chunks.
/// Procfs/open/read failures return `io::Error`; no executable path supplied by the caller is trusted.
pub fn binary_digest()->io::Result<String> {
    let mut file=File::open("/proc/self/exe")?;
    let mut buffer=[0u8;65536]; let mut hash=Sha256::new();
    loop { let n=file.read(&mut buffer)?; if n==0 {break;} hash.update(&buffer[..n]); }
    Ok(format!("{:x}",hash.finalize()))
}
/// Returns the executable digest, embedded clean-source/target metadata, and fixed measurement boundary.
/// `observer_features` describes this crate, which declares no optional features; it does not describe unified dependency features.
/// Errors are those of `binary_digest`.
pub fn identity()->io::Result<Value> {
    Ok(json!({"schema_version":1,"boundary":BOUNDARY,"observer_artifact_sha256":binary_digest()?,
        "source_revision":env!("RTDB_OBSERVER_REVISION"),"source_dirty":env!("RTDB_OBSERVER_DIRTY")!="false",
        "target":env!("RTDB_OBSERVER_TARGET"),"fuser":"0.16.0","fuse_abi":"7.31","passthrough":false,
        "kernel_cache":"normal_page_cache","observer_features":[]}))
}
fn write_json(path:&Path,value:&Value)->io::Result<()> {
    let temporary=path.with_extension("json.tmp");
    let mut file=OpenOptions::new().write(true).create_new(true).open(&temporary)?;
    serde_json::to_writer(&mut file,value)?; file.write_all(b"\n")?; file.sync_all()?;
    fs::rename(temporary,path)
}
fn clean_dir(path:&Path)->io::Result<()> {
    if !path.is_absolute() || fs::symlink_metadata(path)?.file_type().is_symlink() || fs::canonicalize(path)?!=path
        || !fs::metadata(path)?.is_dir() || fs::read_dir(path)?.next().is_some() { return Err(io::Error::other("observer directories must be canonical, empty, disjoint owned directories")); }
    Ok(())
}
fn bounded_text(path:&str,limit:u64)->io::Result<String> {
    let mut text=String::new();File::open(path)?.take(limit+1).read_to_string(&mut text)?;
    if text.len() as u64>limit {return Err(io::Error::other("kernel observation exceeds byte bound"));}Ok(text)
}
fn mount_line(mount:&Path)->io::Result<String> {
    let text=bounded_text("/proc/self/mountinfo",1024*1024)?;
    let target=mount.to_str().ok_or_else(||io::Error::other("mount path is not Unicode"))?;
    if target.bytes().any(|v|v.is_ascii_whitespace() || v==b'\\') {return Err(io::Error::other("mount path cannot contain mountinfo escapes"));}
    text.lines().find(|line|line.split_whitespace().nth(4)==Some(target) && line.contains(" - fuse.rtdb-observer "))
        .map(str::to_owned).ok_or_else(||io::Error::other("owned FUSE mount missing from host mountinfo"))
}
fn snapshot(config:&Config,identity:&Value,measurements:&Measurements,proof:&Value)->io::Result<Value> {
    let mut value=identity.clone();
    let object=value.as_object_mut().unwrap();
    object.extend(serde_json::to_value(measurements.snapshot())?.as_object().unwrap().clone());
    object.insert("namespace".into(),json!(config.namespace));
    object.insert("storage_placement".into(),json!(config.storage_placement));
    object.insert("physical_placement".into(),json!({"backing_parent":config.backing.parent(),"backing_device":fs::metadata(&config.backing)?.dev(),
        "host_boot_id":bounded_text("/proc/sys/kernel/random/boot_id",64)?.trim()}));
    object.insert("mount".into(),proof.clone());
    let limits=format!("v1:{}:{}:{}:normal_page_cache:no_passthrough",config.max_inodes,config.max_handles,config.max_io_bytes);
    object.insert("measurement_configuration_sha256".into(),json!(format!("{:x}",Sha256::digest(limits.as_bytes()))));
    Ok(value)
}
fn control(stream:&mut TcpStream,stop:&AtomicBool,value:&Value)->io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_millis(200)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    let deadline=Instant::now()+Duration::from_secs(3);
    let mut header=Vec::with_capacity(4096);
    while !header.ends_with(b"\r\n\r\n") {
        if stop.load(Ordering::Acquire) || Instant::now()>=deadline || header.len()==4096 { return Err(io::Error::other("control request exceeded lifetime or header bound")); }
        let mut byte=[0u8;1];
        match stream.read(&mut byte) { Ok(0)=>return Err(io::Error::other("incomplete control request")),Ok(_)=>header.push(byte[0]),Err(e) if matches!(e.kind(),io::ErrorKind::WouldBlock|io::ErrorKind::TimedOut|io::ErrorKind::Interrupted)=>continue,Err(e)=>return Err(e) }
    }
    let (status,body)=if header.starts_with(b"GET /__rtdb_observer__ HTTP/1.") {("200 OK",serde_json::to_vec(value)?)}
        else if header.starts_with(b"POST /__rtdb_stop__ HTTP/1.") { stop.store(true,Ordering::Release); ("200 OK",b"{}".to_vec()) }
        else {("404 Not Found",b"{}".to_vec())};
    write!(stream,"HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len())?;
    stream.write_all(&body)
}

/// Owns the FUSE event thread until unmount and join, including failed startup.
/// Inputs are empty caller-owned directories and a cancellation flag.
/// The caller must remove all container bind mounts before cancellation and retain directories until return.
/// Readiness contains the actual mount device and binary identity.
/// Final receipt is written only after the event thread and its file descriptors are joined and dropped.
/// I/O, mount, listener, receipt, and event-thread failures return errors; an uninterruptible backing syscall can delay joining.
pub fn run(config:Config,stop:Arc<AtomicBool>)->io::Result<()> {
    config.validate()?; clean_dir(&config.backing)?; clean_dir(&config.mount)?;
    if config.backing.starts_with(&config.mount) || config.mount.starts_with(&config.backing) {return Err(io::Error::other("backing and mount overlap"));}
    for receipt in [&config.ready,&config.final_receipt] {
        let parent=receipt.parent().ok_or_else(||io::Error::other("receipt parent is absent"))?;
        if !receipt.is_absolute() || fs::canonicalize(parent)?!=parent || receipt.starts_with(&config.backing) || receipt.starts_with(&config.mount)
            || receipt.exists() || receipt.with_extension("json.tmp").exists() || receipt.file_name().is_none() {
            return Err(io::Error::other("receipt paths must be new canonical paths outside the object roots"));
        }
    }
    if config.ready==config.final_receipt || config.ready.with_extension("json.tmp")==config.final_receipt.with_extension("json.tmp") {
        return Err(io::Error::other("receipt paths alias one another"));
    }
    let identity=identity()?;
    if identity["observer_artifact_sha256"]!=config.expected_binary_sha256 {return Err(io::Error::other("observer executable digest differs from selection"));}
    let listener=TcpListener::bind((std::net::Ipv4Addr::LOCALHOST,0))?;
    listener.set_nonblocking(true)?;
    let port=listener.local_addr()?.port();
    let measurements=Measurements::default();
    let filesystem=Backing::new(&config.backing,&config,measurements.clone())?;
    let options=[MountOption::FSName(config.namespace.clone()),MountOption::Subtype("rtdb-observer".into()),MountOption::AllowOther,
        MountOption::DefaultPermissions,MountOption::RW,MountOption::NoSuid,MountOption::NoDev,
        MountOption::CUSTOM(format!("max_read={}",config.max_io_bytes))];
    let mut session=Session::new(filesystem,&config.mount,&options)?;
    let mut unmounter=session.unmount_callable();
    let worker=thread::Builder::new().name("filesystem-observer".into()).spawn(move||session.run())?;
    let result=(|| {
        let device=fs::metadata(&config.mount)?.dev();
        let proof=json!({"host_mountinfo":mount_line(&config.mount)?,"device":device,"mountpoint":config.mount,"source":config.namespace,"filesystem":"fuse.rtdb-observer"});
        let mut ready=snapshot(&config,&identity,&measurements,&proof)?;
        ready["control_port"]=json!(port);
        write_json(&config.ready,&ready)?;
        while !stop.load(Ordering::Acquire) {
            if worker.is_finished() {return Err(io::Error::other("FUSE event source stopped before shutdown"));}
            match listener.accept() {
                Ok((mut stream,_))=>{ let value=snapshot(&config,&identity,&measurements,&proof)?; if control(&mut stream,&stop,&value).is_err() { measurements.rejected(); } },
                Err(e) if e.kind()==io::ErrorKind::WouldBlock=>thread::sleep(Duration::from_millis(10)),
                Err(e) if e.kind()==io::ErrorKind::Interrupted=>continue,
                Err(e)=>return Err(e),
            }
        }
        Ok(proof)
    })();
    drop(listener);
    let path=CString::new(config.mount.as_os_str().as_bytes()).map_err(|_|io::Error::other("invalid mount path"))?;
    let unmount=if unsafe{libc::umount2(path.as_ptr(),0)}<0 {Err(io::Error::last_os_error())} else {Ok(())};
    // fuser's Mount drop can detach after a failed normal unmount; joining still precedes every return.
    let fallback=unmounter.unmount();
    let joined=worker.join().map_err(|_|io::Error::other("FUSE event thread panicked")).and_then(|v|v);
    let unmounted=unmount.is_ok() && mount_line(&config.mount).is_err();
    let did_join=joined.is_ok();
    let proof=result.as_ref().ok().cloned().unwrap_or(Value::Null);
    let mut errors=Vec::new();
    for outcome in [result.map(|_|()),unmount,fallback,joined] {if let Err(error)=outcome {errors.push(error.to_string());}}
    if !unmounted {errors.push("normal unmount did not complete".into());}
    let mut final_value=snapshot(&config,&identity,&measurements,&proof)?;
    final_value["unmounted"]=json!(unmounted); final_value["joined"]=json!(did_join); final_value["errors"]=json!(errors);
    write_json(&config.final_receipt,&final_value)?;
    if errors.is_empty() {Ok(())} else {Err(io::Error::other(errors.join("; ")))}
}
