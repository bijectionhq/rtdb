use std::{collections::BTreeMap, fs::File, io::{Read,Write}, path::Path};
use serde::Deserialize;
type Error=Box<dyn std::error::Error+Send+Sync>;
fn read(path:&Path,limit:u64)->Result<Vec<u8>,Error> {
    let mut bytes=Vec::new(); File::open(path)?.take(limit+1).read_to_end(&mut bytes)?;
    if bytes.len() as u64>limit {return Err("observer input exceeds bound".into());} Ok(bytes)
}
fn output(value:&serde_json::Value)->Result<(),Error> {let mut out=std::io::stdout().lock();serde_json::to_writer(&mut out,value)?;out.write_all(b"\n")?;Ok(())}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Validation {configuration:std::path::PathBuf,environment:BTreeMap<String,String>}
#[cfg(target_os="linux")]
static STOP:std::sync::atomic::AtomicBool=std::sync::atomic::AtomicBool::new(false);
#[cfg(target_os="linux")]
extern "C" fn stop(_:i32) {STOP.store(true,std::sync::atomic::Ordering::Release);}
fn main()->Result<(),Error> {
    let mut args=std::env::args_os().skip(1);
    let command=args.next().ok_or("expected identity, validate-config, validate-workload, or run")?;
    if command=="validate-workload" {
        let path=args.next().ok_or("missing workload proof request")?;
        if args.next().is_some(){return Err("unexpected argument".into());}
        return output(&rtdb_filesystem_observer::validate_workload(&read(Path::new(&path),16*1024*1024)?)?);
    }
    if command=="validate-config" {
        let path=args.next().ok_or("missing validation request")?;
        if args.next().is_some(){return Err("unexpected argument".into());}
        let request:Validation=serde_json::from_slice(&read(Path::new(&path),2*1024*1024)?)?;
        let text=String::from_utf8(read(&request.configuration,1024*1024)?)?;
        return output(&rtdb_filesystem_observer::validate_configuration(&text,request.environment)?);
    }
    #[cfg(target_os="linux")]
    {
        if command=="identity" {if args.next().is_some(){return Err("unexpected argument".into());}return output(&rtdb_filesystem_observer::driver::identity()?);}
        if command!="run" {return Err("unknown observer command".into());}
        let path=args.next().ok_or("missing observer configuration")?;
        if args.next().is_some(){return Err("unexpected argument".into());}
        let config=serde_json::from_slice(&read(Path::new(&path),65536)?)?;
        let token=std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        unsafe {libc::signal(libc::SIGTERM,stop as libc::sighandler_t);libc::signal(libc::SIGINT,stop as libc::sighandler_t);}
        let finished=std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let signal_token=token.clone(); let done=finished.clone();
        let signals=std::thread::spawn(move||while !done.load(std::sync::atomic::Ordering::Acquire) {
            if STOP.load(std::sync::atomic::Ordering::Acquire) {signal_token.store(true,std::sync::atomic::Ordering::Release);break;}
            std::thread::sleep(std::time::Duration::from_millis(10));
        });
        let result=rtdb_filesystem_observer::driver::run(config,token);
        finished.store(true,std::sync::atomic::Ordering::Release);
        let joined=signals.join().map_err(|_|"signal observer panicked");
        result?; joined?; Ok(())
    }
    #[cfg(not(target_os="linux"))]
    Err("filesystem observation requires Linux /dev/fuse and mount privileges".into())
}
