#![cfg(target_os="linux")]

use std::{fs::{self,File,OpenOptions}, io::{Read,Write}, os::{fd::AsRawFd,unix::fs::{DirBuilderExt,FileExt,MetadataExt,OpenOptionsExt,PermissionsExt}}, sync::{Arc,atomic::{AtomicBool,Ordering}}, thread::{self,JoinHandle}, time::{Duration,Instant}};
use rtdb_filesystem_observer::{Config,driver};

struct Mounted { directory:tempfile::TempDir,config:Config,stop:Arc<AtomicBool>,worker:Option<JoinHandle<std::io::Result<()>>> }
impl Mounted {
    fn new()->Self {
        Self::with_umask(None)
    }
    fn with_umask(mask:Option<u32>)->Self {
        let directory=tempfile::tempdir().unwrap();
        let path=directory.path().canonicalize().unwrap();
        let config=Config{namespace:"observer-mounted-regression".into(),storage_placement:"test-device".into(),backing:path.join("backing"),mount:path.join("mount"),
            ready:path.join("ready.json"),final_receipt:path.join("final.json"),expected_binary_sha256:driver::binary_digest().unwrap(),max_inodes:4096,max_handles:128,max_io_bytes:65536};
        fs::create_dir(&config.backing).unwrap();fs::create_dir(&config.mount).unwrap();
        let stop=Arc::new(AtomicBool::new(false));let input=config.clone();let token=stop.clone();
        let worker=thread::spawn(move||{
            if let Some(mask)=mask {
                assert_eq!(unsafe{libc::unshare(libc::CLONE_FS)},0);
                unsafe{libc::umask(mask);}
            }
            driver::run(input,token)
        });
        let owner=Self{directory,config,stop,worker:Some(worker)};
        let deadline=Instant::now()+Duration::from_secs(20);
        while !owner.config.ready.exists() {assert!(!owner.worker.as_ref().unwrap().is_finished(),"mount failed");assert!(Instant::now()<deadline,"mount did not initialize");thread::sleep(Duration::from_millis(10));}
        owner
    }
    fn close(mut self)->serde_json::Value {
        self.stop.store(true,Ordering::Release);
        self.worker.take().unwrap().join().unwrap().unwrap();
        let result=serde_json::from_slice(&fs::read(&self.config.final_receipt).unwrap()).unwrap();
        assert!(!fs::read_to_string("/proc/self/mountinfo").unwrap().lines().any(|line|line.split_whitespace().nth(4)==self.config.mount.to_str()));
        result
    }
}
impl Drop for Mounted {
    fn drop(&mut self) { self.stop.store(true,Ordering::Release);if let Some(worker)=self.worker.take(){let _=worker.join();}let _=&self.directory; }
}
struct Mapping(*mut libc::c_void,usize);
impl Drop for Mapping {fn drop(&mut self){unsafe{libc::munmap(self.0,self.1);}}}
struct Umask(libc::mode_t);
impl Drop for Umask {fn drop(&mut self){unsafe{libc::umask(self.0);}}}

#[test]
#[ignore="requires an explicitly provisioned Linux /dev/fuse worker with mount and allow_other privileges"]
fn mmap_hardlink_rename_unlink_and_fsync_cross_actual_observer() {
    let owner=Mounted::new();
    let root=&owner.config.mount;
    fs::create_dir(root.join("tables")).unwrap();
    let path=root.join("tables/data.arrow");
    {let mut file=File::create(&path).unwrap();file.write_all(&vec![73;8192]).unwrap();file.sync_all().unwrap();}
    fs::hard_link(&path,root.join("tables/link.arrow")).unwrap();
    assert_eq!(fs::metadata(&path).unwrap().ino(),fs::metadata(root.join("tables/link.arrow")).unwrap().ino());
    fs::rename(root.join("tables/link.arrow"),root.join("tables/final.arrow")).unwrap();
    let open=OpenOptions::new().read(true).write(true).open(&path).unwrap();
    assert_eq!(unsafe{libc::posix_fadvise(open.as_raw_fd(),0,8192,libc::POSIX_FADV_DONTNEED)},0);
    let ptr=unsafe{libc::mmap(std::ptr::null_mut(),8192,libc::PROT_READ,libc::MAP_SHARED,open.as_raw_fd(),0)};
    assert_ne!(ptr,libc::MAP_FAILED);let mapping=Mapping(ptr,8192);
    assert_eq!(unsafe{std::ptr::read_volatile(mapping.0.cast::<u8>().add(4096))},73);
    fs::remove_file(&path).unwrap();fs::remove_file(root.join("tables/final.arrow")).unwrap();
    open.write_at(&[91],0).unwrap();open.sync_all().unwrap();
    let mut byte=[0];open.read_at(&mut byte,0).unwrap();assert_eq!(byte,[91]);
    drop(mapping);drop(open);
    for index in 0..500 {File::create(root.join("tables").join(format!("{index:04}"))).unwrap();}
    assert_eq!(fs::read_dir(root.join("tables")).unwrap().count(),500);
    File::open(root.join("tables")).unwrap().sync_all().unwrap();
    let receipt=owner.close();
    assert_eq!(receipt["joined"],true);assert_eq!(receipt["unmounted"],true);
    assert!(receipt["successful_write_bytes"].as_u64().unwrap()>=8193);
    assert!(receipt["successful_read_bytes"].as_u64().unwrap()>=8192);
    assert!(receipt["successful_read_requests"].as_u64().unwrap()>0);
    assert!(receipt["successful_metadata_requests"].as_u64().unwrap()>0);
}

#[test]
#[ignore="requires an explicitly provisioned Linux /dev/fuse worker with mount and allow_other privileges"]
fn replacing_open_destination_keeps_old_descriptor_and_collects_handles() {
    let owner=Mounted::new();let root=&owner.config.mount;
    fs::write(root.join("old"),b"old").unwrap();fs::write(root.join("new"),b"new").unwrap();
    let mut opened=File::open(root.join("old")).unwrap();
    fs::rename(root.join("new"),root.join("old")).unwrap();
    let mut old=Vec::new();opened.read_to_end(&mut old).unwrap();assert_eq!(old,b"old");
    assert_eq!(fs::read(root.join("old")).unwrap(),b"new");drop(opened);
    for _ in 0..1024 {drop(File::open(root.join("old")).unwrap());}
    assert_eq!(owner.close()["rejected_requests"],0);
}

#[test]
#[ignore="requires an explicitly provisioned Linux /dev/fuse worker with mount and allow_other privileges"]
fn cancelled_listener_initialization_still_unmounts_and_joins_event_source() {
    let owner=Mounted::new();
    let ready:serde_json::Value=serde_json::from_slice(&fs::read(&owner.config.ready).unwrap()).unwrap();
    let mut socket=std::net::TcpStream::connect(("127.0.0.1",ready["control_port"].as_u64().unwrap() as u16)).unwrap();
    socket.write_all(b"GET /").unwrap();
    let start=Instant::now();let receipt=owner.close();
    assert!(start.elapsed()<Duration::from_secs(10));
    assert_eq!(receipt["joined"],true);assert_eq!(receipt["unmounted"],true);drop(socket);
}

#[test]
#[ignore="requires an explicitly provisioned Linux /dev/fuse worker with mount, allow_other and unshare(CLONE_FS) privileges"]
fn daemon_umask_does_not_override_client_mode_or_directory_setgid() {
    let owner=Mounted::with_umask(Some(0o077));
    assert_eq!(unsafe{libc::unshare(libc::CLONE_FS)},0);
    let _umask=Umask(unsafe{libc::umask(0)});
    let root=&owner.config.mount;
    let file=OpenOptions::new().write(true).create_new(true).mode(0o666).open(root.join("file")).unwrap();
    assert_eq!(file.metadata().unwrap().mode()&0o777,0o666);drop(file);
    fs::DirBuilder::new().mode(0o777).create(root.join("directory")).unwrap();
    assert_eq!(fs::metadata(root.join("directory")).unwrap().mode()&0o777,0o777);
    fs::set_permissions(root.join("directory"),fs::Permissions::from_mode(0o2777)).unwrap();
    fs::DirBuilder::new().mode(0o777).create(root.join("directory/child")).unwrap();
    let child=fs::metadata(root.join("directory/child")).unwrap();
    assert_eq!(child.mode()&0o2777,0o2777);
    assert_eq!(child.gid(),fs::metadata(root.join("directory")).unwrap().gid());
    owner.close();
}

#[test]
#[ignore="requires an explicitly provisioned Linux /dev/fuse worker with mount and allow_other privileges"]
fn append_open_writable_mapping_preserves_position_and_ordinary_append() {
    let owner=Mounted::new();let path=owner.config.mount.join("mapped");
    fs::write(&path,vec![17;8192]).unwrap();
    let mut file=OpenOptions::new().read(true).append(true).open(&path).unwrap();
    let ptr=unsafe{libc::mmap(std::ptr::null_mut(),8192,libc::PROT_READ|libc::PROT_WRITE,libc::MAP_SHARED,file.as_raw_fd(),0)};
    assert_ne!(ptr,libc::MAP_FAILED);let mapping=Mapping(ptr,8192);
    unsafe{std::ptr::write_volatile(mapping.0.cast::<u8>(),91);}
    assert_eq!(unsafe{libc::msync(mapping.0,8192,libc::MS_SYNC)},0);
    drop(mapping);drop(file);
    assert_eq!(fs::metadata(&path).unwrap().len(),8192);
    assert_eq!(fs::read(&path).unwrap()[0],91);
    file=OpenOptions::new().append(true).open(&path).unwrap();file.write_all(b"z").unwrap();file.sync_all().unwrap();drop(file);
    let bytes=fs::read(&path).unwrap();assert_eq!(bytes.len(),8193);assert_eq!(bytes[8192],b'z');
    assert!(OpenOptions::new().read(true).custom_flags(libc::O_DIRECT).open(&path).is_err());
    owner.close();
}
