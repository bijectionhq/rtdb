use std::{collections::BTreeMap, ffi::{CString, OsStr}, fs::{File, Metadata, OpenOptions}, io, os::{fd::{AsRawFd, FromRawFd, RawFd}, unix::{ffi::OsStrExt, fs::{FileExt, MetadataExt, OpenOptionsExt}}}, path::Path, time::{Duration, SystemTime, UNIX_EPOCH}};
use fuser::*;
use crate::{Config, Measurements};

const TTL: Duration=Duration::ZERO;
struct Inode { anchor: File, lookups: u64, opens: u64, parent: u64 }
struct Handle { file: File, ino: u64 }

pub(crate) struct Backing {
    inodes: BTreeMap<u64,Inode>,
    handles: BTreeMap<u64,Handle>,
    next_handle: u64,
    root_inode: u64,
    device: u64,
    max_inodes: usize,
    max_handles: usize,
    max_io: usize,
    pub measurements: Measurements,
}
fn error(code:i32)->io::Error { io::Error::from_raw_os_error(code) }
fn errno(e:io::Error)->i32 { e.raw_os_error().unwrap_or(libc::EIO) }
fn name(value:&OsStr)->io::Result<CString> {
    if value.is_empty() || value.as_bytes().len()>255 || value=="." || value==".." || value.as_bytes().contains(&b'/') { return Err(error(libc::EINVAL)); }
    CString::new(value.as_bytes()).map_err(|_|error(libc::EINVAL))
}
fn syscall(value:i32)->io::Result<()> { if value<0 { Err(io::Error::last_os_error()) } else { Ok(()) } }
fn openat(parent:RawFd,name:&CString,flags:i32,mode:u32)->io::Result<File> {
    if flags&libc::O_DIRECT!=0 {return Err(error(libc::EOPNOTSUPP));}
    let fd=unsafe { libc::openat(parent,name.as_ptr(),(flags&!libc::O_APPEND)|libc::O_CLOEXEC|libc::O_NOFOLLOW,mode) };
    if fd<0 { Err(io::Error::last_os_error()) } else { Ok(unsafe { File::from_raw_fd(fd) }) }
}
fn time(seconds:i64,nanos:i64)->SystemTime {
    if seconds>=0 { UNIX_EPOCH+Duration::new(seconds as u64,nanos as u32) }
    else { UNIX_EPOCH-Duration::from_secs(seconds.unsigned_abs())+Duration::from_nanos(nanos as u64) }
}
fn kind(metadata:&Metadata)->io::Result<FileType> {
    if metadata.is_dir() { Ok(FileType::Directory) } else if metadata.is_file() { Ok(FileType::RegularFile) } else { Err(error(libc::EOPNOTSUPP)) }
}
impl Backing {
    pub fn new(path:&Path,config:&Config,measurements:Measurements)->io::Result<Self> {
        config.validate()?;
        let anchor=OpenOptions::new().read(true).custom_flags(libc::O_PATH|libc::O_DIRECTORY|libc::O_NOFOLLOW).open(path)?;
        let metadata=anchor.metadata()?;
        let root_inode=metadata.ino();
        let device=metadata.dev();
        Ok(Self { inodes:BTreeMap::from([(1,Inode { anchor,lookups:1,opens:0,parent:1 })]),handles:BTreeMap::new(),next_handle:1,
            root_inode,device,max_inodes:config.max_inodes,max_handles:config.max_handles,max_io:config.max_io_bytes,measurements })
    }
    fn node(&self,ino:u64)->io::Result<&Inode> { self.inodes.get(&ino).ok_or_else(||error(libc::ESTALE)) }
    fn fd(&self,ino:u64)->io::Result<RawFd> { Ok(self.node(ino)?.anchor.as_raw_fd()) }
    fn handle(&self,ino:u64,fh:u64)->io::Result<&File> { self.handles.get(&fh).filter(|v|v.ino==ino).map(|v|&v.file).ok_or_else(||error(libc::EBADF)) }
    fn number(&self,metadata:&Metadata)->io::Result<u64> {
        if metadata.dev()!=self.device { return Err(error(libc::EXDEV)); }
        if metadata.ino()==self.root_inode { Ok(1) } else { metadata.ino().checked_add(2).ok_or_else(||error(libc::EOVERFLOW)) }
    }
    fn attr(&self,ino:u64)->io::Result<FileAttr> {
        let m=self.node(ino)?.anchor.metadata()?;
        Ok(FileAttr { ino,size:m.len(),blocks:m.blocks(),atime:time(m.atime(),m.atime_nsec()),mtime:time(m.mtime(),m.mtime_nsec()),
            ctime:time(m.ctime(),m.ctime_nsec()),crtime:UNIX_EPOCH,kind:kind(&m)?,perm:(m.mode()&0o7777) as u16,
            nlink:u32::try_from(m.nlink()).map_err(|_|error(libc::EOVERFLOW))?,uid:m.uid(),gid:m.gid(),rdev:m.rdev() as u32,blksize:m.blksize() as u32,flags:0 })
    }
    fn intern(&mut self,anchor:File,parent:u64)->io::Result<u64> {
        let metadata=anchor.metadata()?;
        kind(&metadata)?;
        let ino=self.number(&metadata)?;
        if let Some(node)=self.inodes.get_mut(&ino) {
            node.lookups=node.lookups.checked_add(1).ok_or_else(||error(libc::EOVERFLOW))?;
        } else {
            if self.inodes.len()>=self.max_inodes { self.measurements.rejected(); return Err(error(libc::ENOSPC)); }
            self.inodes.insert(ino,Inode {anchor,lookups:1,opens:0,parent});
        }
        Ok(ino)
    }
    fn collect(&mut self,ino:u64) {
        if ino!=1 && self.inodes.get(&ino).is_some_and(|n|n.lookups==0 && n.opens==0) { self.inodes.remove(&ino); }
    }
    fn anchor(&self,parent:u64,name:&CString)->io::Result<File> { openat(self.fd(parent)?,name,libc::O_PATH,0) }
    fn lookup_name(&mut self,parent:u64,value:&OsStr)->io::Result<u64> { self.intern(self.anchor(parent,&name(value)?)?,parent) }
    fn reopen(&self,ino:u64,flags:i32)->io::Result<File> {
        if flags&libc::O_DIRECT!=0 {return Err(error(libc::EOPNOTSUPP));}
        let path=CString::new(format!("/proc/self/fd/{}",self.fd(ino)?)).unwrap();
        let fd=unsafe { libc::open(path.as_ptr(),(flags&!(libc::O_NOFOLLOW|libc::O_CREAT|libc::O_EXCL|libc::O_APPEND))|libc::O_CLOEXEC) };
        if fd<0 { Err(io::Error::last_os_error()) } else { Ok(unsafe { File::from_raw_fd(fd) }) }
    }
    fn adopt(&mut self,ino:u64,file:File)->io::Result<u64> {
        if self.handles.len()>=self.max_handles { self.measurements.rejected(); return Err(error(libc::EMFILE)); }
        let fh=self.next_handle;
        self.next_handle=fh.checked_add(1).ok_or_else(||error(libc::EOVERFLOW))?;
        self.inodes.get_mut(&ino).ok_or_else(||error(libc::ESTALE))?.opens+=1;
        self.handles.insert(fh,Handle{file,ino});
        Ok(fh)
    }
    fn release_handle(&mut self,ino:u64,fh:u64)->io::Result<()> {
        self.handle(ino,fh)?;
        self.handles.remove(&fh);
        self.inodes.get_mut(&ino).unwrap().opens-=1;
        self.collect(ino);
        Ok(())
    }
    fn checked_io(&self,offset:i64,size:usize)->io::Result<u64> {
        if offset<0 || size>self.max_io || (offset as u64).checked_add(size as u64).is_none_or(|n|n>i64::MAX as u64) {
            self.measurements.rejected(); return Err(error(libc::EINVAL));
        }
        Ok(offset as u64)
    }
    fn read_payload(&self,file:&File,offset:u64,output:&mut[u8])->io::Result<usize> {
        let mut count=0;
        while count<output.len() {
            match file.read_at(&mut output[count..],offset+count as u64) {
                Ok(n)=>{ self.measurements.read(n); count+=n; if n==0 { break; } },
                Err(e) if e.kind()==io::ErrorKind::Interrupted=>continue,
                Err(e)=>return Err(e),
            }
        }
        Ok(count)
    }
    fn write_payload(&self,file:&File,offset:u64,input:&[u8])->io::Result<usize> {
        let mut count=0;
        while count<input.len() {
            match file.write_at(&input[count..],offset+count as u64) {
                Ok(0)=>{self.measurements.write(0);return Err(error(libc::EIO));},
                Ok(n)=>{ self.measurements.write(n); count+=n; },
                Err(e) if e.kind()==io::ErrorKind::Interrupted=>continue,
                Err(e)=>return Err(e),
            }
        }
        Ok(count)
    }
    fn outcome<T>(&self,result:io::Result<T>,metadata:bool)->Result<T,i32> {
        match result { Ok(v)=>{ if metadata { self.measurements.metadata(); } Ok(v) },Err(e)=>{self.measurements.failed(); Err(errno(e))} }
    }
    fn room(&self)->io::Result<()> {
        if self.inodes.len()>=self.max_inodes || self.handles.len()>=self.max_handles { self.measurements.rejected(); Err(error(libc::ENOSPC)) } else { Ok(()) }
    }
}

impl Filesystem for Backing {
    fn init(&mut self,_:&Request<'_>,config:&mut KernelConfig)->Result<(),i32> {
        config.set_max_write(self.max_io as u32).map_err(|_|libc::EINVAL)?;
        if let Err(available)=config.set_max_readahead(self.max_io as u32) {
            if available!=0 {config.set_max_readahead(available).map_err(|_|libc::EINVAL)?;}
        }
        config.set_max_background(16).map_err(|_|libc::EINVAL)?;
        Ok(())
    }
    fn lookup(&mut self,_:&Request<'_>,parent:u64,value:&OsStr,reply:ReplyEntry) {
        let _guard=self.measurements.request();
        let result=self.lookup_name(parent,value).and_then(|ino|self.attr(ino));
        match self.outcome(result,true) { Ok(attr)=>reply.entry(&TTL,&attr,0),Err(e)=>reply.error(e) }
    }
    fn forget(&mut self,_:&Request<'_>,ino:u64,count:u64) {
        if ino!=1 { if let Some(node)=self.inodes.get_mut(&ino) { node.lookups=node.lookups.saturating_sub(count); } self.collect(ino); }
    }
    fn getattr(&mut self,_:&Request<'_>,ino:u64,_:Option<u64>,reply:ReplyAttr) {
        let _guard=self.measurements.request();
        match self.outcome(self.attr(ino),true) { Ok(attr)=>reply.attr(&TTL,&attr),Err(e)=>reply.error(e) }
    }
    fn setattr(&mut self,_:&Request<'_>,ino:u64,mode:Option<u32>,uid:Option<u32>,gid:Option<u32>,size:Option<u64>,atime:Option<TimeOrNow>,mtime:Option<TimeOrNow>,_:Option<SystemTime>,fh:Option<u64>,_:Option<SystemTime>,_:Option<SystemTime>,_:Option<SystemTime>,flags:Option<u32>,reply:ReplyAttr) {
        let _guard=self.measurements.request();
        let result=(|| {
            if flags.is_some_and(|v|v!=0) { return Err(error(libc::EOPNOTSUPP)); }
            let owned;
            let file=if let Some(fh)=fh { self.handle(ino,fh)? } else { owned=self.reopen(ino,if size.is_some(){libc::O_WRONLY}else{libc::O_RDONLY})?; &owned };
            if let Some(mode)=mode { syscall(unsafe {libc::fchmod(file.as_raw_fd(),mode)})?; }
            if uid.is_some() || gid.is_some() { syscall(unsafe{libc::fchown(file.as_raw_fd(),uid.unwrap_or(u32::MAX),gid.unwrap_or(u32::MAX))})?; }
            if let Some(size)=size { file.set_len(size)?; }
            if atime.is_some() || mtime.is_some() {
                let stamp=|value:Option<TimeOrNow>|->io::Result<libc::timespec> {
                    Ok(match value {
                        None=>libc::timespec{tv_sec:0,tv_nsec:libc::UTIME_OMIT},
                        Some(TimeOrNow::Now)=>libc::timespec{tv_sec:0,tv_nsec:libc::UTIME_NOW},
                        Some(TimeOrNow::SpecificTime(value))=>{ let n=value.duration_since(UNIX_EPOCH).map(|v|v.as_nanos() as i128).unwrap_or_else(|e|-(e.duration().as_nanos() as i128));
                            libc::timespec{tv_sec:i64::try_from(n.div_euclid(1_000_000_000)).map_err(|_|error(libc::EOVERFLOW))?,tv_nsec:n.rem_euclid(1_000_000_000) as i64} }
                    })
                };
                syscall(unsafe{libc::futimens(file.as_raw_fd(),[stamp(atime)?,stamp(mtime)?].as_ptr())})?;
            }
            self.attr(ino)
        })();
        match self.outcome(result,true) { Ok(attr)=>reply.attr(&TTL,&attr),Err(e)=>reply.error(e) }
    }
    fn mkdir(&mut self,req:&Request<'_>,parent:u64,value:&OsStr,mode:u32,umask:u32,reply:ReplyEntry) {
        let _guard=self.measurements.request();
        let result=(|| {
            self.room()?; let value=name(value)?; let fd=self.fd(parent)?;
            let parent_metadata=self.node(parent)?.anchor.metadata()?;
            let inherited=parent_metadata.mode()&libc::S_ISGID;
            let group=if inherited!=0 {parent_metadata.gid()} else {req.gid()};
            syscall(unsafe{libc::mkdirat(fd,value.as_ptr(),mode&!umask)})?;
            syscall(unsafe{libc::fchownat(fd,value.as_ptr(),req.uid(),group,libc::AT_SYMLINK_NOFOLLOW)})?;
            syscall(unsafe{libc::fchmodat(fd,value.as_ptr(),(mode&!umask)|inherited,0)})?;
            let ino=self.intern(self.anchor(parent,&value)?,parent)?; self.attr(ino)
        })();
        match self.outcome(result,true) { Ok(attr)=>reply.entry(&TTL,&attr,0),Err(e)=>reply.error(e) }
    }
    fn unlink(&mut self,_:&Request<'_>,parent:u64,value:&OsStr,reply:ReplyEmpty) {
        let _guard=self.measurements.request();
        let result=(||syscall(unsafe{libc::unlinkat(self.fd(parent)?,name(value)?.as_ptr(),0)}))();
        match self.outcome(result,true) { Ok(())=>reply.ok(),Err(e)=>reply.error(e) }
    }
    fn rmdir(&mut self,_:&Request<'_>,parent:u64,value:&OsStr,reply:ReplyEmpty) {
        let _guard=self.measurements.request();
        let result=(||syscall(unsafe{libc::unlinkat(self.fd(parent)?,name(value)?.as_ptr(),libc::AT_REMOVEDIR)}))();
        match self.outcome(result,true) { Ok(())=>reply.ok(),Err(e)=>reply.error(e) }
    }
    fn rename(&mut self,_:&Request<'_>,parent:u64,value:&OsStr,newparent:u64,newvalue:&OsStr,flags:u32,reply:ReplyEmpty) {
        let _guard=self.measurements.request();
        let result=(|| {
            if flags&!(libc::RENAME_NOREPLACE|libc::RENAME_EXCHANGE)!=0 { return Err(error(libc::EINVAL)); }
            let old=name(value)?; let new=name(newvalue)?;
            let moved=self.anchor(parent,&old).and_then(|f|self.number(&f.metadata()?))?;
            let exchanged=if flags&libc::RENAME_EXCHANGE!=0 { Some(self.number(&self.anchor(newparent,&new)?.metadata()?)?) } else {None};
            syscall(unsafe{libc::renameat2(self.fd(parent)?,old.as_ptr(),self.fd(newparent)?,new.as_ptr(),flags)})?;
            if let Some(node)=self.inodes.get_mut(&moved) { node.parent=newparent; }
            if let Some(node)=exchanged.and_then(|ino|self.inodes.get_mut(&ino)) { node.parent=parent; }
            Ok(())
        })();
        match self.outcome(result,true) { Ok(())=>reply.ok(),Err(e)=>reply.error(e) }
    }
    fn link(&mut self,_:&Request<'_>,ino:u64,parent:u64,value:&OsStr,reply:ReplyEntry) {
        let _guard=self.measurements.request();
        let result=(|| {
            let proc=CString::new(format!("/proc/self/fd/{}",self.fd(ino)?)).unwrap();
            syscall(unsafe{libc::linkat(libc::AT_FDCWD,proc.as_ptr(),self.fd(parent)?,name(value)?.as_ptr(),libc::AT_SYMLINK_FOLLOW)})?;
            let ino=self.lookup_name(parent,value)?; self.attr(ino)
        })();
        match self.outcome(result,true) { Ok(attr)=>reply.entry(&TTL,&attr,0),Err(e)=>reply.error(e) }
    }
    fn create(&mut self,req:&Request<'_>,parent:u64,value:&OsStr,mode:u32,umask:u32,flags:i32,reply:ReplyCreate) {
        let _guard=self.measurements.request();
        let result=(|| {
            self.room()?; let value=name(value)?;
            let parent_metadata=self.node(parent)?.anchor.metadata()?;
            let group=if parent_metadata.mode()&libc::S_ISGID!=0 {parent_metadata.gid()} else {req.gid()};
            let file=match openat(self.fd(parent)?,&value,flags|libc::O_CREAT|libc::O_EXCL,mode&!umask) {
                Ok(file)=>{syscall(unsafe{libc::fchown(file.as_raw_fd(),req.uid(),group)})?;syscall(unsafe{libc::fchmod(file.as_raw_fd(),mode&!umask)})?;file},
                Err(e) if e.raw_os_error()==Some(libc::EEXIST) && flags&libc::O_EXCL==0=>openat(self.fd(parent)?,&value,flags&!libc::O_CREAT,0)?,
                Err(e)=>return Err(e),
            };
            let ino=self.intern(self.anchor(parent,&value)?,parent)?;
            let attr=self.attr(ino)?; let fh=self.adopt(ino,file)?; Ok((attr,fh))
        })();
        match self.outcome(result,true) { Ok((attr,fh))=>reply.created(&TTL,&attr,0,fh,0),Err(e)=>reply.error(e) }
    }
    fn open(&mut self,_:&Request<'_>,ino:u64,flags:i32,reply:ReplyOpen) {
        let _guard=self.measurements.request();
        let result=self.reopen(ino,flags).and_then(|f|self.adopt(ino,f));
        match self.outcome(result,true) { Ok(fh)=>reply.opened(fh,0),Err(e)=>reply.error(e) }
    }
    fn read(&mut self,_:&Request<'_>,ino:u64,fh:u64,offset:i64,size:u32,_:i32,_:Option<u64>,reply:ReplyData) {
        let _guard=self.measurements.request();
        let result=(|| { let offset=self.checked_io(offset,size as usize)?; let mut data=vec![0;size as usize]; let n=self.read_payload(self.handle(ino,fh)?,offset,&mut data)?; data.truncate(n); Ok(data) })();
        match self.outcome(result,false) { Ok(data)=>reply.data(&data),Err(e)=>reply.error(e) }
    }
    fn write(&mut self,_:&Request<'_>,ino:u64,fh:u64,offset:i64,data:&[u8],_:u32,_:i32,_:Option<u64>,reply:ReplyWrite) {
        let _guard=self.measurements.request();
        let result=(||self.write_payload(self.handle(ino,fh)?,self.checked_io(offset,data.len())?,data))();
        match self.outcome(result,false) { Ok(n)=>reply.written(n as u32),Err(e)=>reply.error(e) }
    }
    fn flush(&mut self,_:&Request<'_>,ino:u64,fh:u64,_:u64,reply:ReplyEmpty) {
        let _guard=self.measurements.request();
        let result=(|| { let fd=unsafe{libc::dup(self.handle(ino,fh)?.as_raw_fd())}; if fd<0 {return Err(io::Error::last_os_error());} syscall(unsafe{libc::close(fd)}) })();
        match self.outcome(result,true) { Ok(())=>reply.ok(),Err(e)=>reply.error(e) }
    }
    fn release(&mut self,_:&Request<'_>,ino:u64,fh:u64,_:i32,_:Option<u64>,_:bool,reply:ReplyEmpty) {
        let _guard=self.measurements.request(); let result=self.release_handle(ino,fh);
        match self.outcome(result,true) { Ok(())=>reply.ok(),Err(e)=>reply.error(e) }
    }
    fn fsync(&mut self,_:&Request<'_>,ino:u64,fh:u64,datasync:bool,reply:ReplyEmpty) {
        let _guard=self.measurements.request();
        let result=self.handle(ino,fh).and_then(|f|if datasync {f.sync_data()} else {f.sync_all()});
        match self.outcome(result,true) { Ok(())=>reply.ok(),Err(e)=>reply.error(e) }
    }
    fn opendir(&mut self,req:&Request<'_>,ino:u64,_:i32,reply:ReplyOpen) { self.open(req,ino,libc::O_RDONLY|libc::O_DIRECTORY,reply); }
    fn readdir(&mut self,_:&Request<'_>,ino:u64,fh:u64,offset:i64,mut reply:ReplyDirectory) {
        let _guard=self.measurements.request();
        let result=(|| {
            let fd=self.handle(ino,fh)?.as_raw_fd();
            if offset<0 || unsafe{libc::lseek(fd,offset,libc::SEEK_SET)}<0 { return Err(io::Error::last_os_error()); }
            let mut buffer=[0u8;16384];
            loop {
                let count=unsafe{libc::syscall(libc::SYS_getdents64,fd,buffer.as_mut_ptr(),buffer.len())};
                if count<0 { return Err(io::Error::last_os_error()); } if count==0 {break;}
                let mut pos=0;
                while pos<count as usize {
                    if pos+19>count as usize {return Err(error(libc::EIO));}
                    let entry=&buffer[pos..count as usize];
                    let real=u64::from_ne_bytes(entry[..8].try_into().unwrap());
                    let cookie=i64::from_ne_bytes(entry[8..16].try_into().unwrap());
                    let length=u16::from_ne_bytes(entry[16..18].try_into().unwrap()) as usize;
                    if length<20 || pos+length>count as usize {return Err(error(libc::EIO));}
                    let end=entry[19..length].iter().position(|v|*v==0).ok_or_else(||error(libc::EIO))?+19;
                    let value=OsStr::from_bytes(&entry[19..end]);
                    let number=if value=="." {ino} else if value==".." {self.node(ino)?.parent} else if real==self.root_inode {1} else {real.checked_add(2).ok_or_else(||error(libc::EOVERFLOW))?};
                    let file_kind=if value=="." || value==".." {FileType::Directory} else {match entry[18] {libc::DT_DIR=>FileType::Directory,libc::DT_REG=>FileType::RegularFile,
                        libc::DT_UNKNOWN=>kind(&self.anchor(ino,&name(value)?)?.metadata()?)?,_=>return Err(error(libc::EOPNOTSUPP))}};
                    if reply.add(number,cookie,file_kind,value) {return Ok(());}
                    pos+=length;
                }
            }
            Ok(())
        })();
        match self.outcome(result,true) { Ok(())=>reply.ok(),Err(e)=>reply.error(e) }
    }
    fn releasedir(&mut self,req:&Request<'_>,ino:u64,fh:u64,flags:i32,reply:ReplyEmpty) {self.release(req,ino,fh,flags,None,false,reply);}
    fn fsyncdir(&mut self,req:&Request<'_>,ino:u64,fh:u64,datasync:bool,reply:ReplyEmpty) {self.fsync(req,ino,fh,datasync,reply);}
    fn statfs(&mut self,_:&Request<'_>,ino:u64,reply:ReplyStatfs) {
        let _guard=self.measurements.request();
        let result=(|| {let mut stat=std::mem::MaybeUninit::<libc::statvfs>::uninit(); syscall(unsafe{libc::fstatvfs(self.fd(ino)?,stat.as_mut_ptr())})?; Ok(unsafe{stat.assume_init()})})();
        match self.outcome(result,true) {Ok(s)=>reply.statfs(s.f_blocks,s.f_bfree,s.f_bavail,s.f_files,s.f_ffree,s.f_bsize as u32,s.f_namemax as u32,s.f_frsize as u32),Err(e)=>reply.error(e)}
    }
    fn fallocate(&mut self,_:&Request<'_>,ino:u64,fh:u64,offset:i64,length:i64,mode:i32,reply:ReplyEmpty) {
        let _guard=self.measurements.request();
        let result=(|| { if offset<0 || length<0 || offset.checked_add(length).is_none() {return Err(error(libc::EINVAL));} syscall(unsafe{libc::fallocate(self.handle(ino,fh)?.as_raw_fd(),mode,offset,length)})})();
        match self.outcome(result,true) {Ok(())=>reply.ok(),Err(e)=>reply.error(e)}
    }
    fn lseek(&mut self,_:&Request<'_>,ino:u64,fh:u64,offset:i64,whence:i32,reply:ReplyLseek) {
        let _guard=self.measurements.request();
        let result=(|| {let n=unsafe{libc::lseek(self.handle(ino,fh)?.as_raw_fd(),offset,whence)}; if n<0 {Err(io::Error::last_os_error())} else {Ok(n)}})();
        match self.outcome(result,true) {Ok(n)=>reply.offset(n),Err(e)=>reply.error(e)}
    }
    fn copy_file_range(&mut self,_:&Request<'_>,_:u64,_:u64,_:i64,_:u64,_:u64,_:i64,_:u64,_:u32,reply:ReplyWrite) {
        // ENOSYS makes the kernel copy through READ/WRITE, keeping payload accounting in one path.
        reply.error(libc::ENOSYS);
    }
}
