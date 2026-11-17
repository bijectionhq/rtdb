use rtdb_columnar::MemoryBudget;
use rtdb_plan::ControlStatement;
use rtdb_sql::{PrepareOptions,StatementKind};
use serde::Deserialize;
use serde_json::{Value,json};
use sha2::{Digest,Sha256};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Workload {schema_version:u32, statements:Vec<SelectedSql>}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SelectedSql {location:String,kind:String,sql:String,psycopg_parameters:bool}

fn postgres_sql(source:&str)->Result<String,Box<dyn std::error::Error+Send+Sync>> {
    let mut output=String::new();let mut chars=source.chars().peekable();let mut parameter=0;
    while let Some(c)=chars.next() {
        if c!='%' {output.push(c);continue;}
        match chars.peek().copied() {
            Some('%')=>{chars.next();output.push('%');},
            Some('s'|'b'|'t')=>{chars.next();parameter+=1;output.push_str(&format!("${parameter}"));},
            None|Some('\n')=>output.push('%'),
            _=>return Err("filesystem workload requires valid positional psycopg placeholders".into()),
        }
    }
    Ok(output)
}

/// Checks an ordered SQL selection through the production parsers without binding or opening services.
/// Input is bounded to 16 MiB, 10,000 statements, and 64 KiB per SQL string.
/// Parser scratch is reserved before each parse against a 256 MiB budget.
/// Returns the exact input digest and per-location source digests after every statement satisfies the local workload policy.
/// Reads, table mutations, BEGIN and the fixed client controls are accepted; management, named execution and multiple statements return an error.
pub fn validate_workload(bytes:&[u8])->Result<Value,Box<dyn std::error::Error+Send+Sync>> {
    if bytes.len()>16*1024*1024 {return Err("workload proof exceeds input bound".into());}
    let memory=MemoryBudget::new(256*1024*1024);
    let _input=memory.reserve(bytes.len().checked_mul(8).ok_or("workload input bound overflow")?.checked_add(65536).ok_or("workload input bound overflow")?)?;
    let workload:Workload=serde_json::from_slice(bytes)?;
    if workload.schema_version!=1 || workload.statements.is_empty() || workload.statements.len()>10000 {return Err("invalid workload proof schema or statement count".into());}
    let mut output=Vec::new();let mut locations=std::collections::BTreeSet::new();
    for selected in workload.statements {
        if selected.sql.is_empty() || selected.sql.len()>65536 || selected.location.len()>1024 || !locations.insert(selected.location.clone()) {return Err("workload statement exceeds source/identity bound".into());}
        let wire=if selected.psycopg_parameters {postgres_sql(&selected.sql)?} else {selected.sql.clone()};
        let _scratch=memory.reserve(wire.len()*256+1024*1024)?;
        let controls=rtdb_sql::parse_control_in(&wire,&PrepareOptions::default(),&memory)?;
        let accepted=match controls {
            Some(ControlStatement::Begin(_))=>selected.kind=="begin" || selected.kind=="generated",
            Some(ControlStatement::Commit(_) | ControlStatement::Rollback | ControlStatement::SetAwaitToken(_))=>selected.kind=="generated",
            Some(ControlStatement::Show {name})=>selected.kind=="generated" && ["latest_submitted_tx","latest_completed_txs","snapshot_token","await_token"].contains(&name.as_str()),
            Some(_)=>false,
            None=>{
                let parsed=rtdb_sql::parse_in(&wire,&memory)?;
                parsed.len()==1 && parsed[0].is_local_workload_statement() && match selected.kind.as_str() {
                    "read" | "generated"=>parsed[0].kind()==StatementKind::Query,
                    "mutation"=>parsed[0].kind()==StatementKind::Dml,
                    "transaction_statement"=>matches!(parsed[0].kind(),StatementKind::Query|StatementKind::Dml),
                    _=>false,
                }
            }
        };
        if !accepted {return Err(format!("workload statement at {} is outside the local measurement policy",selected.location).into());}
        output.push(json!({"location":selected.location,"kind":selected.kind,"psycopg_parameters":selected.psycopg_parameters,
            "sql_sha256":format!("{:x}",Sha256::digest(selected.sql.as_bytes())),"wire_sql_sha256":format!("{:x}",Sha256::digest(wire.as_bytes()))}));
    }
    Ok(json!({"schema_version":1,"request_sha256":format!("{:x}",Sha256::digest(bytes)),"policy":"local-storage-workload-v1",
        "attached_databases_allowed":false,"runtime_configuration_changes_allowed":false,"statements":output}))
}
