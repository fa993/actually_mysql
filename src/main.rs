pub mod db;
pub mod query;

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::rc::Weak;

use query::{Closure, Criteria};

const MAX_MEM_LIM: u32 = 4096; // ROWS

pub type Num_type = i64;
pub type String_type = String;

pub const NUM_BASE: u32 = 10;

#[derive(Default)]
pub struct Table {
    col_names: Vec<ColumnEntry>,
    all: Vec<TableEntry>,
}

struct TableEntry {
    col_data: Vec<TableCell>,
}

#[derive(Clone)]
struct ColumnEntry {
    col_name: String,
    col_type: TableCell,
}

struct ColumnReference {
    entity_type: TableCell,
    entity_offset: u32,
    entity_offset_from: Weak<RowReference>,
}

struct RowReference {
    entity_offset: u32,
    entity_offset_from: Weak<TableReference>,
}

enum TableReference {
    InFile(File),
    InMemory(Table),
    InMemoryBackedByFile(Table, File),
}

impl TableReference {
    pub fn read_table(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut buffer_string = String::new();
        if let TableReference::InFile(t) = self {
            let mut rd = BufReader::new(t);
            // rd.lines()
        } else {
            panic!("File open failed failed")
        }
        Ok(())
    }

    pub fn select(&self, cri: &Criteria) -> TableReference {
        //assign col_name to col_index

        if let TableReference::InMemory(x) = &self {
            let mut rt = Table {
                all: Vec::new(),
                col_names: x.col_names.clone(),
            };
            let lookup = x
                .col_names
                .iter()
                .enumerate()
                .map(|(ind, s)| (&s.col_name, ind))
                .collect::<HashMap<_, _>>();
            for t in &x.all {
                let mut v = Vec::new();
                for cr in &cri.cls.col_name {
                    v.push(t.col_data[lookup[cr] as usize].clone())
                }
                if (cri.cls.act_clo)(v.iter().collect::<Vec<_>>().as_slice()) {
                    rt.all.push(TableEntry { col_data: v })
                }
            }
            TableReference::InMemory(rt)
        } else {
            //read and parse while collecting result
            //make decision whether to commit in memory or store in tmp file and pipe output
            // set memory limit var
            // if exceeds then send to file otherwise keep in memory

            todo!()
        }
    }
}

#[derive(Clone)]
pub enum TableCell {
    Num(Option<Num_type>),
    Str(Option<String_type>),
}

fn create_table(
    name: &str,
    cols: Vec<String>,
) -> Result<TableReference, Box<dyn std::error::Error>> {
    let f = File::create(name)?;
    Ok(TableReference::InFile(f))
}

fn get_table(name: &str) -> Result<TableReference, Box<dyn std::error::Error>> {
    let f = File::open(name)?;
    Ok(TableReference::InFile(f))
}

fn select(t_ref: Vec<&mut TableReference>, criteria: &Criteria) -> Table {
    todo!()
}

fn main() {
    println!("Hello, world!");
}
