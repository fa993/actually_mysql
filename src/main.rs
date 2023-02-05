pub mod db;
pub mod query;

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Write;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::rc::Weak;

use db::TableParser;
use query::Criteria;
use tabled::builder::Builder;

use crate::query::Closure;

const MAX_MEM_LIM: usize = 4096; // ROWS

pub type Num_type = i64;
pub type String_type = String;

pub const NUM_BASE: u32 = 10;

#[derive(Default, Debug)]
pub struct Table {
    col_names: Vec<ColumnEntry>,
    all: Vec<TableEntry>,
}

impl Table {
    pub fn print_table(&self) {
        println!("{:?}", self);
        let mut bu = Builder::default();
        bu.set_columns(self.col_names.iter().map(|f| f.col_name.as_str()));
        self.all.iter().for_each(|f| {
            bu.add_record(f.col_data.iter().map(|y| y.get_cow()));
        });
        // println!("")
        println!("{}", bu.build())
    }

    pub fn copy_into_table(&mut self, other: &Self) -> Result<bool, Box<dyn std::error::Error>> {
        //sanity checks
        if other.col_names.as_slice() == self.col_names.as_slice() {
            //begin copy
            //do constraints check later
            for item in &other.all {
                self.all.push(TableEntry {
                    col_data: item.col_data.clone(),
                })
            }
            Ok(true)
        } else {
            //todo make your own error instead
            Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "column names _lists_ are not same",
            )))
        }
    }
}

#[derive(Debug)]
struct TableEntry {
    col_data: Vec<TableCell>,
}

#[derive(Clone, Debug, PartialEq)]
struct ColumnEntry {
    col_name: String,
    col_type: TableCell,
}

impl ColumnEntry {
    pub fn write_type(&self) -> &str {
        match self.col_type {
            TableCell::Num(_) => "Num",
            TableCell::Str(_) => "String",
        }
    }
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
    File(File),
    Memory(Table, Option<File>),
}

impl TableReference {
    pub fn print_table(&self) {
        if let TableReference::Memory(t, _) = self {
            t.print_table();
        } else {
            todo!()
        }
    }

    pub fn read_table(&mut self) -> Result<TableReference, Box<dyn std::error::Error>> {
        if let TableReference::File(t) = self {
            let fp2 = t.try_clone();
            let rd = BufReader::new(t);
            let mut table = Table::default();
            let mut par = TableParser {
                table: &mut table,
                state: Default::default(),
                buffer: Vec::new(),
            };
            for lt in rd.lines() {
                let st = lt?;
                println!("{}", st.as_str());
                par.next(st)?;
                let y = par.table.all.len();
                if y > MAX_MEM_LIM {
                    //too large
                    return Err(Box::new(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "Table too large",
                    )));
                }
            }
            Ok(TableReference::Memory(table, Some(fp2?)))
        } else {
            panic!("File open failed failed")
        }
    }

    pub fn insert_rows(
        &mut self,
        table: &TableReference,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        match (self, table) {
            (TableReference::Memory(se, _), TableReference::Memory(t, _)) => se.copy_into_table(t),
            _ => unimplemented!(),
        }
    }

    pub fn select(&mut self, cri: &Criteria) -> TableReference {
        //assign col_name to col_index

        if let TableReference::Memory(x, _) = &self {
            let mut rt = Table {
                all: Vec::new(),
                col_names: Vec::new(),
            };
            let lookup = x
                .col_names
                .iter()
                .enumerate()
                .map(|(ind, s)| (&s.col_name, ind))
                .collect::<HashMap<_, _>>();
            for yt in &cri.re {
                rt.col_names.push(x.col_names[lookup[&yt] as usize].clone())
            }
            for t in &x.all {
                let mut v = Vec::new();
                for cr in &cri.cls.col_name {
                    v.push(t.col_data[lookup[cr] as usize].clone())
                }
                if (cri.cls.act_clo)(v.iter().collect::<Vec<_>>().as_slice()) {
                    rt.all.push(TableEntry { col_data: v })
                }
            }
            TableReference::Memory(rt, None)
        } else {
            //read and parse while collecting result
            //make decision whether to commit in memory or store in tmp file and pipe output
            // set memory limit var
            // if exceeds then send to file otherwise keep in memory
            self.read_table();

            todo!()
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum TableCell {
    Num(Option<Num_type>),
    Str(Option<String_type>),
}

impl TableCell {
    pub fn get_cow(&self) -> Cow<'_, str> {
        match &self {
            Self::Num(Some(t)) => Cow::Owned(t.to_string()),
            Self::Str(Some(t)) => Cow::Borrowed(t.as_str()),
            _ => Cow::Borrowed("NULL"),
        }
    }
}

fn create_table(
    name: &str,
    cols: Vec<ColumnEntry>,
) -> Result<TableReference, Box<dyn std::error::Error>> {
    // let mut f = File::create(name)?;
    let mut f = File::options()
        .read(true)
        .write(true)
        .create_new(true)
        .open(name)?;
    let mut buffer = String::new();
    writeln!(&mut buffer, "ColDescStart")?;
    for f2 in cols {
        writeln!(&mut buffer, "\"{}\"", f2.col_name)?;
        writeln!(&mut buffer, "\"{}\"", f2.write_type())?;
    }
    writeln!(&mut buffer, "ColDescEnd")?;
    std::io::Write::write(&mut f, buffer.as_bytes())?;
    Ok(TableReference::File(f))
}

fn get_table(name: &str) -> Result<TableReference, Box<dyn std::error::Error>> {
    // let f = File::open(name)?;
    let f = File::options().read(true).write(true).open(name)?;
    Ok(TableReference::File(f))
}

fn select(t_ref: Vec<&mut TableReference>, criteria: &Criteria) -> Table {
    todo!()
}

fn main() {
    let vec = vec![
        ColumnEntry {
            col_name: "Hei".to_string(),
            col_type: TableCell::Num(None),
        },
        ColumnEntry {
            col_name: "asd".to_string(),
            col_type: TableCell::Str(None),
        },
    ];
    // let mut tb = create_table("asd", vec).expect("Shit happens");
    let mut tb = get_table("asd").expect("Shit happens");

    let mut t1 = tb.read_table().expect("Shit happens pt 2");

    let t2 = TableReference::Memory(Table {
        col_names: vec,
        all: vec![
            TableEntry {
                col_data: vec![
                    TableCell::Num(Some(23)),
                    TableCell::Str(Some("asd".to_string())),
                ],
            },
            TableEntry {
                col_data: vec![TableCell::Num(Some(46)), TableCell::Str(None)],
            },
        ],
    }, None);

    t1.insert_rows(&t2).expect("Shit part 3");
    t1.print_table();
    t1.select(&Criteria {
        cls: Closure {
            col_name: vec!["Hei".to_string()],
            act_clo: Box::new(|f| {
                if let TableCell::Num(Some(rt)) = f[0] {
                    rt > &30
                } else {
                    false
                }
            }),
        },
        re: vec!["asd".to_string()],
    })
    .print_table();
    // TableReference::InMemory(table).select(cri);
    println!("Hello, world!");
}
