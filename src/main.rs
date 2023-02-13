pub mod db;
pub mod query;

use std::collections::HashMap;
use std::fmt::Display;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Seek, Write as ioWrite, Lines, Read};
use std::marker::PhantomData;
use std::rc::{Weak, Rc};

use db::TableParser;
use pest::Parser;
use pest_derive::Parser;
use query::Criteria;

use crate::db::ParseState;
use crate::query::Closure;

const MAX_MEM_LIM: usize = 4096; // ROWS

pub type NumType = i64;
pub type StringType = String;

pub const NUM_BASE: u32 = 10;

#[derive(Default, Debug)]
pub struct Table {
    name: Option<String>,
    col_names: Vec<ColumnEntry>,
    all: Vec<TableEntry>,
}

#[derive(Debug, Clone)]
pub struct TableEntry {
    col_data: Vec<TableCell>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ColumnEntry {
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

// struct ColumnReference {
//     entity_type: TableCell,
//     entity_offset: u32,
//     entity_offset_from: Weak<RowReference>,
// }

// struct RowReference {
//     entity_offset: u32,
//     entity_offset_from: Weak<TableReference<'a>>,
// }

struct TableReference {
    inner: TableKind
}

enum TableKind {
    File,
    Memory,
}

#[derive(Debug)]
pub enum TableLikeError {
    IoError {
        source: std::io::Error
    },
    FmtError,
    SpecificError {
        message: String,
    },
    Other
}

impl TableLikeError {
    pub fn new(msg: &str) -> TableLikeError {
        Self::SpecificError { message: msg.to_string() }
    }
}

impl From<std::io::Error> for TableLikeError {
    fn from(value: std::io::Error) -> Self {
        Self::IoError { source: value }
    }
}

impl From<std::fmt::Error> for TableLikeError {
    fn from(_: std::fmt::Error) -> Self {
        Self::FmtError
    }
}

pub trait TableLike: Display {

    fn get_name(&self) -> Option<&str>;
    //TODO make get_rows return references
    fn get_rows(&self) -> Box<dyn Iterator<Item = Result<TableEntry, TableLikeError>> + '_>;
    fn get_cols(&self) -> Result<Vec<ColumnEntry>, TableLikeError>;
    fn add_rows(&mut self, rows: &mut dyn Iterator<Item = TableEntry>) -> Result<(), TableLikeError>;
    fn flush(&mut self, t: Box<&dyn TableLike>) -> Result<(), TableLikeError>;
    fn move_to_memory(&mut self) -> Result<Table, TableLikeError>;
    fn move_to_file(&mut self, name: &str) -> Result<FileTable, TableLikeError>;

}

impl TableLike for Table {

    fn flush(&mut self, t: Box<&dyn TableLike>) -> Result<(), TableLikeError> {
        self.col_names.clear();
        self.col_names.extend(t.get_cols()?);
        self.all.clear();
        for (idx, row) in t.get_rows().enumerate() {
            if idx > MAX_MEM_LIM {
                return Err(TableLikeError::new("column names _lists_ are not same"));
            }
            self.all.push(row?.clone());
        }
        Ok(())
    }

    fn get_name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    fn add_rows(&mut self, rows: &mut dyn Iterator<Item = TableEntry>) -> Result<(), TableLikeError> {
        rows.for_each(|f| self.all.push(f));
        Ok(())
    }
    
    fn get_cols(&self) -> Result<Vec<ColumnEntry>, TableLikeError> {
        Ok(self.col_names.clone())
    }
    
    fn get_rows(&self) -> Box<dyn Iterator<Item = Result<TableEntry, TableLikeError>> + '_> {
        Box::new(self.all.iter().map(|f| Ok(f.clone())))
    }

    fn move_to_file(&mut self, name: &str) -> Result<FileTable, TableLikeError> {
        let mut f = FileTable::new(name)?;
        f.flush(Box::new(self))?;
        Ok(f)
    }

    fn move_to_memory(&mut self) -> Result<Table, TableLikeError> {
        Err(TableLikeError::new("Already a Memory Table"))
    }

}

#[derive(PartialEq, PartialOrd, Debug)]
pub enum PrintOption {
    FullTable,
    StartTable,
    MidTable,
    EndTable
}

impl Table {
    pub fn print_table(&self, f: &mut std::fmt::Formatter<'_>, widths: &mut [usize], table_option: PrintOption) -> Result<(), TableLikeError> {
        //get max width of each column
        //then it's just simple prints all the way
        //O(n) operations all the way
        // | for column terminators and + for corners
        // - for header into fields


        for t in &self.all {
            for (r, e) in t.col_data.iter().zip(widths.iter_mut()) {
                *e = r.get_len().max(*e)
            }
        }

        if table_option == PrintOption::FullTable || table_option == PrintOption::StartTable {
            for (t, q) in self.col_names.iter().zip(widths.iter_mut()) {
                *q = t.col_name.len().max(*q);
            }

            //now actually print 
            //one char space between col borders
            //for col names
            //border
            for sz in widths.iter() {
                write!(f, "+-{num:-<width$}-", num = '-', width = sz)?;
            }
            writeln!(f, "+")?;
            //for col_header
            for (t, sz) in self.col_names.iter().zip(widths.iter()) {
                write!(f, "| {num:<width$} ", num = t.col_name.as_str(), width = sz)?;
            }
            writeln!(f, "|")?;
            //for col_header bottom border
            for sz in widths.iter() {
                write!(f, "+-{num:-<width$}-", num = '-', width = sz)?;
            }
            writeln!(f, "+")?;
        }
        
        let mut first = true;
        //for row items
        for row in &self.all {
            if !first {
                writeln!(f)?;
            } else {
                first = false;
            }
            for (t, sz) in row.col_data.iter().zip(widths.iter()) {
                write!(f, "| {t:<sz$} ")?;
            }
            write!(f, "|")?;
        }

        if table_option == PrintOption::EndTable || table_option == PrintOption::FullTable {
            //for closing border
            writeln!(f)?;
            for sz in widths.iter() {
                write!(f, "+-{num:-<width$}-", num = '-', width = sz)?;
            }
            write!(f, "+")?;
        }
        
        Ok(())
    }
}

impl Display for Table {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut v = Vec::with_capacity(self.col_names.len());
        (0..self.col_names.len()).for_each(|_| v.push(0));
        self.print_table(f, &mut v,PrintOption::FullTable).map_err(|_| std::fmt::Error)
    }
}

pub struct FileTable {
    name: String,
    inner: File,
}

impl FileTable {
    fn new(name: &str) -> Result<FileTable, TableLikeError> {
        struct EmptyIter<'a> {
            __data: PhantomData<&'a ()>
        }
        impl<'a> Iterator for EmptyIter<'a> {
            type Item = Result<&'a TableEntry, TableLikeError>;
            fn next(&mut self) -> Option<Self::Item> {
                None
            }
        }
        Ok(FileTable { name: name.to_owned(), inner: File::options()
            .read(true)
            .write(true)
            .create_new(false)
            .open(name)? 
        })
    }

}

impl Display for FileTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.print_table(f).map_err(|f| std::fmt::Error)?;
        Ok(())
    }
}

impl TableLike for FileTable {

    fn flush(&mut self, t: Box<&dyn TableLike>) -> Result<(), TableLikeError>{
        let f = &mut self.inner;
        f.set_len(0)?;
        f.flush()?;
        f.rewind()?;
        let mut wri = BufWriter::new(f);
        writeln!(&mut wri, "ColDescStart")?;
        wri.flush()?;
        for f2 in t.get_cols()? {
            writeln!(&mut wri, "\"{}\"", f2.col_name)?;
            writeln!(&mut wri, "\"{}\"", f2.write_type())?;
        }
        writeln!(&mut wri, "ColDescEnd")?;
        for row in t.get_rows() {
            writeln!(&mut wri, "RStart")?;
            for row_col in &row?.col_data {
                writeln!(&mut wri, "\"{row_col}\"")?;
            }
            writeln!(&mut wri, "REnd")?;
        }
        wri.flush()?;
        Ok(())
    }

    fn get_name(&self) -> Option<&str> {
        Some(&self.name)
    }

    fn get_rows(&self) -> Box<dyn Iterator<Item = Result<TableEntry, TableLikeError>> + '_> {
        if let Err(e) = (&mut &(self.inner)).rewind() {
            return Box::new(ErrIter {
                err: Some(e.into())
            });
        }
        let rd = BufReader::new(&self.inner);
        let mut par = TableParser::default();
        let mut y = rd.lines();
        while let Some(Ok(st)) = y.next() {
            // let st = lt?;
            let res = par.next(st);
            
            if res.is_err() {
                return Box::new(ErrIter{ err: Some(res.map_err(TableLikeError::new).unwrap_err())});
            }
            if par.state == ParseState::ExpectingRowStart {
                //cols done break
                return Box::new(Iter {
                    par,
                    reader: y,
                    // last_row_ref: None,
                })
            }
        }
        Box::new(NoneIter {
            __data: PhantomData
        })
        
    }

    fn get_cols(&self) -> Result<Vec<ColumnEntry>, TableLikeError> {
        let f = &mut &self.inner;
        f.rewind()?;
        let rd = BufReader::new(f);
        let mut par = TableParser {
            table: Table::default(),
            state: Default::default(),
            buffer: Vec::new(),
        };
        let mut y = rd.lines();
        while let Some(Ok(lt)) = y.next() {
            if par.next(lt).is_ok() && par.state == ParseState::ExpectingRowStart {
                //cols done break
                break;
            }
        }
        Ok(par.table.col_names)
    }

    fn add_rows(&mut self, rows: &mut dyn Iterator<Item=TableEntry>) -> Result<(), TableLikeError>{
        //no checks here it is the responsibility of the caller for sanity checks
        self.inner.seek(std::io::SeekFrom::End(0))?;
        let mut wri = &mut BufWriter::new(&self.inner);
        for row in rows {
            writeln!(&mut wri, "RStart")?;
            for row_col in &row.col_data {
                writeln!(&mut wri, "\"{row_col}\"")?;
            }
            writeln!(&mut wri, "REnd")?;
        }
        wri.flush()?;
        Ok(())
    }

    fn move_to_file(&mut self, _: &str) -> Result<FileTable, TableLikeError> {
        Err(TableLikeError::new("Already a File Table"))
    }

    fn move_to_memory(&mut self) -> Result<Table, TableLikeError> {
        let mut f = Table::default();
        f.flush(Box::new(self))?;
        Ok(f)
    }
}

impl FileTable {
    pub fn print_table(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), TableLikeError> {
        //create tmp Table object and keep piping output
        //TODO: bug here introduce some way of memoizing column widths of blocks that haven't been parsed yet to get accurate col info
        let mut t = Table::default();
        t.col_names.extend(self.get_cols()?);
        let mut c = 0;
        let mut first = true;
        let mut sizes = Vec::new();
        (0..t.col_names.len()).for_each(|_| sizes.push(0));
        for r in self.get_rows() {
            t.all.push(r?);   
            c += 1;
            if c > MAX_MEM_LIM {
                //print
                t.print_table(f, &mut sizes, if first {PrintOption::StartTable} else {PrintOption::MidTable})?;
                first = false;
                "\n".fmt(f)?;
                c = 0;
                t.all.clear();
            }
        }
        if !t.all.is_empty() {
            t.print_table(f, &mut sizes, if first {PrintOption::FullTable} else {PrintOption::EndTable})?;
        }
        Ok(())
    }
}

struct NoneIter<T> {
    __data: PhantomData<T>
}

impl<T> Iterator for NoneIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

struct ErrIter {
    err: Option<TableLikeError>,
}

impl Iterator for ErrIter {
    type Item = Result<TableEntry, TableLikeError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.err.take().map(Err)
    }
}

pub struct Iter<R> {
    par: TableParser,
    reader: Lines<BufReader<R>>,
}

impl<R> Iterator for Iter< R> where R: Read{
    type Item = Result<TableEntry, TableLikeError>;
    
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(Ok(st)) = self.reader.next() {
            if self.par.next(st).is_ok() && self.par.state == ParseState::ExpectingRowStart {
                //row ended safe to return
                return Some(self.par.table.all.pop().ok_or(TableLikeError::new("No Row found")));
            }
        } 
        None
    }
}


struct TableManager {
    tables: HashMap<String, Box<dyn TableLike>> 
}

impl TableManager {

    pub fn new() -> TableManager {
        TableManager { tables: HashMap::new() }
    }

    pub fn insert(&mut self, nm: String, to_ins: Box<dyn TableLike>) {
        self.tables.insert(nm, to_ins);
    }

    pub fn insert_ft(&mut self, ft: FileTable) {
        self.tables.insert(ft.name.to_string(), Box::new(ft));
    }

    pub fn select(&mut self, stmt: (String, Criteria)) -> Result<Box<dyn TableLike>, TableLikeError> {
        //first check if we already have a record in tables
        //if we don't then insert in memory
        //TODO replace hashmap with LRU

        if !self.tables.contains_key(stmt.0.as_str()) {
            let ty = Box::new(FileTable::new(stmt.0.as_str())?);
            self.tables.insert(stmt.0.clone(), ty);
        } 

        let tb = self.tables.get(stmt.0.as_str()).unwrap().as_ref();

        
        //NOW try and fit result object in Table otherwise flush to file

        let mut rt = Table {
            name: None,
            all: Vec::new(),
            col_names: Vec::new(),
        };

        let ori_cols = tb.get_cols()?;

        let lookup = ori_cols
            .iter()
            .enumerate()
            .map(|(ind, s)| (&s.col_name, ind))
            .collect::<HashMap<_, _>>();
        
        for yt in &stmt.1.re {
            rt.col_names.push(ori_cols[lookup[&yt]].clone())
        }
        for ten in tb.get_rows() {
            let mut v = Vec::new();
            let mut d = Vec::new();
            let t = ten?;
            for cr in &stmt.1.cls.col_name {
                v.push(&t.col_data[lookup[cr]])
            }
            for crd in &stmt.1.re {
                d.push(t.col_data[lookup[crd]].clone())
            }
            if (stmt.1.cls.act_clo)(v.as_slice()) {
                rt.all.push(TableEntry { col_data: d });
                if rt.all.len() > MAX_MEM_LIM {
                    //pipe previous output to file now
                    //then for every batch keep piping
                }
            }
        }
        Ok(Box::new(rt))
    }

}

//     pub fn select(&mut self, cri: &Criteria) -> TableReference {
//         //assign col_name to col_index

//         if let TableKind::Memory(x, _) = &self.inner {
//             let mut rt = Table {
//                 name: None,
//                 all: Vec::new(),
//                 col_names: Vec::new(),
//             };
//             let lookup = x
//                 .col_names
//                 .iter()
//                 .enumerate()
//                 .map(|(ind, s)| (&s.col_name, ind))
//                 .collect::<HashMap<_, _>>();
//             for yt in &cri.re {
//                 rt.col_names.push(x.col_names[lookup[&yt]].clone())
//             }
//             for t in &x.all {
//                 let mut v = Vec::new();
//                 let mut d = Vec::new();
//                 for cr in &cri.cls.col_name {
//                     v.push(&t.col_data[lookup[cr]])
//                 }
//                 for crd in &cri.re {
//                     d.push(t.col_data[lookup[crd]].clone())
//                 }
//                 if (cri.cls.act_clo)(v.as_slice()) {
//                     rt.all.push(TableEntry { col_data: d })
//                 }
//             }
//             TableReference {
//                 inner: TableKind::Memory(rt, None),
//             }
//         } else {
//             //read and parse while collecting result
//             //make decision whether to commit in memory or store in tmp file and pipe output
//             // set memory limit var
//             // if exceeds then send to file otherwise keep in memory
//             // self.read_table();

//             todo!()
//         }
//     }
// }

#[derive(Clone, Debug, PartialEq)]
pub enum TableCell {
    Num(Option<NumType>),
    Str(Option<StringType>),
}

impl TableCell {
    pub fn get_len(&self) -> usize {
        match &self {
            Self::Num(Some(t)) => t.to_string().len(),
            Self::Str(Some(t)) => t.len(),
            _ => "NULL".len(),
        }
    }
}

impl Display for TableCell {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self {
            Self::Num(Some(t)) => t.fmt(f),
            Self::Str(Some(t)) => t.fmt(f),
            _ => "NULL".fmt(f),
        }
    }
}

fn select<T>(t_ref: Vec<&mut TableReference>, criteria: &Criteria) -> Table {
    todo!()
}

#[derive(Parser)]
#[grammar = "sql_gram.pest"]
struct SQLParser;

fn main() {
    let mut f1 = FileTable::new("asd").expect("pt1");
    println!("{}", f1.move_to_memory().expect("could not convert"));
    println!("{:?}", f1.get_cols());
    println!("{:?}", f1.get_rows().collect::<Vec<_>>());
    println!("{f1}");
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
    // let mut tb = get_table("asd").expect("Shit happens");

    let mut t1 = f1;

    let t2 = 
            Table {
                name: None,
                col_names: vec,
                all: vec![
                    TableEntry {
                        col_data: vec![
                            TableCell::Num(Some(55)),
                            TableCell::Str(Some("hdhdh".to_string())),
                        ],
                    },
                    TableEntry {
                        col_data: vec![TableCell::Num(Some(46)), TableCell::Str(None)],
                    },
                ],
            };

    let mut tm = TableManager::new();
    let mut r1row = t2.get_rows().map(|f| f.unwrap());
    // t1.add_rows(&mut r1row).expect("SHHHHHIIIITT");
    println!("{t1}");
    tm.insert_ft(t1);
    let cri1 = Criteria {
        cls: Closure {
            col_name: vec!["Hei".to_string()],
            act_clo:Box::new(|f| {
                if let TableCell::Num(Some(rt)) = f[0] {
                    rt > &30
                } else {
                    false
                }
            }),
        },
        re: vec!["asd".to_string()],
    };
    let ret1 = tm.select(("asd".to_string(), cri1)).expect("fuck");
    println!("{ret1}");
    println!("Hello, world!");

    let pairs =
        SQLParser::parse(Rule::sql, "SELECT Hei, asd from asd;").expect("Something happened");

    for pair in pairs {
        let mut stmt: (String, Criteria) = (
            "".to_string(),
            Criteria {
                cls: Closure {
                    col_name: Vec::new(),
                    act_clo: Box::new(|_| true),
                },
                re: Vec::new(),
            },
        );
        for tokens in pair.into_inner() {
            println!("{:?}", tokens.as_rule());
            match tokens.as_rule() {
                Rule::clauses => {
                    for tk in tokens.into_inner() {
                        match tk.as_rule() {
                            Rule::select_clause => {
                                let se = tk.into_inner().next().unwrap();
                                let cols = se.as_str().to_string();
                                stmt.1.re.append(
                                    &mut cols.split(',').map(|f| f.trim().to_string()).collect(),
                                );
                            }
                            Rule::from_clause => {
                                let se = tk.into_inner().next().unwrap();
                                stmt.0 = se.as_str().to_string();
                            }
                            _ => {}
                        }
                    }
                }
                _ => {}
            }
        }
        let ret2 = tm.select(stmt).expect("ty");
        println!("{ret2}");
    }
}
