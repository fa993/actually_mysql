/*

File structure
table name is file name
encoding is UTF-8

ColDescStart
"<ColName>"
"<ColType>" either String or Num
ColDescEnd

RStart
"<Data>"
...
REnd

*/

use std::slice::Iter;

use const_format::concatcp;

use crate::{ColumnEntry, Num_type, Table, TableCell, TableEntry, NUM_BASE};

pub struct TableParser<'a> {
    pub table: &'a mut Table,
    pub state: ParseState,
    pub buffer: Vec<String>,
}

#[derive(Debug)]
pub struct ParseError {
    message: String,
}

impl std::fmt::Display for ParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        String::fmt(&self.message, f)
    }
}

impl std::error::Error for ParseError {}

impl From<&str> for ParseError {
    fn from(a: &str) -> Self {
        ParseError {
            message: a.to_string(),
        }
    }
}

impl From<String> for ParseError {
    fn from(a: String) -> Self {
        ParseError { message: a }
    }
}

impl From<ParseError> for String {
    fn from(a: ParseError) -> Self {
        a.message
    }
}

// impl<T> From<Result<T, &str>> for ParseError {
//     fn from(a: Result<T, &str>) -> Self {
//         ParseError { message: () }
//     }
// }

impl<'a> TableParser<'a> {
    pub fn next(&mut self, mut inp: String) -> Result<(), &str> {
        let next_s = ParseState::next(&self.state, inp.as_str())?;

        if inp.as_str() == "ColDescEnd" {
            //finished parsing columns
            let r = PairIter {
                inner: self.buffer.iter(),
            };
            for (name, value_type) in r {
                self.table.col_names.push(ColumnEntry {
                    col_name: name.to_owned(),
                    col_type: match value_type.as_str() {
                        "String" => Ok(TableCell::Str(None)),
                        "Num" => Ok(TableCell::Num(None)),
                        _ => Err("Validation failed"),
                    }?,
                })
            }
        }

        if inp.ends_with('"') && inp.starts_with('"') {
            inp.pop();
            inp.remove(0);
            self.buffer.push(inp);
        } else if inp == "REnd" {
            //buffer has row items
            //so make an entry and append to them
            if self.buffer.len() != self.table.col_names.len() {
                return Err("Incorrect table Length");
            }

            let mut row = TableEntry {
                col_data: Vec::new(),
            };

            for (col, bu) in self.table.col_names.iter().zip(self.buffer.iter()) {
                //TODO handle NULL and EMPTY
                row.col_data.push(match col.col_type {
                    TableCell::Str(_) => {
                        Result::<TableCell, &str>::Ok(TableCell::Str(Some(bu.to_string())))
                    }
                    TableCell::Num(_) => {
                        let ry = Num_type::from_str_radix(bu.as_str(), NUM_BASE).map_err(|_| {
                            concatcp!("Failed to decode integer in base ", NUM_BASE)
                        })?;
                        Ok(TableCell::Num(Some(ry)))
                    }
                }?);
            }

            self.table.all.push(row);
        }

        self.state = next_s;
        Ok(())
    }
}

#[derive(PartialEq, Clone, Copy)]
pub enum ParseState {
    ExpectingColStart,
    ExpectingColName,
    ExpectingColValue,
    ExpectingRowStart,
    ExpectingCellValue,
}

impl ParseState {
    pub fn next(prev_state: &Self, inp: &str) -> Result<ParseState, &'static str> {
        match prev_state {
            Self::ExpectingColStart if inp == "ColDescStart" => Ok(Self::ExpectingColName),
            Self::ExpectingColName if inp.ends_with('"') && inp.starts_with('"') => {
                Ok(Self::ExpectingColValue)
            }
            Self::ExpectingColName if inp == "ColDescEnd" => Ok(Self::ExpectingRowStart),
            Self::ExpectingColValue
                if inp.ends_with('"')
                    && inp.starts_with('"')
                    && (inp == "\"String\"" || inp == "\"Num\"") =>
            {
                Ok(Self::ExpectingColName)
            }
            Self::ExpectingRowStart if inp == "RStart" => Ok(Self::ExpectingCellValue),
            Self::ExpectingCellValue if inp.ends_with('"') && inp.starts_with('"') => {
                Ok(Self::ExpectingCellValue)
            }
            Self::ExpectingCellValue if inp == "REnd" => Ok(Self::ExpectingRowStart),
            _ => Err("Syntax Error"),
        }
    }
}

impl Default for ParseState {
    fn default() -> Self {
        Self::ExpectingColStart
    }
}

//ISSUE: make better generic
struct PairIter<'a, T> {
    inner: Iter<'a, T>,
}

impl<'a, T> Iterator for PairIter<'a, T> {
    type Item = (&'a T, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        let a = self.inner.next();
        let b = self.inner.next();
        if a.is_some() && b.is_some() {
            Some((a.unwrap(), b.unwrap()))
        } else {
            None
        }
    }
}
