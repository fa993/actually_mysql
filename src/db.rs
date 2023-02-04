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

use crate::{ColumnEntry, Num_type, Table, TableCell, TableEntry, NUM_BASE};

#[derive(Default)]
pub struct TableParser {
    pub table: Table,
    state: ParseState,
}

impl TableParser {
    pub fn next(&mut self, mut inp: String) -> Result<(), String> {
        let next_s = self.state.next(inp.as_str())?;
        let mut buffer = Vec::<String>::new();

        if inp.as_str() == "ColDescEnd" {
            //finished parsing columns
            let r = PairIter {
                inner: buffer.iter(),
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
            buffer.push(inp);
        } else if inp == "REnd" {
            //buffer has row items
            //so make an entry and append to them
            if buffer.len() != self.table.col_names.len() {
                return Err("Incorrect table Length".to_string());
            }

            let mut row = TableEntry {
                col_data: Vec::new(),
            };

            for (col, bu) in self.table.col_names.iter().zip(buffer.into_iter()) {
                //TODO handle NULL and EMPTY
                row.col_data.push(match col.col_type {
                    TableCell::Str(_) => Result::<TableCell, String>::Ok(TableCell::Str(Some(bu))),
                    TableCell::Num(_) => {
                        let ry = Num_type::from_str_radix(bu.as_str(), NUM_BASE).map_err(|_| {
                            format!("Failed to decode integer in base {}", NUM_BASE)
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

#[derive(PartialEq)]
enum ParseState {
    ExpectingColStart,
    ExpectingColName,
    ExpectingColValue,
    ExpectingRowStart,
    ExpectingCellValue,
}

impl ParseState {
    pub fn next(&self, inp: &str) -> Result<ParseState, String> {
        match &self {
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
            _ => Err("Syntax Error".to_string()),
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
