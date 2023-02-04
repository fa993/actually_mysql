use crate::TableCell;

pub struct Criteria {
    // closues that take col values and eval against given col
    pub cls: Closure,
    pub re: Vec<String>,
}

pub struct Closure {
    pub col_name: Vec<String>,
    pub act_clo: Box<dyn Fn(&[&TableCell]) -> bool>,
}

impl Closure {
    fn and(mut a: Closure, mut b: Closure) -> Closure {
        let frs = a.col_name.len();
        let sec = b.col_name.len();
        let mut r = Vec::with_capacity(frs + sec);
        r.append(&mut a.col_name);
        r.append(&mut b.col_name);
        Closure {
            col_name: r,
            act_clo: Box::new(move |v| {
                (a.act_clo)(&v[0..frs]) && (b.act_clo)(&v[frs + 1..(frs + sec)])
            }),
        }
    }

    fn or(mut a: Closure, mut b: Closure) -> Closure {
        let frs = a.col_name.len();
        let sec = b.col_name.len();
        let mut r = Vec::with_capacity(frs + sec);
        r.append(&mut a.col_name);
        r.append(&mut b.col_name);
        Closure {
            col_name: r,
            act_clo: Box::new(move |v| {
                (a.act_clo)(&v[0..frs]) || (b.act_clo)(&v[frs + 1..(frs + sec)])
            }),
        }
    }
}
