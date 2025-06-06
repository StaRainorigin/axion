use axion_data::Series;

fn main() {
    let s1 = Series::new("nihao".into(), [1, 2, 3, 4, 5]);
    println!("Series: {}", s1);
}