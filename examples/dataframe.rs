use axion_data::{series::new_list_series, DataFrame, AxionError, Series, SeriesTrait};

fn main() -> Result<(), AxionError> {
    let s0 = Series::new("a".into(), [1i64, 2, 3]);
    let s1 = Series::new("b".into(), [1i64, 1, 1]);
    let s2 = Series::new("c".into(), [2i64, 2, 2]);

    let series_to_box: Vec<Box<dyn SeriesTrait>> = vec![
        Box::new(s0),
        Box::new(s1),
        Box::new(s2),
    ];

    // 在调用时指定一个类型参数，例如 ::<()>
    let list_series = new_list_series("list_col".into(), series_to_box)?;

    let df_s0 = Series::new("B".into(), [1, 2, 3]);
    let df_s1 = Series::new("C".into(), [1, 1, 1]);

    let df = DataFrame::new(vec![
        Box::new(df_s0),
        Box::new(df_s1),
        Box::new(list_series),
    ])?;

    println!("{}", df);

    Ok(())
}