use axion_data::df;
use axion_data::error::{AxionResult, AxionError};
use axion_data::dataframe::DataFrame;
use axion_data::dtype::DataType;
use axion_data::series::Series;

#[test]
fn test_groupby_count_single_key_string() -> AxionResult<()> {
    let df = df![
        "group" => &["a", "b", "a", "b", "a", "c"],
        "value" => &[1, 2, 3, 4, 5, 6]
    ]?;

    let grouped_df = df.groupby(&["group"])?.count()?;
    let expected_df = df![
        "group" => &["a", "b", "c"],
        "count" => &[3_u32, 2_u32, 1_u32]
    ]?.sort(&["group"], &[false])?;

    let sorted_grouped_df = grouped_df.sort(&["group"], &[false])?;

    assert_eq!(sorted_grouped_df.shape(), (3, 2));
    assert_eq!(sorted_grouped_df.columns_names(), vec!["group", "count"]);
    assert_eq!(sorted_grouped_df.column("group")?.dtype(), DataType::String);
    assert_eq!(sorted_grouped_df.column("count")?.dtype(), DataType::UInt32);
    assert_eq!(sorted_grouped_df, expected_df);

    Ok(())
}

#[test]
fn test_groupby_count_single_key_int() -> AxionResult<()> {
    let df = df![
        "id" => &[1, 2, 1, 2, 1, 3],
        "value" => &["a", "b", "c", "d", "e", "f"]
    ]?;

    let grouped_df = df.groupby(&["id"])?.count()?;
    let expected_df = df![
        "id" => &[1, 2, 3],
        "count" => &[3_u32, 2_u32, 1_u32]
    ]?.sort(&["id"], &[false])?;

    let sorted_grouped_df = grouped_df.sort(&["id"], &[false])?;

    assert_eq!(sorted_grouped_df.shape(), (3, 2));
    assert_eq!(sorted_grouped_df.columns_names(), vec!["id", "count"]);
    assert_eq!(sorted_grouped_df, expected_df);

    Ok(())
}

#[test]
fn test_groupby_count_multi_keys() -> AxionResult<()> {
    let df = df![
        "key1" => &["a", "a", "b", "b", "a", "b"],
        "key2" => &[1, 2, 1, 1, 1, 2],
        "value" => &[10, 20, 30, 40, 50, 60]
    ]?;

    let grouped_df = df.groupby(&["key1", "key2"])?.count()?;
    let expected_df = df![
        "key1" => &["a", "a", "b", "b"],
        "key2" => &[1, 2, 1, 2],
        "count" => &[2_u32, 1_u32, 2_u32, 1_u32]
    ]?.sort(&["key1", "key2"], &[false, false])?;

    let sorted_grouped_df = grouped_df.sort(&["key1", "key2"], &[false, false])?;

    assert_eq!(sorted_grouped_df.shape(), (4, 3));
    assert_eq!(sorted_grouped_df.columns_names(), vec!["key1", "key2", "count"]);
    assert_eq!(sorted_grouped_df, expected_df);

    Ok(())
}

#[test]
fn test_groupby_count_with_nulls_in_key() -> AxionResult<()> {
    let group_data: Vec<Option<String>> = vec![Some("a".into()), None, Some("a".into()), Some("b".into()), None];
    let df = df![
        "group" => group_data,
        "value" => &[1, 2, 3, 4, 5]
    ]?;

    let grouped_df = df.groupby(&["group"])?.count()?;
    let expected_df = df![
        "group" => &["a", "b"],
        "count" => &[2_u32, 1_u32]
    ]?.sort(&["group"], &[false])?;

    let sorted_grouped_df = grouped_df.sort(&["group"], &[false])?;

    assert_eq!(sorted_grouped_df.shape(), (2, 2));
    assert_eq!(sorted_grouped_df, expected_df);

    Ok(())
}

#[test]
fn test_groupby_count_empty_df() -> AxionResult<()> {
    let df = DataFrame::new(vec![
        Box::new(Series::<String>::new_empty("group".into(), DataType::String)),
        Box::new(Series::<i32>::new_empty("value".into(), DataType::Int32)),
    ])?;

    let grouped_df = df.groupby(&["group"])?.count()?;
    let expected_df = DataFrame::new(vec![
        Box::new(Series::<String>::new_empty("group".into(), DataType::String)),
        Box::new(Series::<u32>::new_empty("count".into(), DataType::UInt32)),
    ])?;

    assert_eq!(grouped_df.shape(), (0, 2));
    assert_eq!(grouped_df.columns_names(), vec!["group", "count"]);
    assert_eq!(grouped_df, expected_df);

    Ok(())
}

#[test]
fn test_groupby_error_handling() -> AxionResult<()> {
    let df = df!["a" => &[1, 2]]?;
    
    // 不存在的列
    match df.groupby(&["nonexistent"]) {
        Err(AxionError::ColumnNotFound(col)) if col == "nonexistent" => {}
        _ => panic!("Expected ColumnNotFound error"),
    }

    // 不支持的类型
    let df_float = df![
        "group": f64 => vec![1.1, 2.2, 1.1],
        "value": i64 => vec![1, 2, 3]
    ]?;
    match df_float.groupby(&["group"]) {
        Err(AxionError::UnsupportedOperation(msg)) if msg.contains("Float64") => {}
        _ => panic!("Expected UnsupportedOperation error"),
    }

    Ok(())
}

#[test]
fn test_groupby_sum_comprehensive() -> AxionResult<()> {
    let df = df![
        "key": String => &["a", "b", "a", "b", "a", "b"],
        "val_i32": i32 => vec![1, 2, 3, 4, 5, 6],
        "val_f64": f64 => vec![1.1, 2.2, 3.3, 4.4, 5.5, 6.6],
        "val_nulls": i32 => vec![Some(10), None, Some(30), Some(40), None, Some(60)],
        "non_numeric": String => &["x", "y", "z", "p", "q", "r"]
    ]?;

    let summed_df = df.groupby(&["key"])?.sum()?;
    let expected_df = df![
        "key": String => &["a", "b"],
        "val_i32": i32 => &[9_i32, 12_i32],
        "val_f64": f64 => &[9.9_f64, 13.2_f64],
        "val_nulls": i32 => vec![Some(40_i32), Some(100_i32)]
    ]?.sort(&["key"], &[false])?;

    let sorted_summed_df = summed_df.sort(&["key"], &[false])?;

    assert_eq!(sorted_summed_df.shape(), (2, 4));
    assert_eq!(sorted_summed_df.columns_names(), vec!["key", "val_i32", "val_f64", "val_nulls"]);
    assert_eq!(sorted_summed_df, expected_df);
    Ok(())
}

#[test]
fn test_groupby_sum_multi_keys() -> AxionResult<()> {
    let df = df![
        "key1" => &["a", "a", "b", "b", "a", "b"],
        "key2" => &[1, 2, 1, 1, 1, 2],
        "value" => &[10, 20, 30, 40, 50, 60]
    ]?;

    let summed_df = df.groupby(&["key1", "key2"])?.sum()?;
    let expected_df = df![
        "key1" => &["a", "a", "b", "b"],
        "key2" => &[1, 2, 1, 2],
        "value" => &[60_i32, 20_i32, 70_i32, 60_i32]
    ]?.sort(&["key1", "key2"], &[false, false])?;

    let sorted_summed_df = summed_df.sort(&["key1", "key2"], &[false, false])?;

    assert_eq!(sorted_summed_df.shape(), (4, 3));
    assert_eq!(sorted_summed_df, expected_df);
    Ok(())
}

#[test]
fn test_groupby_sum_with_nulls_and_nan() -> AxionResult<()> {
    // 测试包含空值的分组求和
    let df_nulls = df![
        "key": String => &["a", "b", "a", "b"],
        "val": i32 => vec![Some(1), None, Some(3), None]
    ]?;

    let summed_df = df_nulls.groupby(&["key"])?.sum()?;
    let expected_df = df![
        "key": String => &["a", "b"],
        "val": i32 => vec![Some(4), None]
    ]?.sort(&["key"], &[false])?;

    let sorted_summed_df = summed_df.sort(&["key"], &[false])?;
    assert_eq!(sorted_summed_df, expected_df);

    // 测试包含NaN的分组求和
    let df_nan = df![
        "key": String => &["a", "b", "a", "b", "a"],
        "val": f64 => vec![1.0, 2.0, f64::NAN, 4.0, 5.0]
    ]?;

    let summed_nan_df = df_nan.groupby(&["key"])?.sum()?;
    let expected_nan_df = df![
        "key": String => &["a", "b"],
        "val": f64 => &[6.0_f64, 6.0_f64] // NaN被忽略
    ]?.sort(&["key"], &[false])?;

    let sorted_summed_nan_df = summed_nan_df.sort(&["key"], &[false])?;

    // 比较key列
    let key_col_actual = sorted_summed_nan_df.column("key")?.as_any().downcast_ref::<Series<String>>().unwrap();
    let key_col_expected = expected_nan_df.column("key")?.as_any().downcast_ref::<Series<String>>().unwrap();
    assert_eq!(key_col_actual.data, key_col_expected.data);

    // 比较val列
    let val_col_actual = sorted_summed_nan_df.column("val")?.as_any().downcast_ref::<Series<f64>>().unwrap();
    let val_col_expected = expected_nan_df.column("val")?.as_any().downcast_ref::<Series<f64>>().unwrap();
    assert_eq!(val_col_actual.data, val_col_expected.data);

    Ok(())
}

#[test]
fn test_groupby_sum_empty_df() -> AxionResult<()> {
    let df = DataFrame::new(vec![
        Box::new(Series::<String>::new_empty("key".into(), DataType::String)),
        Box::new(Series::<i32>::new_empty("val".into(), DataType::Int32)),
    ])?;

    let summed_df = df.groupby(&["key"])?.sum()?;
    let expected_df = DataFrame::new(vec![
        Box::new(Series::<String>::new_empty("key".into(), DataType::String)),
        Box::new(Series::<i32>::new_empty("val".into(), DataType::Int32)),
    ])?;

    assert_eq!(summed_df.shape(), (0, 2));
    assert_eq!(summed_df, expected_df);
    Ok(())
}

#[test]
fn test_groupby_mean_comprehensive() -> AxionResult<()> {
    let df = df![
        "key": String => &["a", "b", "a", "b", "a", "b"],
        "val_i32": i32 => vec![1, 2, 3, 4, 5, 6],
        "val_f64": f64 => vec![1.1, 2.2, 3.3, 4.4, 5.5, 6.6],
        "val_nulls": i32 => vec![Some(10), None, Some(30), Some(40), None, Some(60)],
        "non_numeric": String => &["x", "y", "z", "p", "q", "r"]
    ]?;

    let mean_df = df.groupby(&["key"])?.mean()?;
    let expected_df = df![
        "key": String => &["a", "b"],
        "val_i32": f64 => &[3.0, 4.0],
        "val_f64": f64 => &[3.3, 4.4],
        "val_nulls": f64 => &[20.0, 50.0]
    ]?.sort(&["key"], &[false])?;

    let sorted_mean_df = mean_df.sort(&["key"], &[false])?;

    assert_eq!(sorted_mean_df.shape(), (2, 4));
    assert_eq!(sorted_mean_df.columns_names(), vec!["key", "val_i32", "val_f64", "val_nulls"]);
    assert_eq!(sorted_mean_df, expected_df);

    Ok(())
}

#[test]
fn test_groupby_mean_with_nulls_and_nan() -> AxionResult<()> {
    // 测试包含空值和NaN的分组平均值
    let df = df![
        "key": String => &["a", "b", "a", "b", "a", "b", "a"],
        "val": f64 => vec![Some(1.0), Some(2.0), None, Some(f64::NAN), Some(5.0), Some(6.0), Some(f64::NAN)]
    ]?;

    let mean_df = df.groupby(&["key"])?.mean()?;
    let expected_df = df![
        "key": String => &["a", "b"],
        "val": f64 => &[3.0_f64, 4.0_f64] // None和NaN被忽略
    ]?.sort(&["key"], &[false])?;

    let sorted_mean_df = mean_df.sort(&["key"], &[false])?;

    assert_eq!(sorted_mean_df.shape(), (2, 2));
    assert_eq!(sorted_mean_df.columns_names(), vec!["key", "val"]);
    assert_eq!(sorted_mean_df, expected_df);

    Ok(())
}

#[test]
fn test_groupby_mean_all_nulls_in_group() -> AxionResult<()> {
    let df = df![
        "key": String => &["a", "b", "a", "b"],
        "val": i32 => vec![Some(1), None, Some(3), None]
    ]?;

    let mean_df = df.groupby(&["key"])?.mean()?;
    let expected_df = df![
        "key": String => &["a", "b"],
        "val": f64 => vec![Some(2.0), None]
    ]?.sort(&["key"], &[false])?;

    let sorted_mean_df = mean_df.sort(&["key"], &[false])?;

    assert_eq!(sorted_mean_df.shape(), (2, 2));
    assert_eq!(sorted_mean_df.columns_names(), vec!["key", "val"]);
    assert_eq!(sorted_mean_df, expected_df);

    Ok(())
}

#[test]
fn test_groupby_mean_empty_df() -> AxionResult<()> {
    let df = DataFrame::new(vec![
        Box::new(Series::<String>::new_empty("key".to_string(), DataType::String)),
        Box::new(Series::<i32>::new_empty("value".to_string(), DataType::Int32)),
    ])?;

    let mean_df = df.groupby(&["key"])?.mean()?;
    let expected_df = DataFrame::new(vec![
        Box::new(Series::<String>::new_empty("key".to_string(), DataType::String)),
        Box::new(Series::<f64>::new_empty("value".to_string(), DataType::Float64)),
    ])?;

    assert_eq!(mean_df.shape(), (0, 2));
    assert_eq!(mean_df.columns_names(), vec!["key", "value"]);
    assert_eq!(mean_df, expected_df);

    Ok(())
}