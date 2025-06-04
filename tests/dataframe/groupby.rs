// --- 手动导入所有需要的项 --- 喵
use axion::df; // 导入 df! 宏
use axion::error::{AxionResult, AxionError}; // 从 error 模块导入
use axion::dataframe::DataFrame; // 从 dataframe 模块导入
use axion::dtype::DataType; // 从 dtype 模块导入
use axion::series::Series; // 从 series 模块导入
// GroupBy 本身在测试代码中没有直接使用其类型，所以不一定需要导入

// --- 添加 num_traits::Float 用于 is_nan ---
// use num_traits::Float;

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
    assert_eq!(sorted_grouped_df.column("id")?.dtype(), DataType::Int32);
    assert_eq!(sorted_grouped_df.column("count")?.dtype(), DataType::UInt32);
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
    assert_eq!(sorted_grouped_df.column("key1")?.dtype(), DataType::String);
    assert_eq!(sorted_grouped_df.column("key2")?.dtype(), DataType::Int32);
    assert_eq!(sorted_grouped_df.column("count")?.dtype(), DataType::UInt32);
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
    let empty_group_series = Series::<String>::new_empty("group".into(), DataType::String);
    let empty_value_series = Series::<i32>::new_empty("value".into(), DataType::Int32);

    let df = DataFrame::new(vec![
        Box::new(empty_group_series),
        Box::new(empty_value_series),
    ])?;

    let grouped_df = df.groupby(&["group"])?.count()?;

    let expected_empty_group = Series::<String>::new_empty("group".into(), DataType::String);
    let expected_empty_count = Series::<u32>::new_empty("count".into(), DataType::UInt32);
    let expected_df = DataFrame::new(vec![
        Box::new(expected_empty_group),
        Box::new(expected_empty_count),
    ])?;

    assert_eq!(grouped_df.shape(), (0, 2));
    assert_eq!(grouped_df.columns_names(), vec!["group", "count"]);
    assert_eq!(grouped_df.column("group")?.dtype(), DataType::String);
    assert_eq!(grouped_df.column("count")?.dtype(), DataType::UInt32);
    assert_eq!(grouped_df, expected_df);

    Ok(())
}

#[test]
fn test_groupby_nonexistent_key() -> AxionResult<()> {
    let df = df!["a" => &[1, 2]]?;
    match df.groupby(&["nonexistent"]) {
        Err(AxionError::ColumnNotFound(col)) if col == "nonexistent" => {
            Ok(())
        }
        Ok(_) => panic!("Expected ColumnNotFound error, but got Ok"),
        Err(e) => panic!("Expected ColumnNotFound error, but got {:?}", e),
    }
}

#[test]
fn test_groupby_unsupported_key_type() -> AxionResult<()> {
    let df = df![
        "group": f64 => vec![1.1, 2.2, 1.1],
        "value": i64 => vec![1, 2, 3]
    ]?;
    match df.groupby(&["group"]) {
        Err(AxionError::UnsupportedOperation(msg)) if msg.contains("Float64") && msg.contains("group") => {
            Ok(())
        }
        Ok(_) => panic!("Expected UnsupportedOperation error, but got Ok"),
        Err(e) => panic!("Expected UnsupportedOperation error, but got {:?}", e),
    }
}

#[test]
fn test_groupby_sum_single_key() -> AxionResult<()> {
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
fn test_groupby_sum_empty_df() -> AxionResult<()> {
    let empty_key_series = Series::<String>::new_empty("key".into(), DataType::String);
    let empty_value_series = Series::<i32>::new_empty("val".into(), DataType::Int32);
    let df = DataFrame::new(vec![
        Box::new(empty_key_series),
        Box::new(empty_value_series),
    ])?;

    let summed_df = df.groupby(&["key"])?.sum()?;

    let expected_empty_key = Series::<String>::new_empty("key".into(), DataType::String);
    let expected_empty_val = Series::<i32>::new_empty("val".into(), DataType::Int32);
    let expected_df = DataFrame::new(vec![
        Box::new(expected_empty_key),
        Box::new(expected_empty_val),
    ])?;

    assert_eq!(summed_df.shape(), (0, 2));
    assert_eq!(summed_df.columns_names(), vec!["key", "val"]);
    assert_eq!(summed_df, expected_df);
    Ok(())
}

#[test]
fn test_groupby_sum_all_nulls_in_group() -> AxionResult<()> {
    let df = df![
        "key": String => &["a", "b", "a", "b"],
        "val": i32 => vec![Some(1), None, Some(3), None]
    ]?;

    let summed_df = df.groupby(&["key"])?.sum()?;

    let expected_df = df![
        "key": String => &["a", "b"],
        "val": i32 => vec![Some(4), None]
    ]?.sort(&["key"], &[false])?;

    let sorted_summed_df = summed_df.sort(&["key"], &[false])?;

    assert_eq!(sorted_summed_df, expected_df);
    Ok(())
}

#[test]
fn test_groupby_sum_with_nan() -> AxionResult<()> {
    let df = df![
        "key": String => &["a", "b", "a", "b", "a"],
        "val": f64 => vec![1.0, 2.0, f64::NAN, 4.0, 5.0]
    ]?;

    let summed_df = df.groupby(&["key"])?.sum()?;

    let expected_df = df![
        "key": String => &["a", "b"],
        "val": f64 => &[6.0_f64, 6.0_f64] // NaN is ignored in sum
    ]?.sort(&["key"], &[false])?;

    let sorted_summed_df = summed_df.sort(&["key"], &[false])?;

    assert_eq!(sorted_summed_df.shape(), (2, 2));

    // --- 比较 key 列的数据 --- 喵
    let key_col_actual = sorted_summed_df.column("key")?.as_any().downcast_ref::<Series<String>>().unwrap();
    let key_col_expected = expected_df.column("key")?.as_any().downcast_ref::<Series<String>>().unwrap();
    assert_eq!(key_col_actual.data, key_col_expected.data);

    // --- 比较 val 列的数据 --- 喵
    let val_col_actual = sorted_summed_df.column("val")?.as_any().downcast_ref::<Series<f64>>().unwrap();
    let val_col_expected = expected_df.column("val")?.as_any().downcast_ref::<Series<f64>>().unwrap();
    // 直接比较 Vec<Option<f64>>。注意：如果包含 NaN，NaN != NaN 会导致比较失败。
    // 如果需要 NaN == NaN 的比较，需要自定义比较逻辑。
    // 但在这个测试中，预期结果不包含 NaN，所以直接比较应该可行。
    assert_eq!(val_col_actual.data, val_col_expected.data);

    Ok(())
}

#[test]
fn test_groupby_mean_single_key() -> AxionResult<()> {
    let df = df![
        "key": String => &["a", "b", "a", "b", "a", "b"],
        "val_i32": i32 => vec![1, 2, 3, 4, 5, 6],
        "val_f64": f64 => vec![1.1, 2.2, 3.3, 4.4, 5.5, 6.6],
        "val_nulls": i32 => vec![Some(10), None, Some(30), Some(40), None, Some(60)],
        "non_numeric": String => &["x", "y", "z", "p", "q", "r"]
    ]?;

    let mean_df = df.groupby(&["key"])?.mean()?;

    // 预期结果：
    // key a: i32(1,3,5) -> mean 3.0; f64(1.1,3.3,5.5) -> mean 3.3; nulls(10,30) -> mean 20.0
    // key b: i32(2,4,6) -> mean 4.0; f64(2.2,4.4,6.6) -> mean 4.4; nulls(40,60) -> mean 50.0
    let expected_df = df![
        "key": String => &["a", "b"],
        "val_i32": f64 => &[3.0, 4.0], // 结果是 f64
        "val_f64": f64 => &[3.3, 4.4],
        "val_nulls": f64 => &[20.0, 50.0] // 结果是 f64
    ]?.sort(&["key"], &[false])?;

    let sorted_mean_df = mean_df.sort(&["key"], &[false])?;

    assert_eq!(sorted_mean_df.shape(), (2, 4));
    assert_eq!(sorted_mean_df.columns_names(), vec!["key", "val_i32", "val_f64", "val_nulls"]);

    // --- 尝试直接比较 DataFrame (可能因浮点数失败) --- 喵
    // 如果失败，请取消注释下面的逐列比较代码
    assert_eq!(sorted_mean_df, expected_df);

    /*
    // --- 逐列比较 (处理浮点数) ---
    let keys_actual = sorted_mean_df.column("key")?.as_any().downcast_ref::<Series<String>>().unwrap();
    let keys_expected = expected_df.column("key")?.as_any().downcast_ref::<Series<String>>().unwrap();
    assert_eq!(keys_actual.data, keys_expected.data);

    let epsilon = 1e-9;

    let val_i32_actual = sorted_mean_df.column("val_i32")?.as_any().downcast_ref::<Series<f64>>().unwrap();
    assert!((val_i32_actual.get(0).unwrap().unwrap() - 3.0).abs() < epsilon);
    assert!((val_i32_actual.get(1).unwrap().unwrap() - 4.0).abs() < epsilon);

    let val_f64_actual = sorted_mean_df.column("val_f64")?.as_any().downcast_ref::<Series<f64>>().unwrap();
    assert!((val_f64_actual.get(0).unwrap().unwrap() - 3.3).abs() < epsilon);
    assert!((val_f64_actual.get(1).unwrap().unwrap() - 4.4).abs() < epsilon);

    let val_nulls_actual = sorted_mean_df.column("val_nulls")?.as_any().downcast_ref::<Series<f64>>().unwrap();
    assert!((val_nulls_actual.get(0).unwrap().unwrap() - 20.0).abs() < epsilon);
    assert!((val_nulls_actual.get(1).unwrap().unwrap() - 50.0).abs() < epsilon);
    */

    Ok(())
}

#[test]
fn test_groupby_mean_with_nulls_and_nan() -> AxionResult<()> {
    let df = df![
        "key": String => &["a", "b", "a", "b", "a", "b", "a"],
        "val": f64 => vec![Some(1.0), Some(2.0), None, Some(f64::NAN), Some(5.0), Some(6.0), Some(f64::NAN)]
    ]?;

    let mean_df = df.groupby(&["key"])?.mean()?;

    // 预期结果：
    // key a: (1.0, 5.0) -> mean 3.0 (None 和 NaN 被忽略)
    // key b: (2.0, 6.0) -> mean 4.0 (NaN 被忽略)
     let expected_df = df![
        "key": String => &["a", "b"],
        "val": f64 => &[3.0_f64, 4.0_f64]
    ]?.sort(&["key"], &[false])?;

     let sorted_mean_df = mean_df.sort(&["key"], &[false])?;

     assert_eq!(sorted_mean_df.shape(), (2, 2));
     assert_eq!(sorted_mean_df.columns_names(), vec!["key", "val"]);
     // --- 尝试直接比较 (可能因浮点数失败) ---
     assert_eq!(sorted_mean_df, expected_df);

    Ok(())
}

#[test]
fn test_groupby_mean_all_nulls_in_group() -> AxionResult<()> {
    let df = df![
        "key": String => &["a", "b", "a", "b"],
        "val": i32 => vec![Some(1), None, Some(3), None] // Group 'b' has only None
    ]?;

    let mean_df = df.groupby(&["key"])?.mean()?;

    // 预期结果：
    // key a: (1, 3) -> mean 2.0
    // key b: (None) -> mean None
    let expected_df = df![
        "key": String => &["a", "b"],
        "val": f64 => vec![Some(2.0), None] // 结果是 f64
    ]?.sort(&["key"], &[false])?;

    let sorted_mean_df = mean_df.sort(&["key"], &[false])?;

    assert_eq!(sorted_mean_df.shape(), (2, 2));
    assert_eq!(sorted_mean_df.columns_names(), vec!["key", "val"]);
    // --- 尝试直接比较 (可能因浮点数失败) ---
    assert_eq!(sorted_mean_df, expected_df);

    Ok(())
}

#[test]
fn test_groupby_mean_empty_df() -> AxionResult<()> {
    // 创建一个空的 DataFrame，但需要指定列类型以便 mean 知道哪些是数值列
    let df = DataFrame::new(vec![
        Box::new(Series::<String>::new_empty("key".to_string(), DataType::String)),
        Box::new(Series::<i32>::new_empty("value".to_string(), DataType::Int32)),
    ])?;

    let mean_df = df.groupby(&["key"])?.mean()?;

    // 预期结果：空的 DataFrame，包含 key 列和 value 列 (类型为 f64)
    let expected_df = DataFrame::new(vec![
        Box::new(Series::<String>::new_empty("key".to_string(), DataType::String)),
        Box::new(Series::<f64>::new_empty("value".to_string(), DataType::Float64)), // Mean 结果是 f64
    ])?;

    assert_eq!(mean_df.shape(), (0, 2));
    assert_eq!(mean_df.columns_names(), vec!["key", "value"]);
    assert_eq!(mean_df, expected_df);

    Ok(())
}