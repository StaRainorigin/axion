use axion_data::{DataType, Series, SeriesCompare, SeriesArithScalar, SeriesArithSeries};
use axion_data::error::{AxionError, AxionResult};

fn create_float_series_with_none() -> Series<f64> {
    Series::from((
        "float_series".to_string(),
        vec![Some(1.0), None, Some(3.5), Some(0.5)],
    ))
}

fn create_int_series() -> Series<i32> {
    Series::from((
        "int_series".to_string(),
        vec![Some(1), Some(2), Some(3), Some(4)],
    ))
}

#[test]
fn test_series_creation() {
    // 基本创建
    let s1 = Series::new("s1".into(), vec![1, 2, 3]);
    assert_eq!(s1.name(), "s1");
    assert_eq!(s1.dtype(), DataType::Int32);
    assert_eq!(s1.len(), 3);
    assert_eq!(s1.data_internal(), &vec![Some(1), Some(2), Some(3)]);

    let s2 = Series::new("s2".into(), [10.0f64, 20.0]);
    assert_eq!(s2.name(), "s2");
    assert_eq!(s2.dtype(), DataType::Float64);
    assert_eq!(s2.data_internal(), &vec![Some(10.0), Some(20.0)]);

    // From 创建
    let s3 = Series::from(("s3_from".to_string(), vec![true, false]));
    assert_eq!(s3.name(), "s3_from");
    assert_eq!(s3.dtype(), DataType::Bool);
    assert_eq!(s3.data_internal(), &vec![Some(true), Some(false)]);

    // 空Series创建
    let s_empty: Series<i32> = Series::new_empty("empty_int".into(), DataType::Int32);
    assert_eq!(s_empty.name(), "empty_int");
    assert_eq!(s_empty.dtype(), DataType::Int32);
    assert!(s_empty.is_empty());
    assert_eq!(s_empty.len(), 0);

    // 从空容器创建
    let empty_vec: Vec<i32> = Vec::new();
    let empty_series: Series<i32> = Series::new("empty_vec_series".into(), empty_vec);
    assert!(empty_series.is_empty());
    assert_eq!(empty_series.len(), 0);
    assert_eq!(empty_series.name(), "empty_vec_series");
    assert_eq!(empty_series.dtype(), DataType::Int32);
}

#[test]
fn test_basic_properties_and_access() {
    let s = create_int_series();
    assert_eq!(s.name(), "int_series");
    assert_eq!(s.dtype(), DataType::Int32);
    assert_eq!(s.len(), 4);
    assert!(!s.is_empty());

    // 数据访问
    let s_float = create_float_series_with_none();
    assert_eq!(s_float.get(0), Some(&1.0));
    assert_eq!(s_float.get(1), None);
    assert_eq!(s_float.get(2), Some(&3.5));
    assert_eq!(s_float.get(4), None);

    // 迭代器
    let mut iter = s_float.iter();
    assert_eq!(iter.next(), Some(Some(&1.0)));
    assert_eq!(iter.next(), Some(None));
    assert_eq!(iter.next(), Some(Some(&3.5)));
    assert_eq!(iter.next(), Some(Some(&0.5)));
    assert_eq!(iter.next(), None);

    let valid_values: Vec<&f64> = s_float.iter_valid().collect();
    assert_eq!(valid_values, vec![&1.0, &3.5, &0.5]);

    let owned_values: Vec<f64> = s_float.iter_valid_owned().collect();
    assert_eq!(owned_values, vec![1.0, 3.5, 0.5]);
}

#[test]
fn test_modification_operations() {
    // Push操作
    let mut s: Series<i32> = Series::new_empty("push_test".into(), DataType::Null);
    assert_eq!(s.dtype(), DataType::Null);
    s.push(Some(10));
    assert_eq!(s.len(), 1);
    assert_eq!(s.dtype(), DataType::Int32);
    assert_eq!(s.data_internal(), &vec![Some(10)]);
    s.push(None);
    s.push(Some(20));
    assert_eq!(s.len(), 3);
    assert_eq!(s.data_internal(), &vec![Some(10), None, Some(20)]);

    // Clear操作
    let mut s2 = create_int_series();
    s2.set_sorted_flag(true, false);
    assert!(!s2.is_empty());
    assert!(s2.is_sorted());
    s2.clear();
    assert!(s2.is_empty());
    assert_eq!(s2.len(), 0);
    assert_eq!(s2.dtype(), DataType::Null);
    assert!(!s2.is_sorted());

    // 重命名
    let mut s3 = create_int_series();
    assert_eq!(s3.name(), "int_series");
    s3.rename("new_name".into());
    assert_eq!(s3.name(), "new_name");

    let s4 = s3.with_name("another_name".into());
    assert_eq!(s4.name(), "another_name");
}

#[test]
fn test_flags_and_sorting() {
    let mut s = create_int_series();
    let flags = s.get_flags();
    assert!(!flags.is_sorted_ascending());
    assert!(!flags.is_sorted_descending());
    assert!(!flags.is_sorted());

    s.set_sorted_flag(true, false);
    assert!(s.is_sorted_ascending());
    assert!(!s.is_sorted_descending());
    assert!(s.is_sorted());

    s.push(Some(5));
    assert!(!s.is_sorted());

    // 排序测试
    let mut s2 = Series::from((
        "sort_test".to_string(),
        vec![Some(3), None, Some(1), Some(4), None, Some(1)],
    ));
    assert!(!s2.is_sorted());

    s2.sort(false);
    assert_eq!(
        s2.data_internal(),
        &vec![None, None, Some(1), Some(1), Some(3), Some(4)]
    );
    assert!(s2.is_sorted_ascending());

    s2.sort(true);
    assert_eq!(
        s2.data_internal(),
        &vec![Some(4), Some(3), Some(1), Some(1), None, None]
    );
    assert!(s2.is_sorted_descending());
}

#[test]
fn test_functional_operations() {
    // Map操作
    let s_int = Series::from(("s_int".to_string(), vec![Some(1), None, Some(3)]));
    let s_mapped = s_int.map(|opt_val| {
        opt_val.map(|v| format!("val: {}", v * 2))
    });
    assert_eq!(s_mapped.name(), "s_int");
    assert_eq!(s_mapped.dtype(), DataType::String);
    assert_eq!(
        s_mapped.data_internal(),
        &vec![Some("val: 2".to_string()), None, Some("val: 6".to_string())]
    );

    // Filter操作
    let s_filter = Series::from(("s_int".to_string(), vec![Some(10), None, Some(30), Some(40)]));
    let mask = Series::from(("mask".to_string(), vec![Some(true), Some(false), None, Some(true)]));
    let s_filtered = s_filter.filter(&mask);
    assert_eq!(s_filtered.name(), "s_int");
    assert_eq!(s_filtered.len(), 2);
    assert_eq!(s_filtered.data_internal(), &vec![Some(10), Some(40)]);

    // Apply操作
    let series_int = Series::new_from_options("nums".into(), vec![Some(1), Some(2), None, Some(4)]);
    let series_plus_10 = series_int.apply(|opt_v| opt_v.map(|v| v + 10));
    assert_eq!(series_plus_10.data_internal(), &vec![Some(11), Some(12), None, Some(14)]);

    let series_str = series_int.apply(|opt_v| opt_v.map(|v| format!("val: {}", v)));
    assert_eq!(series_str.dtype(), DataType::String);
    assert_eq!(
        series_str.data_internal(),
        &vec![Some("val: 1".to_string()), Some("val: 2".to_string()), None, Some("val: 4".to_string())]
    );
}

#[test]
#[should_panic(expected = "Filter mask length (3) must match Series length (4)")]
fn test_filter_panic_length_mismatch() {
    let s_int = Series::from(("s_int".to_string(), vec![Some(10), None, Some(30), Some(40)]));
    let mask_short = Series::from(("mask".to_string(), vec![Some(true), Some(false), None]));
    let _ = s_int.filter(&mask_short);
}

#[test]
fn test_bool_operations() {
    let s_bool1 = Series::from(("b1".into(), vec![Some(true), Some(true), None]));
    assert!(!s_bool1.all());
    assert!(s_bool1.any());

    let s_bool2 = Series::from(("b2".into(), vec![Some(true), Some(true), Some(true)]));
    assert!(s_bool2.all());
    assert!(s_bool2.any());

    let s_bool3 = Series::from(("b3".into(), vec![Some(false), None, Some(false)]));
    assert!(!s_bool3.all());
    assert!(!s_bool3.any());

    let s_bool4: Series<bool> = Series::new_empty("b4".into(), DataType::Bool);
    assert!(s_bool4.all());
    assert!(!s_bool4.any());
}

#[test]
fn test_equals() {
    let s1a = Series::from(("a".to_string(), vec![Some(1), None, Some(3)]));
    let s1b = Series::from(("a".to_string(), vec![Some(1), None, Some(3)]));
    let s1c = Series::from(("a".to_string(), vec![Some(1), Some(0), Some(3)]));
    let s1d = Series::from(("a".to_string(), vec![Some(1), None, Some(4)]));
    let s2_name = Series::from(("b".to_string(), vec![Some(1), None, Some(3)]));

    assert!(s1a.equals(&s1b));
    assert!(!s1a.equals(&s1c));
    assert!(!s1a.equals(&s1d));
    assert!(!s1a.equals(&s2_name));

    // NaN测试
    let s_f1 = Series::from(("f".to_string(), vec![Some(1.0), Some(f64::NAN), None]));
    let s_f2 = Series::from(("f".to_string(), vec![Some(1.0), Some(f64::NAN), None]));
    assert!(!s_f1.equals(&s_f2), "NaN should not equal NaN in equals");

    // equals_missing测试
    assert!(s1a.equals_missing(&s1b));
    assert!(!s1a.equals_missing(&s1c));
    assert!(!s_f1.equals_missing(&s_f2), "NaN should not equal NaN in equals_missing");

    let s_n1 = Series::from(("n".to_string(), vec![None, Some(1), None]));
    let s_n2 = Series::from(("n".to_string(), vec![None, Some(1), None]));
    let s_n3 = Series::from(("n".to_string(), vec![Some(0), Some(1), None]));
    assert!(s_n1.equals_missing(&s_n2));
    assert!(!s_n1.equals_missing(&s_n3));
}

#[test]
fn test_series_compare_operations() -> AxionResult<()> {
    let series = Series::new_from_options("numbers".into(), vec![Some(10), Some(20), None, Some(30)]);

    // 与标量比较
    let gt_result = series.gt(15).unwrap();
    assert_eq!(gt_result.name(), "numbers_gt_scalar");
    assert_eq!(
        gt_result.data_internal(),
        &vec![Some(false), Some(true), None, Some(true)]
    );

    let eq_result = series.eq(20).unwrap();
    assert_eq!(eq_result.name(), "numbers_eq_scalar");
    assert_eq!(
        eq_result.data_internal(),
        &vec![Some(false), Some(true), None, Some(false)]
    );

    // 字符串比较
    let series_str = Series::new_from_options("words".into(), vec![Some("apple".to_string()), Some("banana".to_string()), None, Some("cherry".to_string())]);
    let gt_str_result = series_str.gt("banana".to_string()).unwrap();
    assert_eq!(
        gt_str_result.data_internal(),
        &vec![Some(false), Some(false), None, Some(true)]
    );

    // Series间比较
    let series_a = Series::new_from_options("a".into(), vec![Some(10), Some(20), None, Some(30), Some(40)]);
    let series_b = Series::new_from_options("b".into(), vec![Some(15), Some(20), Some(25), None, Some(50)]);

    let gt_series_result = series_a.gt(&series_b)?;
    assert_eq!(gt_series_result.name(), "a_gt_series");
    assert_eq!(
        gt_series_result.data_internal(),
        &vec![Some(false), Some(false), None, None, Some(false)]
    );

    let eq_series_result = series_a.eq(&series_b)?;
    assert_eq!(
        eq_series_result.data_internal(),
        &vec![Some(false), Some(true), None, None, Some(false)]
    );

    // 长度不匹配错误
    let series_short = Series::new_from_options("short".into(), vec![Some(1), Some(2)]);
    let mismatch_result = series_a.gt(&series_short);
    assert!(mismatch_result.is_err());
    match mismatch_result.err().unwrap() {
        AxionError::MismatchedLengths { expected, found, name } => {
            assert_eq!(expected, series_a.len());
            assert_eq!(found, series_short.len());
            assert_eq!(name, "short");
        }
        _ => panic!("Expected MismatchedLengths error"),
    }

    Ok(())
}

#[test]
fn test_series_arithmetic_operations() -> AxionResult<()> {
    let series_int = Series::new_from_options("int".into(), vec![Some(10), Some(20), None, Some(40)]);

    // 与标量运算
    let add_result = series_int.add_scalar(5)?;
    assert_eq!(add_result.name(), "int_add_scalar");
    assert_eq!(add_result.data_internal(), &vec![Some(15), Some(25), None, Some(45)]);

    let mul_result = series_int.mul_scalar(2)?;
    assert_eq!(mul_result.name(), "int_mul_scalar");
    assert_eq!(mul_result.data_internal(), &vec![Some(20), Some(40), None, Some(80)]);

    let div_result = series_int.div_scalar(5)?;
    assert_eq!(div_result.name(), "int_div_scalar");
    assert_eq!(div_result.data_internal(), &vec![Some(2), Some(4), None, Some(8)]);

    // 浮点数运算
    let series_float = Series::new_from_options("float".into(), vec![Some(10.0), Some(20.5), None, Some(40.0)]);
    let add_float_result = series_float.add_scalar(2.0)?;
    assert_eq!(add_float_result.data_internal(), &vec![Some(12.0), Some(22.5), None, Some(42.0)]);

    // Series间运算
    let series_a = Series::new_from_options("a_int".into(), vec![Some(10), Some(20), None, Some(40)]);
    let series_b = Series::new_from_options("b_int".into(), vec![Some(5), Some(2), Some(100), None]);

    let add_series_result = series_a.add_series(&series_b)?;
    assert_eq!(add_series_result.name(), "a_int_add_series_b_int");
    assert_eq!(add_series_result.data_internal(), &vec![Some(15), Some(22), None, None]);

    let mul_series_result = series_a.mul_series(&series_b)?;
    assert_eq!(mul_series_result.data_internal(), &vec![Some(50), Some(40), None, None]);

    // 长度不匹配错误
    let series_short = Series::new_from_options("short".into(), vec![Some(1), Some(2)]);
    let mismatch_result = series_a.add_series(&series_short);
    assert!(mismatch_result.is_err());

    Ok(())
}

#[test]
#[should_panic(expected = "attempt to divide by zero")]
fn test_series_arith_series_int_div_zero() {
    let series_a = Series::new_from_options("a".into(), vec![Some(10), Some(20)]);
    let series_zero = Series::new_from_options("zero".into(), vec![Some(2), Some(0)]);
    let _ = series_a.div_series(&series_zero);
}

#[test]
fn test_series_aggregation() {
    let series_int = Series::new_from_options("int_agg".into(), vec![Some(10), Some(20), None, Some(5), Some(10)]);

    assert_eq!(series_int.sum(), Some(45));
    assert_eq!(series_int.min(), Some(5));
    assert_eq!(series_int.max(), Some(20));
    assert_eq!(series_int.mean(), Some(11.25));

    let series_float = Series::new_from_options("float_agg".into(), vec![Some(10.0), Some(20.5), None, Some(5.0), Some(10.0)]);
    assert_eq!(series_float.sum(), Some(45.5));
    assert!((series_float.mean().unwrap() - 11.375).abs() < f64::EPSILON);

    // NaN处理
    let series_nan = Series::new_from_options("nan_agg".into(), vec![Some(1.0), Some(f64::NAN), Some(3.0)]);
    assert!(series_nan.sum().unwrap().is_nan());
    assert_eq!(series_nan.min(), Some(1.0));
    assert_eq!(series_nan.max(), Some(3.0));

    // 空Series
    let series_empty: Series<i32> = Series::new_empty("empty_int_agg".into(), DataType::Int32);
    assert_eq!(series_empty.sum(), None);
    assert_eq!(series_empty.mean(), None);

    // 全为None
    let series_only_none: Series<f64> = Series::new_from_options("only_none_agg".into(), vec![None, None]);
    assert_eq!(series_only_none.sum(), None);
    assert_eq!(series_only_none.mean(), None);
}

#[test]
fn test_series_string_operations() -> AxionResult<()> {
    let series_str = Series::new_from_options(
        "text".into(),
        vec![
            Some("hello".to_string()),
            Some("world".to_string()),
            None,
            Some("rust".to_string()),
            Some("".to_string()),
            Some("hello rust".to_string()),
        ],
    );

    // Contains操作
    let contains_o = series_str.str().contains("o")?;
    assert_eq!(contains_o.name(), "text_contains_o");
    assert_eq!(contains_o.dtype(), DataType::Bool);
    assert_eq!(
        contains_o.data_internal(),
        &vec![Some(true), Some(true), None, Some(false), Some(false), Some(true)]
    );

    // StartsWith操作
    let starts_h = series_str.str().startswith("h")?;
    assert_eq!(starts_h.name(), "text_startswith_h");
    assert_eq!(
        starts_h.data_internal(),
        &vec![Some(true), Some(false), None, Some(false), Some(false), Some(true)]
    );

    // EndsWith操作
    let ends_o = series_str.str().endswith("o")?;
    assert_eq!(ends_o.name(), "text_endswith_o");
    assert_eq!(
        ends_o.data_internal(),
        &vec![Some(true), Some(false), None, Some(false), Some(false), Some(false)]
    );

    let ends_rust = series_str.str().endswith("rust")?;
    assert_eq!(ends_rust.name(), "text_endswith_rust");
    assert_eq!(
        ends_rust.data_internal(),
        &vec![Some(false), Some(false), None, Some(true), Some(false), Some(true)]
    );

    // 长度计算
    let lengths = series_str.str().str_len()?;
    assert_eq!(lengths.name(), "text_len");
    assert_eq!(lengths.dtype(), DataType::UInt32);
    assert_eq!(
        lengths.data_internal(),
        &vec![Some(5), Some(5), None, Some(4), Some(0), Some(10)]
    );

    // 替换操作
    let replaced = series_str.str().replace("l", "X")?;
    assert_eq!(replaced.name(), "text_replace");
    assert_eq!(
        replaced.data_internal(),
        &vec![
            Some("heXXo".to_string()),
            Some("worXd".to_string()),
            None,
            Some("rust".to_string()),
            Some("".to_string()),
            Some("heXXo rust".to_string()),
        ]
    );

    // 大小写转换
    let series_mixed = Series::new_from_options(
        "mixed".into(),
        vec![Some("Hello".to_string()), Some("WORLD".to_string()), None, Some("RuSt".to_string())]
    );

    let lower = series_mixed.str().to_lowercase()?;
    assert_eq!(lower.name(), "mixed_lower");
    assert_eq!(
        lower.data_internal(),
        &vec![Some("hello".to_string()), Some("world".to_string()), None, Some("rust".to_string())]
    );

    let upper = series_mixed.str().to_uppercase()?;
    assert_eq!(upper.name(), "mixed_upper");
    assert_eq!(
        upper.data_internal(),
        &vec![Some("HELLO".to_string()), Some("WORLD".to_string()), None, Some("RUST".to_string())]
    );

    // 去除空白
    let series_whitespace = Series::new_from_options(
        "whitespace".into(),
        vec![
            Some("  hello ".to_string()),
            Some("\tworld\n".to_string()),
            None,
            Some(" rust ".to_string()),
            Some("   ".to_string()),
        ]
    );

    let stripped = series_whitespace.str().strip()?;
    assert_eq!(stripped.name(), "whitespace_strip");
    assert_eq!(
        stripped.data_internal(),
        &vec![
            Some("hello".to_string()),
            Some("world".to_string()),
            None,
            Some("rust".to_string()),
            Some("".to_string()),
        ]
    );

    // 空Series测试
    let empty_str_series: Series<String> = Series::new_empty("empty_text".into(), DataType::String);
    let empty_replace = empty_str_series.str().replace("a", "b")?;
    assert!(empty_replace.is_empty());
    assert_eq!(empty_replace.dtype(), DataType::String);

    Ok(())
}

#[test]
fn test_null_handling() {
    let s = Series::<i32>::new_from_options("a".into(), vec![Some(1), None, Some(3), None]);
    
    let is_null = s.is_null();
    assert_eq!(is_null.data_internal(), &vec![Some(false), Some(true), Some(false), Some(true)]);

    let filled = s.fill_null(99);
    assert_eq!(filled.data_internal(), &vec![Some(1), Some(99), Some(3), Some(99)]);
}

#[test]
fn test_parallel_apply() {
    // 简单乘法
    let s1 = Series::new_from_options(
        "integers".to_string(),
        vec![Some(1i32), Some(2), None, Some(4), Some(5)],
    );
    let s_doubled = s1.par_apply(|opt_x| opt_x.map(|x| x * 2));
    assert_eq!(s_doubled.name(), "integers");
    assert_eq!(s_doubled.dtype(), DataType::Int32);
    assert_eq!(
        s_doubled.data_internal(),
        &vec![Some(2i32), Some(4), None, Some(8), Some(10)]
    );

    // 类型转换
    let s_stringified = s1.par_apply(|opt_x| opt_x.map(|x| x.to_string()));
    assert_eq!(s_stringified.dtype(), DataType::String);
    assert_eq!(
        s_stringified.data_internal(),
        &vec![Some("1".to_string()), Some("2".to_string()), None, Some("4".to_string()), Some("5".to_string())]
    );

    // 空Series
    let s_empty: Series<i32> = Series::new_empty("empty_i32".to_string(), DataType::Int32);
    let s_applied = s_empty.par_apply(|opt_x: Option<&i32>| opt_x.map(|x| x + 100));
    assert!(s_applied.is_empty());
    assert_eq!(s_applied.name(), "empty_i32");
    assert_eq!(s_applied.dtype(), DataType::Int32);

    // 全为None
    let s_all_none: Series<f64> = Series::new_from_options(
        "all_none_f64".to_string(),
        vec![None, None, None],
    );
    let s_processed = s_all_none.par_apply(|opt_x| opt_x.map(|x| x.powi(2)));
    assert_eq!(s_processed.data_internal(), &vec![None, None, None]);
}

#[test]
fn test_take_inner() {
    let s = Series::new("take_test".into(), vec![10, 20]);
    let inner_vec = s.take_inner();
    assert_eq!(inner_vec, vec![Some(10), Some(20)]);
}