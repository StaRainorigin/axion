use axion::{DataType, Series, SeriesCompare, SeriesArithScalar, SeriesArithSeries}; // <--- 只需要导入 SeriesCompare
use axion::error::{AxionError, AxionResult};

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
fn test_series_creation_new() {
    let s1 = Series::new("s1".into(), vec![1, 2, 3]);
    assert_eq!(s1.name(), "s1");
    assert_eq!(s1.dtype(), DataType::Int32);
    assert_eq!(s1.len(), 3);
    assert_eq!(s1.data_internal(), &vec![Some(1), Some(2), Some(3)]);

    let s2 = Series::new("s2".into(), [10.0f64, 20.0]);
    assert_eq!(s2.name(), "s2");
    assert_eq!(s2.dtype(), DataType::Float64);
    assert_eq!(s2.len(), 2);
    assert_eq!(s2.data_internal(), &vec![Some(10.0), Some(20.0)]);

    let data_slice: &[i8] = &[5, 6];
    let s3 = Series::new("s3".into(), data_slice);
    assert_eq!(s3.name(), "s3");
    assert_eq!(s3.dtype(), DataType::Int8);
    assert_eq!(s3.len(), 2);
    assert_eq!(s3.data_internal(), &vec![Some(5i8), Some(6)]);
}

#[test]
fn test_series_creation_from() {
    let s1 = Series::from(("s1_from".to_string(), vec![true, false]));
    assert_eq!(s1.name(), "s1_from");
    assert_eq!(s1.dtype(), DataType::Bool);
    assert_eq!(s1.len(), 2);
    assert_eq!(s1.data_internal(), &vec![Some(true), Some(false)]);

    let data_slice: &[&str] = &["a", "b"];
    let data_vec: Vec<String> = data_slice.iter().map(|s| s.to_string()).collect();
    let s2 = Series::from(("s2_from".to_string(), data_vec));
    assert_eq!(s2.name(), "s2_from");
    assert_eq!(s2.dtype(), DataType::String);
    assert_eq!(s2.len(), 2);
    assert_eq!(s2.data_internal(), &vec![Some("a".to_string()), Some("b".to_string())]);

    let s3 = Series::from(("s3_from".to_string(), [100u16, 200]));
    assert_eq!(s3.name(), "s3_from");
    assert_eq!(s3.dtype(), DataType::UInt16);
    assert_eq!(s3.len(), 2);
    assert_eq!(s3.data_internal(), &vec![Some(100u16), Some(200)]);
}

#[test]
fn test_series_creation_empty() {
    let s_empty: Series<i32> = Series::new_empty("empty_int".into(), DataType::Int32);
    assert_eq!(s_empty.name(), "empty_int");
    assert_eq!(s_empty.dtype(), DataType::Int32);
    assert!(s_empty.is_empty());
    assert_eq!(s_empty.len(), 0);

    let s_null_dtype: Series<f64> = Series::new_empty("empty_f64".into(), DataType::Null);
    assert_eq!(s_null_dtype.dtype(), DataType::Null);
}

#[test]
// #[should_panic(expected = "Cannot create Series from empty Vec")] // REMOVE THIS LINE
fn test_series_creation_new_from_empty_vec() { // RENAME for clarity
    let empty_vec: Vec<i32> = Vec::new();
    let empty_series: Series<i32> = Series::new("empty_vec_series".into(), empty_vec);
    assert!(empty_series.is_empty());
    assert_eq!(empty_series.len(), 0);
    assert_eq!(empty_series.name(), "empty_vec_series");
    assert_eq!(empty_series.dtype(), DataType::Int32); // Or DataType::Null if Series::new behaves that way for empty inputs
}

#[test]
// #[should_panic(expected = "Cannot create Series from empty slice")] // REMOVE THIS LINE
fn test_series_creation_new_from_empty_slice() { // RENAME for clarity
    let empty_slice: &[f32] = &[];
    let empty_series: Series<f32> = Series::new("empty_slice_series".into(), empty_slice);
    assert!(empty_series.is_empty());
    assert_eq!(empty_series.len(), 0);
    assert_eq!(empty_series.name(), "empty_slice_series");
    assert_eq!(empty_series.dtype(), DataType::Float32); // Or DataType::Null
}

#[test]
fn test_series_creation_new_from_empty_array() { // This one is already good
    let empty_array: [bool; 0] = [];
    let empty_series: Series<bool> = Series::new("empty_bool_array".into(), empty_array);
    assert!(empty_series.is_empty());
    assert_eq!(empty_series.len(), 0);
    assert_eq!(empty_series.name(), "empty_bool_array");
    assert_eq!(empty_series.dtype(), DataType::Bool);
}

#[test]
fn test_basic_properties() {
    let s = create_int_series();
    assert_eq!(s.name(), "int_series");
    assert_eq!(s.dtype(), DataType::Int32);
    assert_eq!(s.len(), 4);
    assert!(!s.is_empty());
}

#[test]
fn test_data_access_get() {
    let s = create_float_series_with_none();
    assert_eq!(s.get(0), Some(&1.0));
    assert_eq!(s.get(1), None);
    assert_eq!(s.get(2), Some(&3.5));
    assert_eq!(s.get(3), Some(&0.5));
    assert_eq!(s.get(4), None);
}

#[test]
fn test_data_access_iter() {
    let s = create_float_series_with_none();
    let mut iter = s.iter();
    assert_eq!(iter.next(), Some(Some(&1.0)));
    assert_eq!(iter.next(), Some(None));
    assert_eq!(iter.next(), Some(Some(&3.5)));
    assert_eq!(iter.next(), Some(Some(&0.5)));
    assert_eq!(iter.next(), None);
    let collected: Vec<_> = (&s).into_iter().collect();
    assert_eq!(collected, vec![Some(&1.0), None, Some(&3.5), Some(&0.5)]);
}

#[test]
fn test_data_access_iter_valid() {
    let s = create_float_series_with_none();
    let collected: Vec<&f64> = s.iter_valid().collect();
    assert_eq!(collected, vec![&1.0, &3.5, &0.5]);
}

#[test]
fn test_data_access_iter_valid_owned() {
    let s = create_float_series_with_none();
    let collected: Vec<f64> = s.iter_valid_owned().collect();
    assert_eq!(collected, vec![1.0, 3.5, 0.5]);
}

#[test]
fn test_take_inner() {
    let s = Series::new("take_test".into(), vec![10, 20]);
    let inner_vec = s.take_inner();
    assert_eq!(inner_vec, vec![Some(10), Some(20)]);
}

#[test]
fn test_modification_push() {
    let mut s: Series<i32> = Series::new_empty("push_test".into(), DataType::Null);
    assert_eq!(s.dtype(), DataType::Null);
    s.push(Some(10));
    assert_eq!(s.len(), 1);
    assert_eq!(s.dtype(), DataType::Int32);
    assert_eq!(s.data_internal(), &vec![Some(10)]);
    s.push(None);
    assert_eq!(s.len(), 2);
    assert_eq!(s.dtype(), DataType::Int32);
    assert_eq!(s.data_internal(), &vec![Some(10), None]);
    s.push(Some(20));
    assert_eq!(s.len(), 3);
    assert_eq!(s.data_internal(), &vec![Some(10), None, Some(20)]);
    assert!(!s.is_sorted());
}

#[test]
fn test_modification_clear() {
    let mut s = create_int_series();
    s.set_sorted_flag(true, false);
    assert!(!s.is_empty());
    assert!(s.is_sorted());
    s.clear();
    assert!(s.is_empty());
    assert_eq!(s.len(), 0);
    assert_eq!(s.dtype(), DataType::Null);
    assert_eq!(s.data_internal(), &Vec::<Option<i32>>::new());
    assert!(!s.is_sorted());
}

#[test]
fn test_modification_rename() {
    let mut s = create_int_series();
    assert_eq!(s.name(), "int_series");
    s.rename("new_name".into());
    assert_eq!(s.name(), "new_name");
}

#[test]
fn test_modification_with_name() {
    let s = create_int_series();
    assert_eq!(s.name(), "int_series");
    let s_renamed = s.with_name("another_name".into());
    assert_eq!(s_renamed.name(), "another_name");
}

#[test]
fn test_flags() {
    let mut s = create_int_series();
    let flags = s.get_flags();
    assert!(!flags.is_sorted_ascending());
    assert!(!flags.is_sorted_descending());
    assert!(!flags.is_sorted());

    s.set_sorted_flag(true, false);
    assert!(s.is_sorted_ascending());
    assert!(!s.is_sorted_descending());
    assert!(s.is_sorted());

    s.set_sorted_flag(false, true);
    assert!(!s.is_sorted_ascending());
    assert!(s.is_sorted_descending());
    assert!(s.is_sorted());

    s.push(Some(5));
    assert!(!s.is_sorted_ascending());
    assert!(!s.is_sorted_descending());
    assert!(!s.is_sorted());
}

#[test]
fn test_sort() {
    let mut s = Series::from((
        "sort_test".to_string(),
        vec![Some(3), None, Some(1), Some(4), None, Some(1)],
    ));
    assert!(!s.is_sorted());

    s.sort(false);
    assert_eq!(
        s.data_internal(),
        &vec![None, None, Some(1), Some(1), Some(3), Some(4)]
    );
    assert!(s.is_sorted_ascending());
    assert!(!s.is_sorted_descending());
    assert!(s.is_sorted());

    s.sort(true);
    assert_eq!(
        s.data_internal(),
        &vec![Some(4), Some(3), Some(1), Some(1), None, None]
    );
    assert!(!s.is_sorted_ascending());
    assert!(s.is_sorted_descending());
    assert!(s.is_sorted());
}

#[test]
fn test_map() {
    let s_int = Series::from(("s_int".to_string(), vec![Some(1), None, Some(3)]));

    let s_mapped = s_int.map(|opt_val| {
        opt_val.map(|v| format!("val: {}", v * 2))
    });

    assert_eq!(s_mapped.name(), "s_int");
    assert_eq!(s_mapped.dtype(), DataType::String);
    assert_eq!(s_mapped.len(), 3);
    assert_eq!(
        s_mapped.data_internal(),
        &vec![Some("val: 2".to_string()), None, Some("val: 6".to_string())]
    );
}

#[test]
fn test_filter() {
    let s_int = Series::from(("s_int".to_string(), vec![Some(10), None, Some(30), Some(40)]));
    let mask = Series::from(("mask".to_string(), vec![Some(true), Some(false), None, Some(true)]));

    let s_filtered = s_int.filter(&mask);

    assert_eq!(s_filtered.name(), "s_int");
    assert_eq!(s_filtered.dtype(), DataType::Int32);
    assert_eq!(s_filtered.len(), 2);
    assert_eq!(
        s_filtered.data_internal(),
        &vec![Some(10), Some(40)]
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
fn test_bool_all_any() {
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
    let s4_len = Series::from(("a".to_string(), vec![Some(1), None]));

    assert!(s1a.equals(&s1b));
    assert!(!s1a.equals(&s1c));
    assert!(!s1a.equals(&s1d));
    assert!(!s1a.equals(&s2_name));
    assert!(!s1a.equals(&s4_len));

    let s_f1 = Series::from(("f".to_string(), vec![Some(1.0), Some(f64::NAN), None]));
    let s_f2 = Series::from(("f".to_string(), vec![Some(1.0), Some(f64::NAN), None]));
    let s_f3 = Series::from(("f".to_string(), vec![Some(1.0), Some(2.0), None]));
    assert!(!s_f1.equals(&s_f2), "NaN should not equal NaN in equals");
    assert!(!s_f1.equals(&s_f3));
}

#[test]
fn test_equals_missing() {
    let s1a = Series::from(("a".to_string(), vec![Some(1), None, Some(3)]));
    let s1b = Series::from(("a".to_string(), vec![Some(1), None, Some(3)]));
    let s1c = Series::from(("a".to_string(), vec![Some(1), Some(0), Some(3)]));
    let s1d = Series::from(("a".to_string(), vec![Some(1), None, Some(4)]));
    let s2_name = Series::from(("b".to_string(), vec![Some(1), None, Some(3)]));
    let s4_len = Series::from(("a".to_string(), vec![Some(1), None]));

    assert!(s1a.equals_missing(&s1b));
    assert!(!s1a.equals_missing(&s1c));
    assert!(!s1a.equals_missing(&s1d));
    assert!(!s1a.equals_missing(&s2_name));
    assert!(!s1a.equals_missing(&s4_len));

    let s_f1 = Series::from(("f".to_string(), vec![Some(1.0), Some(f64::NAN), None]));
    let s_f2 = Series::from(("f".to_string(), vec![Some(1.0), Some(f64::NAN), None]));
    assert!(!s_f1.equals_missing(&s_f2), "NaN should not equal NaN in equals_missing");

    let s_n1 = Series::from(("n".to_string(), vec![None, Some(1), None]));
    let s_n2 = Series::from(("n".to_string(), vec![None, Some(1), None]));
    let s_n3 = Series::from(("n".to_string(), vec![Some(0), Some(1), None]));
    assert!(s_n1.equals_missing(&s_n2));
    assert!(!s_n1.equals_missing(&s_n3));
}

#[test]
fn test_series_compare_scalar() {
    let series = Series::new_from_options("numbers".into(), vec![Some(10), Some(20), None, Some(30)]);

    let gt_result = series.gt(15).unwrap();
    assert_eq!(gt_result.name(), "numbers_gt_scalar");
    assert_eq!(
        gt_result.data_internal(),
        &vec![Some(false), Some(true), None, Some(true)]
    );

    let lt_result = series.lt(25).unwrap();
    assert_eq!(lt_result.name(), "numbers_lt_scalar");
    assert_eq!(
        lt_result.data_internal(),
        &vec![Some(true), Some(true), None, Some(false)]
    );

    let eq_result = series.eq(20).unwrap();
    assert_eq!(eq_result.name(), "numbers_eq_scalar");
    assert_eq!(
        eq_result.data_internal(),
        &vec![Some(false), Some(true), None, Some(false)]
    );

    let neq_result = series.neq(20).unwrap();
    assert_eq!(neq_result.name(), "numbers_neq_scalar");
    assert_eq!(
        neq_result.data_internal(),
        &vec![Some(true), Some(false), None, Some(true)]
    );

    let gte_result = series.gte(20).unwrap();
    assert_eq!(gte_result.name(), "numbers_gte_scalar");
    assert_eq!(
        gte_result.data_internal(),
        &vec![Some(false), Some(true), None, Some(true)]
    );

    let lte_result = series.lte(20).unwrap();
    assert_eq!(lte_result.name(), "numbers_lte_scalar");
    assert_eq!(
        lte_result.data_internal(),
        &vec![Some(true), Some(true), None, Some(false)]
    );
}

#[test]
fn test_series_compare_scalar_with_strings() {
    let series = Series::new_from_options("words".into(), vec![Some("apple".to_string()), Some("banana".to_string()), None, Some("cherry".to_string())]);

    let gt_result = series.gt("banana".to_string()).unwrap();
    assert_eq!(
        gt_result.data_internal(),
        &vec![Some(false), Some(false), None, Some(true)]
    );

    let eq_result = series.eq("banana".to_string()).unwrap();
    assert_eq!(
        eq_result.data_internal(),
        &vec![Some(false), Some(true), None, Some(false)]
    );
}

#[test]
fn test_series_compare_scalar_short_methods() {
    let series = Series::new_from_options("numbers".into(), vec![Some(10), Some(20), None, Some(30)]);

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
}

#[test]
fn test_series_compare_series() -> AxionResult<()> {
    let series_a = Series::new_from_options("a".into(), vec![Some(10), Some(20), None, Some(30), Some(40)]);
    let series_b = Series::new_from_options("b".into(), vec![Some(15), Some(20), Some(25), None, Some(50)]);

    let gt_result = series_a.gt(&series_b)?;
    assert_eq!(gt_result.name(), "a_gt_series");
    assert_eq!(
        gt_result.data_internal(),
        &vec![Some(false), Some(false), None, None, Some(false)]
    );

    let eq_result = series_a.eq(&series_b)?;
    assert_eq!(eq_result.name(), "a_eq_series");
    assert_eq!(
        eq_result.data_internal(),
        &vec![Some(false), Some(true), None, None, Some(false)]
    );

    let lte_result = series_a.lte(&series_b)?;
    assert_eq!(lte_result.name(), "a_lte_series");
    assert_eq!(
        lte_result.data_internal(),
        &vec![Some(true), Some(true), None, None, Some(true)]
    );

    let series_c = Series::new_from_options("c".into(), vec![Some("apple".to_string()), None, Some("banana".to_string())]);
    let series_d = Series::new_from_options("d".into(), vec![Some("apple".to_string()), Some("orange".to_string()), None]);

    let eq_str_result = series_c.eq(&series_d)?;
    assert_eq!(
        eq_str_result.data_internal(),
        &vec![Some(true), None, None]
    );

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
fn test_unified_compare() -> AxionResult<()> {
    let series_a = Series::new_from_options("a".into(), vec![Some(10), Some(20), None, Some(30), Some(40)]);
    let series_b = Series::new_from_options("b".into(), vec![Some(15), Some(20), Some(25), None, Some(50)]);
    let scalar = 20;

    let gt_scalar_result = series_a.gt(scalar)?;
    assert_eq!(
        gt_scalar_result.data_internal(),
        &vec![Some(false), Some(false), None, Some(true), Some(true)]
    );

    let eq_series_result = series_a.eq(&series_b)?;
    assert_eq!(
        eq_series_result.data_internal(),
        &vec![Some(false), Some(true), None, None, Some(false)]
    );

    Ok(())
}

// #[test]
// #[should_panic(expected = "attempt to divide by zero")]
// fn test_series_arith_scalar_int_div_zero() {
//     let series_int = Series::new_from_options("int".into(), vec![Some(10), Some(20), None, Some(40)]);
//     let _ = series_int.div_scalar(0);
// }

// #[test]
// #[should_panic(expected = "attempt to calculate the remainder with a divisor of zero")]
// fn test_series_arith_scalar_int_rem_zero() {
//     let series_int = Series::new_from_options("int".into(), vec![Some(10), Some(20), None, Some(40)]);
//     let _ = series_int.rem_scalar(0);
// }

#[test]
fn test_series_arith_scalar() -> AxionResult<()> {
    let series_int = Series::new_from_options("int".into(), vec![Some(10), Some(20), None, Some(40)]);
    let scalar_int = 5;

    // --- 测试加法 ---
    let add_result = series_int.add_scalar(scalar_int)?;
    assert_eq!(add_result.name(), "int_add_scalar");
    assert_eq!(add_result.data_internal(), &vec![Some(15), Some(25), None, Some(45)]);

    // --- 测试减法 ---
    let sub_result = series_int.sub_scalar(scalar_int)?;
    assert_eq!(sub_result.name(), "int_sub_scalar");
    assert_eq!(sub_result.data_internal(), &vec![Some(5), Some(15), None, Some(35)]);

    // --- 测试乘法 ---
    let mul_result = series_int.mul_scalar(scalar_int)?;
    assert_eq!(mul_result.name(), "int_mul_scalar");
    assert_eq!(mul_result.data_internal(), &vec![Some(50), Some(100), None, Some(200)]);

    // --- 测试除法 (非零) ---
    let div_result = series_int.div_scalar(scalar_int)?;
    assert_eq!(div_result.name(), "int_div_scalar");
    assert_eq!(div_result.data_internal(), &vec![Some(2), Some(4), None, Some(8)]);

    // --- 测试取余 (非零) ---
    let rem_result = series_int.rem_scalar(3)?; // 用 3 测试取余
    assert_eq!(rem_result.name(), "int_rem_scalar");
    assert_eq!(rem_result.data_internal(), &vec![Some(1), Some(2), None, Some(1)]);

    // --- 测试浮点数 ---
    let series_float = Series::new_from_options("float".into(), vec![Some(10.0), Some(20.5), None, Some(40.0)]);
    let scalar_float = 2.0;

    let add_float_result = series_float.add_scalar(scalar_float)?;
    assert_eq!(add_float_result.name(), "float_add_scalar");
    assert_eq!(add_float_result.data_internal(), &vec![Some(12.0), Some(22.5), None, Some(42.0)]);

    let div_float_result = series_float.div_scalar(scalar_float)?;
    assert_eq!(div_float_result.name(), "float_div_scalar");
    assert_eq!(div_float_result.data_internal(), &vec![Some(5.0), Some(10.25), None, Some(20.0)]);

    // // --- 测试浮点数除零 (保持不变，期望 Ok(Inf)) ---
    // let div_float_zero_result: Series<f64> = series_float.div_scalar(0.0)?;
    // assert_eq!(div_float_zero_result.name(), "float_div_scalar_by_zero"); // <--- 修改期望的名称
    // let data = div_float_zero_result.data_internal();
    // assert!(data[0].unwrap().is_infinite() && data[0].unwrap().is_sign_positive());
    // assert!(data[1].unwrap().is_infinite() && data[1].unwrap().is_sign_positive());
    // assert!(data[2].is_none());
    // assert!(data[3].unwrap().is_infinite() && data[3].unwrap().is_sign_positive());

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
#[should_panic(expected = "attempt to calculate the remainder with a divisor of zero")]
fn test_series_arith_series_int_rem_zero() {
    let series_a = Series::new_from_options("a".into(), vec![Some(10), Some(20)]);
    let series_zero = Series::new_from_options("zero".into(), vec![Some(3), Some(0)]);
    let _ = series_a.rem_series(&series_zero);
}

#[test]
fn test_series_arith_series() -> AxionResult<()> {
    let series_a_int = Series::new_from_options("a_int".into(), vec![Some(10), Some(20), None, Some(40)]);
    let series_b_int = Series::new_from_options("b_int".into(), vec![Some(5), Some(2), Some(100), None]);

    let add_result = series_a_int.add_series(&series_b_int)?;
    assert_eq!(add_result.name(), "a_int_add_series_b_int");
    assert_eq!(add_result.data_internal(), &vec![Some(15), Some(22), None, None]);

    let sub_result = series_a_int.sub_series(&series_b_int)?;
    assert_eq!(sub_result.name(), "a_int_sub_series_b_int");
    assert_eq!(sub_result.data_internal(), &vec![Some(5), Some(18), None, None]);

    let mul_result = series_a_int.mul_series(&series_b_int)?;
    assert_eq!(mul_result.name(), "a_int_mul_series_b_int");
    assert_eq!(mul_result.data_internal(), &vec![Some(50), Some(40), None, None]);

    let series_c_int = Series::new_from_options("c_int".into(), vec![Some(2), Some(5), Some(10), Some(4)]);
    let div_result = series_a_int.div_series(&series_c_int)?;
    assert_eq!(div_result.name(), "a_int_div_series_c_int");
    assert_eq!(div_result.data_internal(), &vec![Some(5), Some(4), None, Some(10)]);

    let rem_result = series_a_int.rem_series(&series_c_int)?;
    assert_eq!(rem_result.name(), "a_int_rem_series_c_int");
    assert_eq!(rem_result.data_internal(), &vec![Some(0), Some(0), None, Some(0)]);

    let series_short_int = Series::new_from_options("short".into(), vec![Some(1), Some(2)]);
    let mismatch_result = series_a_int.add_series(&series_short_int);
    assert!(mismatch_result.is_err());
    match mismatch_result.err().unwrap() {
        AxionError::MismatchedLengths { expected, found, name } => {
            assert_eq!(expected, series_a_int.len());
            assert_eq!(found, series_short_int.len());
            assert_eq!(name, "short");
        }
        _ => panic!("Expected MismatchedLengths error"),
    }

    let series_a_float = Series::new_from_options("a_float".into(), vec![Some(10.0), Some(20.5), None, Some(40.0)]);
    let series_b_float = Series::new_from_options("b_float".into(), vec![Some(2.0), Some(0.5), Some(10.0), None]);

    let add_float_result = series_a_float.add_series(&series_b_float)?;
    assert_eq!(add_float_result.name(), "a_float_add_series_b_float");
    assert_eq!(add_float_result.data_internal(), &vec![Some(12.0), Some(21.0), None, None]);

    let mul_float_result = series_a_float.mul_series(&series_b_float)?;
    assert_eq!(mul_float_result.name(), "a_float_mul_series_b_float");
    assert_eq!(mul_float_result.data_internal(), &vec![Some(20.0), Some(10.25), None, None]);

    let series_c_float = Series::new_from_options("c_float".into(), vec![Some(2.0), Some(0.0), Some(4.0), Some(5.0)]);
    let div_float_zero_result: Series<f64> = series_a_float.div_series(&series_c_float)?;
    assert_eq!(div_float_zero_result.name(), "a_float_div_series_c_float");
    let data = div_float_zero_result.data_internal();
    assert_eq!(data[0], Some(5.0));
    assert!(data[1].unwrap().is_infinite() && data[1].unwrap().is_sign_positive());
    assert!(data[2].is_none());
    assert_eq!(data[3], Some(8.0));

    Ok(())
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
    assert_eq!(series_float.min(), Some(5.0));
    assert_eq!(series_float.max(), Some(20.5));
    assert!((series_float.mean().unwrap() - 11.375).abs() < f64::EPSILON);

    let series_nan = Series::new_from_options("nan_agg".into(), vec![Some(1.0), Some(f64::NAN), Some(3.0)]);
    assert!(series_nan.sum().unwrap().is_nan());
    assert_eq!(series_nan.min(), Some(1.0));
    assert_eq!(series_nan.max(), Some(3.0));
    assert!(series_nan.mean().unwrap().is_nan());

    let series_empty_int: Series<i32> = Series::new_empty("empty_int_agg".into(), DataType::Int32);
    assert_eq!(series_empty_int.sum(), None);
    assert_eq!(series_empty_int.min(), None);
    assert_eq!(series_empty_int.max(), None);
    assert_eq!(series_empty_int.mean(), None);

    let series_only_none: Series<f64> = Series::new_from_options("only_none_agg".into(), vec![None, None]);
    assert_eq!(series_only_none.sum(), None);
    assert_eq!(series_only_none.min(), None);
    assert_eq!(series_only_none.max(), None);
    assert_eq!(series_only_none.mean(), None);
}

#[test]
fn test_series_apply() {
    let series_int = Series::new_from_options("nums".into(), vec![Some(1), Some(2), None, Some(4)]);

    let series_plus_10 = series_int.apply(|opt_v| opt_v.map(|v| v + 10));

    assert_eq!(series_plus_10.name(), "nums");
    assert_eq!(series_plus_10.dtype(), DataType::Int32);
    assert_eq!(series_plus_10.data_internal(), &vec![Some(11), Some(12), None, Some(14)]);

    let series_str = series_int.apply(|opt_v| opt_v.map(|v| format!("val: {}", v)));

    assert_eq!(series_str.name(), "nums");
    assert_eq!(series_str.dtype(), DataType::String);
    assert_eq!(
        series_str.data_internal(),
        &vec![Some("val: 1".to_string()), Some("val: 2".to_string()), None, Some("val: 4".to_string())]
    );

    let series_fill_none = series_int.apply(|opt_v| {
        match opt_v {
            Some(v) => Some(v * 2),
            None => Some(0),
        }
    });

    assert_eq!(series_fill_none.name(), "nums");
    assert_eq!(series_fill_none.dtype(), DataType::Int32);
    assert_eq!(series_fill_none.data_internal(), &vec![Some(2), Some(4), Some(0), Some(8)]);

    let series_bool_filter = series_int.apply(|opt_v| {
        opt_v.filter(|&v| *v > 2).map(|_| true)
    });

    assert_eq!(series_bool_filter.name(), "nums");
    assert_eq!(series_bool_filter.dtype(), DataType::Bool);
    assert_eq!(series_bool_filter.data_internal(), &vec![None, None, None, Some(true)]);

    let series_empty: Series<i32> = Series::new_empty("empty".into(), DataType::Int32);
    let applied_empty = series_empty.apply(|opt_v| opt_v.map(|v| v + 1));
    assert_eq!(applied_empty.name(), "empty");
    assert_eq!(applied_empty.dtype(), DataType::Int32);
    assert!(applied_empty.is_empty());

    let series_only_none: Series<f64> = Series::new_from_options("nones".into(), vec![None, None]);
    let applied_nones = series_only_none.apply(|opt_v| {
        match opt_v {
            Some(_) => None,
            None => Some("missing".to_string()),
        }
    });
    assert_eq!(applied_nones.name(), "nones");
    assert_eq!(applied_nones.dtype(), DataType::String);
    assert_eq!(applied_nones.data_internal(), &vec![Some("missing".to_string()), Some("missing".to_string())]);
}

#[test]
fn test_series_string_ops() -> AxionResult<()> {
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

    let contains_o = series_str.str().contains("o")?;
    assert_eq!(contains_o.name(), "text_contains_o");
    assert_eq!(contains_o.dtype(), DataType::Bool);
    assert_eq!(
        contains_o.data_internal(),
        &vec![Some(true), Some(true), None, Some(false), Some(false), Some(true)]
    );

    let contains_xyz = series_str.str().contains("xyz")?;
    assert_eq!(contains_xyz.name(), "text_contains_xyz");
    assert_eq!(
        contains_xyz.data_internal(),
        &vec![Some(false), Some(false), None, Some(false), Some(false), Some(false)]
    );

    let contains_empty = series_str.str().contains("")?;
    assert_eq!(contains_empty.name(), "text_contains_");
    assert_eq!(
        contains_empty.data_internal(),
        &vec![Some(true), Some(true), None, Some(true), Some(true), Some(true)]
    );

    let starts_h = series_str.str().startswith("h")?;
    assert_eq!(starts_h.name(), "text_startswith_h");
    assert_eq!(starts_h.dtype(), DataType::Bool);
    assert_eq!(
        starts_h.data_internal(),
        &vec![Some(true), Some(false), None, Some(false), Some(false), Some(true)]
    );

    let starts_rust = series_str.str().startswith("rust")?;
    assert_eq!(starts_rust.name(), "text_startswith_rust");
    assert_eq!(
        starts_rust.data_internal(),
        &vec![Some(false), Some(false), None, Some(true), Some(false), Some(false)]
    );

    let starts_empty = series_str.str().startswith("")?;
    assert_eq!(starts_empty.name(), "text_startswith_");
    assert_eq!(
        starts_empty.data_internal(),
        &vec![Some(true), Some(true), None, Some(true), Some(true), Some(true)]
    );

    let lengths = series_str.str().str_len()?;
    assert_eq!(lengths.name(), "text_len");
    assert_eq!(lengths.dtype(), DataType::UInt32);
    assert_eq!(
        lengths.data_internal(),
        &vec![Some(5), Some(5), None, Some(4), Some(0), Some(10)]
    );

    let ends_o = series_str.str().endswith("o")?;
    assert_eq!(ends_o.name(), "text_endswith_o");
    assert_eq!(ends_o.dtype(), DataType::Bool);
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

    let ends_empty = series_str.str().endswith("")?;
    assert_eq!(ends_empty.name(), "text_endswith_");
    assert_eq!(
        ends_empty.data_internal(),
        &vec![Some(true), Some(true), None, Some(true), Some(true), Some(true)]
    );

    let replaced_l = series_str.str().replace("l", "X")?;
    assert_eq!(replaced_l.name(), "text_replace");
    assert_eq!(replaced_l.dtype(), DataType::String);
    assert_eq!(
        replaced_l.data_internal(),
        &vec![
            Some("heXXo".to_string()),
            Some("worXd".to_string()),
            None,
            Some("rust".to_string()),
            Some("".to_string()),
            Some("heXXo rust".to_string()),
        ]
    );

    let replaced_all = series_str.str().replace("hello", "goodbye")?;
    assert_eq!(replaced_all.name(), "text_replace");
    assert_eq!(
        replaced_all.data_internal(),
        &vec![
            Some("goodbye".to_string()),
            Some("world".to_string()),
            None,
            Some("rust".to_string()),
            Some("".to_string()),
            Some("goodbye rust".to_string()),
        ]
    );

    let series_mixed_case = Series::new_from_options(
        "mixed".into(),
        vec![Some("Hello".to_string()), Some("WORLD".to_string()), None, Some("RuSt".to_string())]
    );

    let lower = series_mixed_case.str().to_lowercase()?;
    assert_eq!(lower.name(), "mixed_lower");
    assert_eq!(lower.dtype(), DataType::String);
    assert_eq!(
        lower.data_internal(),
        &vec![Some("hello".to_string()), Some("world".to_string()), None, Some("rust".to_string())]
    );

    let upper = series_mixed_case.str().to_uppercase()?;
    assert_eq!(upper.name(), "mixed_upper");
    assert_eq!(upper.dtype(), DataType::String);
    assert_eq!(
        upper.data_internal(),
        &vec![Some("HELLO".to_string()), Some("WORLD".to_string()), None, Some("RUST".to_string())]
    );

    let series_whitespace = Series::new_from_options(
        "whitespace".into(),
        vec![
            Some("  hello ".to_string()),
            Some("\tworld\n".to_string()),
            None,
            Some(" rust ".to_string()),
            Some("   ".to_string()),
            Some("no_space".to_string()),
        ]
    );

    let stripped = series_whitespace.str().strip()?;
    assert_eq!(stripped.name(), "whitespace_strip");
    assert_eq!(stripped.dtype(), DataType::String);
    assert_eq!(
        stripped.data_internal(),
        &vec![
            Some("hello".to_string()),
            Some("world".to_string()),
            None,
            Some("rust".to_string()),
            Some("".to_string()),
            Some("no_space".to_string()),
        ]
    );

    let lstripped = series_whitespace.str().lstrip()?;
    assert_eq!(lstripped.name(), "whitespace_lstrip");
    assert_eq!(
        lstripped.data_internal(),
        &vec![
            Some("hello ".to_string()),
            Some("world\n".to_string()),
            None,
            Some("rust ".to_string()),
            Some("".to_string()),
            Some("no_space".to_string()),
        ]
    );

    let rstripped = series_whitespace.str().rstrip()?;
    assert_eq!(rstripped.name(), "whitespace_rstrip");
    assert_eq!(
        rstripped.data_internal(),
        &vec![
            Some("  hello".to_string()),
            Some("\tworld".to_string()),
            None,
            Some(" rust".to_string()),
            Some("".to_string()),
            Some("no_space".to_string()),
        ]
    );

    let empty_str_series: Series<String> = Series::new_empty("empty_text".into(), DataType::String);

    let empty_endswith = empty_str_series.str().endswith("a")?;
    assert!(empty_endswith.is_empty());
    assert_eq!(empty_endswith.dtype(), DataType::Bool);

    let empty_replace = empty_str_series.str().replace("a", "b")?;
    assert!(empty_replace.is_empty());
    assert_eq!(empty_replace.dtype(), DataType::String);

    let empty_lower = empty_str_series.str().to_lowercase()?;
    assert!(empty_lower.is_empty());
    assert_eq!(empty_lower.dtype(), DataType::String);

    let empty_upper = empty_str_series.str().to_uppercase()?;
    assert!(empty_upper.is_empty());
    assert_eq!(empty_upper.dtype(), DataType::String);

    let empty_strip = empty_str_series.str().strip()?;
    assert!(empty_strip.is_empty());
    assert_eq!(empty_strip.dtype(), DataType::String);

    let empty_lstrip = empty_str_series.str().lstrip()?;
    assert!(empty_lstrip.is_empty());
    assert_eq!(empty_lstrip.dtype(), DataType::String);

    let empty_rstrip = empty_str_series.str().rstrip()?;
    assert!(empty_rstrip.is_empty());
    assert_eq!(empty_rstrip.dtype(), DataType::String);

    Ok(())
}

#[test]
fn test_series_is_null_and_fill_null() {
    // 使用 new_from_options 来创建包含 Option 的 Series，更清晰
    let s = Series::<i32>::new_from_options("a".into(), vec![Some(1), None, Some(3), None]);
    let is_null = s.is_null();
    // is_null 返回的 Series 内部数据也是 Vec<Option<bool>>
    assert_eq!(is_null.data_internal(), &vec![Some(false), Some(true), Some(false), Some(true)]);

    let filled = s.fill_null(99);
    // fill_null 返回的 Series 内部数据也是 Vec<Option<T>>
    assert_eq!(filled.data_internal(), &vec![Some(1), Some(99), Some(3), Some(99)]);
}

#[test]
fn test_series_par_apply_simple_multiplication() {
    let s1 = Series::new_from_options(
        "integers".to_string(),
        vec![Some(1i32), Some(2), None, Some(4), Some(5)],
    );
    let s_doubled = s1.par_apply(|opt_x| opt_x.map(|x| x * 2));

    assert_eq!(s_doubled.name(), "integers");
    assert_eq!(s_doubled.dtype(), DataType::Int32); // Assuming T=i32, U=i32
    assert_eq!(
        s_doubled.data_internal(), // Or your method to get internal Vec<Option<T>>
        &vec![Some(2i32), Some(4), None, Some(8), Some(10)]
    );
}

#[test]
fn test_series_par_apply_type_conversion_to_string() {
    let s1 = Series::new_from_options("numbers".to_string(), vec![Some(10i32), None, Some(30)]);
    let s_stringified = s1.par_apply(|opt_x| opt_x.map(|x| x.to_string()));

    assert_eq!(s_stringified.name(), "numbers");
    assert_eq!(s_stringified.dtype(), DataType::String);
    assert_eq!(
        s_stringified.data_internal(),
        &vec![Some("10".to_string()), None, Some("30".to_string())]
    );
}

#[test]
fn test_series_par_apply_on_empty_series() {
    let s_empty_i32: Series<i32> = Series::new_empty("empty_i32".to_string(), DataType::Int32);
    let s_applied_i32 = s_empty_i32.par_apply(|opt_x: Option<&i32>| opt_x.map(|x| x + 100));

    assert!(s_applied_i32.is_empty());
    assert_eq!(s_applied_i32.name(), "empty_i32");
    assert_eq!(s_applied_i32.dtype(), DataType::Int32);

    let s_empty_str: Series<String> = Series::new_empty("empty_str".to_string(), DataType::String);
    // Example: apply a function that returns a different type (usize -> Int64)
    let s_applied_len = s_empty_str.par_apply(|opt_s: Option<&String>| opt_s.map(|s| s.len() as i64));

    assert!(s_applied_len.is_empty());
    assert_eq!(s_applied_len.name(), "empty_str");
    assert_eq!(s_applied_len.dtype(), DataType::Int64); // U is i64
}

#[test]
fn test_series_par_apply_all_none() {
    let s1: Series<f64> = Series::new_from_options(
        "all_none_f64".to_string(),
        vec![None, None, None],
    );
    let s_processed = s1.par_apply(|opt_x| opt_x.map(|x| x.powi(2))); // Function won't be called on Some

    assert_eq!(s_processed.name(), "all_none_f64");
    assert_eq!(s_processed.dtype(), DataType::Float64);
    assert_eq!(
        s_processed.data_internal(),
        &vec![None, None, None]
    );
}