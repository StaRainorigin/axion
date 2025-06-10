use axion_data::{df, AxionError, DataType, AxionResult, DataFrame, SeriesTrait, Series};

#[test]
fn test_df_macro_creation_and_properties() -> Result<(), AxionError> {
    let df = df![
        "integers" => &[10, 20, 30],
        "floats" => vec![1.1, 2.2, 3.3],
        "strings" => &["a", "b", "c"],
    ]?;

    assert_eq!(df.shape(), (3, 3));
    assert_eq!(df.height(), 3);
    assert_eq!(df.width(), 3);
    assert_eq!(df.columns_names(), vec!["integers", "floats", "strings"]);

    let schema = df.schema();
    assert_eq!(schema.get("integers"), Some(&DataType::Int32));
    assert_eq!(schema.get("floats"), Some(&DataType::Float64));
    assert_eq!(schema.get("strings"), Some(&DataType::String));

    let int_col = df.column("integers")?;
    assert_eq!(int_col.len(), 3);
    assert_eq!(int_col.dtype(), DataType::Int32);

    let int_series = df.downcast_column::<i32>("integers")?;
    assert_eq!(int_series.get(0), Some(&10));

    let str_series = df.downcast_column::<String>("strings")?;
    assert_eq!(str_series.get(0), Some(&"a".to_string()));

    Ok(())
}

#[test]
fn test_df_macro_empty() -> Result<(), AxionError> {
    let df_empty = df![]?;
    assert_eq!(df_empty.shape(), (0, 0));
    assert!(df_empty.schema().is_empty());
    Ok(())
}

#[test]
fn test_df_macro_duplicate_column_name() {
    let result = df![
        "a" => &[1, 2],
        "b" => &[3, 4],
        "a" => &[5, 6]
    ];
    assert!(matches!(result, Err(AxionError::DuplicateColumnName(name)) if name == "a"));
}

#[test]
fn test_df_macro_mismatched_lengths() {
    let result = df![
        "a" => &[1, 2, 3],
        "b" => &[4, 5]
    ];
    assert!(matches!(result, Err(AxionError::MismatchedLengths { .. })));
}

fn create_test_df() -> AxionResult<DataFrame> {
    df! {
        "col_a" => vec![Some(1), Some(2), None, Some(4)],
        "col_b" => vec![Some("a".to_string()), Some("b".to_string()), Some("c".to_string()), Some("d".to_string())],
        "col_c" => vec![Some(true), None, Some(false), Some(true)]
    }
}

#[test]
fn test_new_dataframe_success() {
    let df = create_test_df().unwrap();
    assert_eq!(df.shape(), (4, 3));
    assert_eq!(df.height(), 4);
    assert_eq!(df.width(), 3);
    assert_eq!(df.columns_names(), vec!["col_a", "col_b", "col_c"]);
    assert_eq!(df.dtypes(), vec![DataType::Int32, DataType::String, DataType::Bool]);
}

#[test]
fn test_new_dataframe_mismatched_lengths() {
    let columns: Vec<Box<dyn SeriesTrait>> = vec![
        Box::new(Series::new_from_options("a".into(), vec![Some(1), Some(2)])),
        Box::new(Series::new_from_options("b".into(), vec![Some("x".to_string())])),
    ];
    let df_res = DataFrame::new(columns);
    assert!(df_res.is_err());
    match df_res.err().unwrap() {
        AxionError::MismatchedLengths { expected, found, name } => {
            assert_eq!(expected, 2);
            assert_eq!(found, 1);
            assert_eq!(name, "b");
        }
        _ => panic!("Expected MismatchedLengths error"),
    }
}

#[test]
fn test_new_dataframe_duplicate_names() {
    let columns: Vec<Box<dyn SeriesTrait>> = vec![
        Box::new(Series::new_from_options("a".into(), vec![Some(1)])),
        Box::new(Series::new_from_options("a".into(), vec![Some("x".to_string())])),
    ];
    let df_res = DataFrame::new(columns);
    assert!(df_res.is_err());
    match df_res.err().unwrap() {
        AxionError::DuplicateColumnName(name) => {
            assert_eq!(name, "a");
        }
        _ => panic!("Expected DuplicateColumnName error"),
    }
}

#[test]
fn test_column_access() {
    let df = create_test_df().unwrap();

    let col_b_res = df.column("col_b");
    assert!(col_b_res.is_ok());
    let col_b = col_b_res.unwrap();
    assert_eq!(col_b.name(), "col_b");
    assert_eq!(col_b.dtype(), DataType::String);

    let col_d = df.column("col_d");
    assert!(col_d.is_err());
    match col_d.err().unwrap() {
        AxionError::ColumnNotFound(name) => assert_eq!(name, "col_d"),
        _ => panic!("Expected ColumnNotFound error"),
    }

    let col_a = df.column_at(0);
    assert!(col_a.is_ok());
    assert_eq!(col_a.unwrap().name(), "col_a");

    let col_3 = df.column_at(3);
    assert!(col_3.is_err());
    match col_3.err().unwrap() {
        AxionError::ColumnNotFound(name) => assert!(name.contains("index 3")),
        _ => panic!("Expected ColumnNotFound error"),
    }
}

#[test]
fn test_downcast_column() {
    let df = create_test_df().unwrap();

    let col_a_typed: AxionResult<&Series<i32>> = df.downcast_column("col_a");
    assert!(col_a_typed.is_ok());
    assert_eq!(col_a_typed.unwrap().data_internal(), &vec![Some(1), Some(2), None, Some(4)]);

    let col_a_as_str: AxionResult<&Series<String>> = df.downcast_column("col_a");
    assert!(col_a_as_str.is_err());
    match col_a_as_str.err().unwrap() {
        AxionError::TypeMismatch { expected, found, name } => {
            assert_eq!(expected, DataType::String);
            assert_eq!(found, DataType::Int32);
            assert_eq!(name, "col_a");
        }
        _ => panic!("Expected TypeMismatch error"),
    }

    let col_d_typed: AxionResult<&Series<i32>> = df.downcast_column("col_d");
    assert!(col_d_typed.is_err());
    match col_d_typed.err().unwrap() {
        AxionError::ColumnNotFound(name) => assert_eq!(name, "col_d"),
        _ => panic!("Expected ColumnNotFound error"),
    }
}

#[test]
fn test_select() {
    let df = create_test_df().unwrap();
    let selected_df = df.select(&["col_c", "col_a"]).unwrap();
    assert_eq!(selected_df.shape(), (4, 2));
    assert_eq!(selected_df.columns_names(), vec!["col_c", "col_a"]);
    assert_eq!(selected_df.dtypes(), vec![DataType::Bool, DataType::Int32]);

    let select_err = df.select(&["col_a", "col_d"]);
    assert!(select_err.is_err());
    match select_err.err().unwrap() {
        AxionError::ColumnNotFound(name) => assert_eq!(name, "col_d"),
        _ => panic!("Expected ColumnNotFound error"),
    }
}

#[test]
fn test_drop() {
    let df = create_test_df().unwrap();
    let dropped_df = df.drop("col_b").unwrap();
    assert_eq!(dropped_df.shape(), (4, 2));
    assert_eq!(dropped_df.columns_names(), vec!["col_a", "col_c"]);

    let drop_err = df.drop("col_d");
    assert!(drop_err.is_err());
    match drop_err.err().unwrap() {
        AxionError::ColumnNotFound(name) => assert_eq!(name, "col_d"),
        _ => panic!("Expected ColumnNotFound error"),
    }
}

#[test]
fn test_head() {
    let df = create_test_df().unwrap();
    let head_df = df.head(2);
    assert_eq!(head_df.shape(), (2, 3));
    let col_a: &Series<i32> = head_df.downcast_column("col_a").unwrap();
    assert_eq!(col_a.data_internal(), &vec![Some(1), Some(2)]);

    let head_all = df.head(5);
    assert_eq!(head_all.shape(), (4, 3));
    assert_eq!(head_all.height(), df.height());
}

#[test]
fn test_tail() {
    let df = create_test_df().unwrap();
    let tail_df = df.tail(2);
    assert_eq!(tail_df.shape(), (2, 3));
    let col_a: &Series<i32> = tail_df.downcast_column("col_a").unwrap();
    assert_eq!(col_a.data_internal(), &vec![None, Some(4)]);

    let tail_all = df.tail(5);
    assert_eq!(tail_all.shape(), (4, 3));
    assert_eq!(tail_all.height(), df.height());
}

#[test]
fn test_join_column_name_conflict() -> AxionResult<()> {
    let df_left = df! {
        "id" => vec!["a", "b", "c", "d"],
        "value" => vec![1, 2, 3, 4],
        "left_only" => vec![10, 20, 30, 40],
    }?;

    let df_right = df! {
        "id" => vec!["c", "d", "e", "f"],
        "value" => vec![300, 400, 500, 600],
        "right_only" => vec![true, false, true, false],
    }?;

    // Inner Join
    let inner_df = df_left.inner_join(&df_right, "id", "id")?;
    assert_eq!(inner_df.shape(), (2, 5));
    assert_eq!(
        inner_df.columns_names(),
        vec!["id", "value", "left_only", "value_right", "right_only"]
    );
    let id_col: &Series<String> = inner_df.downcast_column("id")?;
    assert_eq!(id_col.data_internal(), &vec![Some("c".to_string()), Some("d".to_string())]);
    let value_col: &Series<i32> = inner_df.downcast_column("value")?;
    assert_eq!(value_col.data_internal(), &vec![Some(3), Some(4)]);
    let value_right_col: &Series<i32> = inner_df.downcast_column("value_right")?;
    assert_eq!(value_right_col.data_internal(), &vec![Some(300), Some(400)]);

    // Left Join
    let left_df = df_left.left_join(&df_right, "id", "id")?;
    assert_eq!(left_df.shape(), (4, 5));
    assert_eq!(
        left_df.columns_names(),
        vec!["id", "value", "left_only", "value_right", "right_only"]
    );
    let value_right_col_left: &Series<i32> = left_df.downcast_column("value_right")?;
    assert_eq!(value_right_col_left.data_internal(), &vec![None, None, Some(300), Some(400)]);

    // Right Join
    let right_df = df_left.right_join(&df_right, "id", "id")?;
    assert_eq!(right_df.shape(), (4, 5));
    assert_eq!(
        right_df.columns_names(),
        vec!["id", "value", "right_only", "value_left", "left_only"]
    );
    let value_left_col: &Series<i32> = right_df.downcast_column("value_left")?;
    assert_eq!(value_left_col.data_internal(), &vec![Some(3), Some(4), None, None]);
    let value_col_right: &Series<i32> = right_df.downcast_column("value")?;
    assert_eq!(value_col_right.data_internal(), &vec![Some(300), Some(400), Some(500), Some(600)]);

    // Outer Join
    let outer_df = df_left.outer_join(&df_right, "id", "id")?;
    assert_eq!(outer_df.shape(), (6, 5));
    assert_eq!(
        outer_df.columns_names(),
        vec!["id", "value", "left_only", "value_right", "right_only"]
    );
    let id_col_outer: &Series<String> = outer_df.downcast_column("id")?;
    assert_eq!(id_col_outer.len(), 6);
    let value_col_outer: &Series<i32> = outer_df.downcast_column("value")?;
    assert_eq!(value_col_outer.len(), 6);
    let value_right_col_outer: &Series<i32> = outer_df.downcast_column("value_right")?;
    assert_eq!(value_right_col_outer.len(), 6);

    Ok(())
}

fn create_sample_df_for_col_ops() -> AxionResult<DataFrame> {
    df![
        "col_a" => vec![Some(10), Some(20), Some(30)],
        "col_b" => vec![Some("x".to_string()), Some("y".to_string()), Some("z".to_string())],
        "col_c" => vec![Some(true), Some(false), Some(true)]
    ]
}

#[test]
fn test_df_add_column() -> AxionResult<()> {
    let mut df = create_sample_df_for_col_ops()?;
    let initial_width = df.width();
    let initial_height = df.height();

    // 成功添加新列
    let new_series_f64: Series<f64> = Series::new_from_options("col_d_f64".into(), vec![Some(1.1), Some(2.2), Some(3.3)]);
    df.add_column(Box::new(new_series_f64))?;
    assert_eq!(df.width(), initial_width + 1);
    assert_eq!(df.height(), initial_height);
    assert_eq!(df.columns_names().last(), Some(&"col_d_f64"));
    assert_eq!(df.schema().get("col_d_f64"), Some(&DataType::Float64));
    let fetched_col = df.column("col_d_f64")?;
    assert_eq!(fetched_col.len(), initial_height);

    // 添加同名列应该失败
    let series_dup_name: Series<i32> = Series::new_from_options("col_a".into(), vec![Some(0), Some(0), Some(0)]);
    match df.add_column(Box::new(series_dup_name)) {
        Err(AxionError::DuplicateColumnName(name)) => {
            assert_eq!(name, "col_a");
        }
        _ => panic!("Expected DuplicateColumnName error when adding column with existing name"),
    }
    assert_eq!(df.width(), initial_width + 1);

    // 添加长度不匹配的列应该失败
    let series_wrong_len: Series<i32> = Series::new_from_options("col_e_short".into(), vec![Some(100), Some(200)]);
    match df.add_column(Box::new(series_wrong_len)) {
        Err(AxionError::MismatchedLengths { expected, found, name }) => {
            assert_eq!(expected, initial_height);
            assert_eq!(found, 2);
            assert_eq!(name, "col_e_short");
        }
        _ => panic!("Expected MismatchedLengths error when adding column with wrong length"),
    }
    assert_eq!(df.width(), initial_width + 1);

    Ok(())
}

#[test]
fn test_df_add_column_to_empty_dataframe() -> AxionResult<()> {
    let mut df = DataFrame::new(vec![])?;
    assert_eq!(df.shape(), (0, 0));

    // 添加第一列
    let series1: Series<i32> = Series::new_from_options("first_col".into(), vec![Some(10), Some(20)]);
    df.add_column(Box::new(series1))?;
    assert_eq!(df.shape(), (2, 1));
    assert_eq!(df.height(), 2);
    assert_eq!(df.width(), 1);
    assert_eq!(df.column("first_col")?.len(), 2);

    // 添加第二列
    let series2_ok: Series<String> = Series::new_from_options("second_col".into(), vec![Some("a".to_string()), Some("b".to_string())]);
    df.add_column(Box::new(series2_ok))?;
    assert_eq!(df.shape(), (2, 2));

    // 添加长度不匹配的列应该失败
    let series3_wrong_len: Series<bool> = Series::new_from_options("third_col_wrong".into(), vec![Some(true)]);
     match df.add_column(Box::new(series3_wrong_len)) {
        Err(AxionError::MismatchedLengths { expected, .. }) => {
            assert_eq!(expected, 2);
        }
        _ => panic!("Expected MismatchedLengths error"),
    }
    Ok(())
}

#[test]
fn test_df_drop_column() -> AxionResult<()> {
    let mut df = create_sample_df_for_col_ops()?;
    let initial_width = df.width();

    // 成功删除存在的列
    let dropped_col_c = df.drop_column("col_c")?;
    assert_eq!(df.width(), initial_width - 1);
    assert!(df.column("col_c").is_err());
    assert_eq!(df.schema().get("col_c"), None);
    assert_eq!(dropped_col_c.name(), "col_c");
    assert_eq!(dropped_col_c.dtype(), DataType::Bool);

    // 删除不存在的列应该失败
    match df.drop_column("non_existent_col") {
        Err(AxionError::ColumnNotFound(name)) => {
            assert_eq!(name, "non_existent_col");
        }
        _ => panic!("Expected ColumnNotFound error when dropping non-existent column"),
    }
    assert_eq!(df.width(), initial_width - 1);

    // 删除所有列
    let _ = df.drop_column("col_b")?;
    let _ = df.drop_column("col_a")?;
    assert_eq!(df.width(), 0);
    assert_eq!(df.height(), 0);
    assert!(df.columns_names().is_empty());
    assert!(df.schema().is_empty());

    // 从空DataFrame删除列应该失败
    match df.drop_column("any_col") {
        Err(AxionError::ColumnNotFound(name)) => {
            assert_eq!(name, "any_col");
        }
        _ => panic!("Expected ColumnNotFound error when dropping from empty DataFrame"),
    }
    Ok(())
}

#[test]
fn test_df_rename_column() -> AxionResult<()> {
    let mut df = create_sample_df_for_col_ops()?;

    // 成功重命名列
    let old_name = "col_b";
    let new_name = "col_b_renamed";
    df.rename_column(old_name, new_name)?;
    assert!(df.column(old_name).is_err());
    let renamed_col = df.column(new_name)?;
    assert_eq!(renamed_col.name(), new_name);
    assert_eq!(renamed_col.dtype(), DataType::String);
    assert_eq!(df.schema().get(old_name), None);
    assert_eq!(df.schema().get(new_name), Some(&DataType::String));

    // 重命名为已存在的列名应该失败
    match df.rename_column("col_a", new_name) {
        Err(AxionError::DuplicateColumnName(name)) => {
            assert_eq!(name, new_name);
        }
        _ => panic!("Expected DuplicateColumnName error when renaming to an existing column name"),
    }

    // 重命名不存在的列应该失败
    match df.rename_column("non_existent_col", "any_new_name") {
        Err(AxionError::ColumnNotFound(name)) => {
            assert_eq!(name, "non_existent_col");
        }
        _ => panic!("Expected ColumnNotFound error when renaming a non-existent column"),
    }

    // 重命名为当前名称应该成功
    df.rename_column("col_a", "col_a")?;
    assert!(df.column("col_a").is_ok());

    // 检查列顺序保持不变
    let expected_names_after_rename = vec!["col_a", "col_b_renamed", "col_c"];
    assert_eq!(df.columns_names(), expected_names_after_rename);

    Ok(())
}