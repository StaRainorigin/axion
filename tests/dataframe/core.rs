use axion_data::{df, AxionError, DataType, AxionResult, DataFrame, SeriesTrait, Series};

#[test]
fn test_df_macro_creation_and_properties() -> Result<(), AxionError> {
    // 使用 df! 宏创建 DataFrame
    let df = df![
        "integers" => &[10, 20, 30],
        "floats" => vec![1.1, 2.2, 3.3],
        "strings" => &["a", "b", "c"],
    ]?;

    // 验证形状
    assert_eq!(df.shape(), (3, 3), "DataFrame shape should be (3, 3)");
    assert_eq!(df.height(), 3, "DataFrame height should be 3");
    assert_eq!(df.width(), 3, "DataFrame width should be 3");

    // 验证列名
    assert_eq!(df.columns_names(), vec!["integers", "floats", "strings"], "Column names should match");

    // 验证 Schema
    let schema = df.schema();
    assert_eq!(schema.get("integers"), Some(&DataType::Int32), "Type of 'integers' should be Int32");
    assert_eq!(schema.get("floats"), Some(&DataType::Float64), "Type of 'floats' should be Float64");
    assert_eq!(schema.get("strings"), Some(&DataType::String), "Type of 'strings' should be String");

    // 验证通过列名访问
    let int_col = df.column("integers")?;
    assert_eq!(int_col.len(), 3);
    assert_eq!(int_col.dtype(), DataType::Int32);

    // 验证向下转型和数据访问
    let int_series = df.downcast_column::<i32>("integers")?;
    assert_eq!(int_series.get(0), Some(&10));

    let str_series = df.downcast_column::<String>("strings")?;
    assert_eq!(str_series.get(0), Some(&"a".to_string()));

    Ok(())
}

#[test]
fn test_df_macro_empty() -> Result<(), AxionError> {
    let df_empty = df![]?;
    assert_eq!(df_empty.shape(), (0, 0), "Empty DataFrame shape should be (0, 0)");
    assert!(df_empty.schema().is_empty(), "Empty DataFrame schema should be empty");
    Ok(())
}

#[test]
fn test_df_macro_duplicate_column_name() {
    let result = df![
        "a" => &[1, 2],
        "b" => &[3, 4],
        "a" => &[5, 6] // 重复的列名 "a"
    ];
    assert!(matches!(result, Err(AxionError::DuplicateColumnName(name)) if name == "a"));
}

#[test]
fn test_df_macro_mismatched_lengths() {
    let result = df![
        "a" => &[1, 2, 3],
        "b" => &[4, 5] // 长度不匹配
    ];
    assert!(matches!(result, Err(AxionError::MismatchedLengths { .. })));
}

// 辅助函数创建测试 DataFrame
fn create_test_df() -> AxionResult<DataFrame> {
    df! {
        "col_a" => vec![Some(1), Some(2), None, Some(4)],
        "col_b" => vec![Some("a".to_string()), Some("b".to_string()), Some("c".to_string()), Some("d".to_string())],
        "col_c" => vec![Some(true), None, Some(false), Some(true)]
    }
}

#[test]
fn test_new_dataframe_success() {
    let df_res = create_test_df();
    assert!(df_res.is_ok());
    let df = df_res.unwrap();
    assert_eq!(df.shape(), (4, 3));
    assert_eq!(df.height(), 4);
    assert_eq!(df.width(), 3);
    assert_eq!(df.columns_names(), vec!["col_a", "col_b", "col_c"]);
    assert_eq!(df.dtypes(), vec![DataType::Int32, DataType::String, DataType::Bool]);
}

#[test]
fn test_new_dataframe_mismatched_lengths() {
    let columns: Vec<Box<dyn SeriesTrait>> = vec![
        // 使用 new_from_options 因为输入是 Vec<Option<T>>
        Box::new(Series::new_from_options("a".into(), vec![Some(1), Some(2)])),
        Box::new(Series::new_from_options("b".into(), vec![Some("x".to_string())])), // 长度不匹配
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
        // --- 修改这里 ---
        // 使用 new_from_options 处理 Vec<Option<T>>，并用 .into() 转 &str 为 String
        Box::new(Series::new_from_options("a".into(), vec![Some(1)])),
        // --- 修改这里 ---
        Box::new(Series::new_from_options("a".into(), vec![Some("x".to_string())])), // 重复名称
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

    // 按名称获取
    let col_b_res = df.column("col_b");
    assert!(col_b_res.is_ok());
    let col_b = col_b_res.unwrap();
    assert_eq!(col_b.name(), "col_b");
    assert_eq!(col_b.dtype(), DataType::String);

    // 按名称获取不存在的列
    let col_d = df.column("col_d");
    assert!(col_d.is_err());
    match col_d.err().unwrap() {
        AxionError::ColumnNotFound(name) => assert_eq!(name, "col_d"),
        _ => panic!("Expected ColumnNotFound error"),
    }

    // 按索引获取
    let col_a = df.column_at(0);
    assert!(col_a.is_ok());
    assert_eq!(col_a.unwrap().name(), "col_a");

    // 按索引获取越界
    let col_3 = df.column_at(3);
    assert!(col_3.is_err());
    match col_3.err().unwrap() {
        AxionError::ColumnNotFound(name) => assert!(name.contains("index 3")),
        _ => panic!("Expected ColumnNotFound error"),
    }
}

#[test]
fn test_downcast_column() {
    let df = create_test_df().unwrap(); // 使用 df! 宏创建

    // 成功向下转型
    let col_a_typed: AxionResult<&Series<i32>> = df.downcast_column("col_a"); // T 应该是 i32
    assert!(col_a_typed.is_ok());
    // 访问内部数据
    assert_eq!(col_a_typed.unwrap().data_internal(), &vec![Some(1), Some(2), None, Some(4)]);

    // 类型不匹配
    let col_a_as_str: AxionResult<&Series<String>> = df.downcast_column("col_a"); // T 是 String
    assert!(col_a_as_str.is_err());
    match col_a_as_str.err().unwrap() {
        AxionError::TypeMismatch { expected, found, name } => {
            assert_eq!(expected, DataType::String);
            assert_eq!(found, DataType::Int32); // 确认 found 是 Int32
            assert_eq!(name, "col_a");
        }
        _ => panic!("Expected TypeMismatch error"),
    }

    // 列不存在
    let col_d_typed: AxionResult<&Series<i32>> = df.downcast_column("col_d");
    assert!(col_d_typed.is_err());
    match col_d_typed.err().unwrap() {
        AxionError::ColumnNotFound(name) => assert_eq!(name, "col_d"), // downcast 内部调用 column
        _ => panic!("Expected ColumnNotFound error"),
    }
}

#[test]
fn test_select() {
    let df = create_test_df().unwrap();
    let selected_df_res = df.select(&["col_c", "col_a"]);
    assert!(selected_df_res.is_ok());
    let selected_df = selected_df_res.unwrap();
    assert_eq!(selected_df.shape(), (4, 2));
    assert_eq!(selected_df.columns_names(), vec!["col_c", "col_a"]); // 检查顺序
    assert_eq!(selected_df.dtypes(), vec![DataType::Bool, DataType::Int32]);

    // 选择不存在的列
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
    let dropped_df_res = df.drop("col_b");
    assert!(dropped_df_res.is_ok());
    let dropped_df = dropped_df_res.unwrap();
    assert_eq!(dropped_df.shape(), (4, 2));
    assert_eq!(dropped_df.columns_names(), vec!["col_a", "col_c"]);

    // drop 不存在的列
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

    let head_all = df.head(5); // n > height
    assert_eq!(head_all.shape(), (4, 3));
    assert_eq!(head_all.height(), df.height()); // 应该是克隆
}

#[test]
fn test_tail() {
    let df =create_test_df().unwrap();
    let tail_df = df.tail(2);
    assert_eq!(tail_df.shape(), (2, 3));
    let col_a: &Series<i32> = tail_df.downcast_column("col_a").unwrap();
    assert_eq!(col_a.data_internal(), &vec![None, Some(4)]);

    let tail_all = df.tail(5); // n > height
    assert_eq!(tail_all.shape(), (4, 3));
    assert_eq!(tail_all.height(), df.height()); // 应该是克隆
}

#[test]
fn test_join_column_name_conflict() -> AxionResult<()> {
    let df_left = df! {
        "id" => vec!["a", "b", "c", "d"],
        "value" => vec![1, 2, 3, 4], // 冲突列
        "left_only" => vec![10, 20, 30, 40],
    }?;

    let df_right = df! {
        "id" => vec!["c", "d", "e", "f"],
        "value" => vec![300, 400, 500, 600], // 冲突列
        "right_only" => vec![true, false, true, false],
    }?;

    // --- 1. Inner Join ---
    let inner_df = df_left.inner_join(&df_right, "id", "id")?;
    // 预期列: id, value, left_only, value_right, right_only
    // 预期行: 匹配 "c", "d"
    assert_eq!(inner_df.shape(), (2, 5));
    assert_eq!(
        inner_df.columns_names(),
        vec!["id", "value", "left_only", "value_right", "right_only"] // 检查 value_right
    );
    // 检查数据 (可选但推荐)
    let id_col: &Series<String> = inner_df.downcast_column("id")?;
    assert_eq!(id_col.data_internal(), &vec![Some("c".to_string()), Some("d".to_string())]);
    let value_col: &Series<i32> = inner_df.downcast_column("value")?;
    assert_eq!(value_col.data_internal(), &vec![Some(3), Some(4)]);
    let value_right_col: &Series<i32> = inner_df.downcast_column("value_right")?;
    assert_eq!(value_right_col.data_internal(), &vec![Some(300), Some(400)]);

    // --- 2. Left Join ---
    let left_df = df_left.left_join(&df_right, "id", "id")?;
    // 预期列: id, value, left_only, value_right, right_only
    // 预期行: 4 (来自左侧)
    assert_eq!(left_df.shape(), (4, 5));
    assert_eq!(
        left_df.columns_names(),
        vec!["id", "value", "left_only", "value_right", "right_only"] // 检查 value_right
    );
    // 检查数据 (可选)
    let value_right_col_left: &Series<i32> = left_df.downcast_column("value_right")?;
    assert_eq!(value_right_col_left.data_internal(), &vec![None, None, Some(300), Some(400)]); // a, b 不匹配 -> None

    // --- 3. Right Join ---
    let right_df = df_left.right_join(&df_right, "id", "id")?;
    // 预期列: id, value, right_only, value_left, left_only (右侧列优先，左侧冲突列加_left后缀)
    // 预期行: 4 (来自右侧)
    assert_eq!(right_df.shape(), (4, 5));
    assert_eq!(
        right_df.columns_names(),
        vec!["id", "value", "right_only", "value_left", "left_only"]
    );
        // 检查数据 (可选)
    let value_left_col: &Series<i32> = right_df.downcast_column("value_left")?;
    assert_eq!(value_left_col.data_internal(), &vec![Some(3), Some(4), None, None]); // c, d 匹配; e, f 不匹配 -> None
    // 可以加一个检查，确保右侧原始 value 列的数据正确
    let value_col_right: &Series<i32> = right_df.downcast_column("value")?;
    assert_eq!(value_col_right.data_internal(), &vec![Some(300), Some(400), Some(500), Some(600)]);

    // --- 4. Outer Join ---
    let outer_df = df_left.outer_join(&df_right, "id", "id")?;
    // 预期列: id, value, left_only, value_right, right_only
    // 预期行: 6 (a, b, c, d, e, f)
    assert_eq!(outer_df.shape(), (6, 5));
        assert_eq!(
        outer_df.columns_names(),
        vec!["id", "value", "left_only", "value_right", "right_only"] // 检查 value_right
    );
    // 检查数据 (可选)
    let id_col_outer: &Series<String> = outer_df.downcast_column("id")?;
    // 注意：outer_join 的行顺序取决于实现细节 (先处理左边还是右边)
    // 这里假设先处理左边匹配/仅左，再处理仅右
    assert_eq!(id_col_outer.len(), 6); // 简单检查长度
    let value_col_outer: &Series<i32> = outer_df.downcast_column("value")?;
    assert_eq!(value_col_outer.len(), 6);
    let value_right_col_outer: &Series<i32> = outer_df.downcast_column("value_right")?;
    assert_eq!(value_right_col_outer.len(), 6);

    Ok(())
}

// 辅助函数，用于创建后续测试中会用到的 DataFrame 实例
// 你可以根据你的 df! 宏或者 DataFrame::new 的具体用法来调整它
fn create_sample_df_for_col_ops() -> AxionResult<DataFrame> {
    df![
        "col_a" => vec![Some(10), Some(20), Some(30)], // 假设 df! 宏能正确处理
        "col_b" => vec![Some("x".to_string()), Some("y".to_string()), Some("z".to_string())],
        "col_c" => vec![Some(true), Some(false), Some(true)]
    ]
}

#[test]
fn test_df_add_column() -> AxionResult<()> {
    let mut df = create_sample_df_for_col_ops()?;
    let initial_width = df.width();
    let initial_height = df.height();

    // 1. 成功添加新列
    // 使用 new_from_options 因为输入是 Vec<Option<f64>>
    let new_series_f64: Series<f64> = Series::new_from_options("col_d_f64".into(), vec![Some(1.1), Some(2.2), Some(3.3)]);
    df.add_column(Box::new(new_series_f64))?;
    assert_eq!(df.width(), initial_width + 1, "Width should increase by 1 after adding a column");
    assert_eq!(df.height(), initial_height, "Height should remain the same");
    assert_eq!(df.columns_names().last(), Some(&"col_d_f64"), "New column name should be last");
    assert_eq!(df.schema().get("col_d_f64"), Some(&DataType::Float64), "New column type should be in schema");
    let fetched_col = df.column("col_d_f64")?;
    assert_eq!(fetched_col.len(), initial_height);

    // 2. 尝试添加同名列 (应该失败)
    // 使用 new_from_options 因为输入是 Vec<Option<i32>>
    let series_dup_name: Series<i32> = Series::new_from_options("col_a".into(), vec![Some(0), Some(0), Some(0)]);
    match df.add_column(Box::new(series_dup_name)) {
        Err(AxionError::DuplicateColumnName(name)) => {
            assert_eq!(name, "col_a", "Error should report the duplicate column name");
        }
        _ => panic!("Expected DuplicateColumnName error when adding column with existing name"),
    }
    assert_eq!(df.width(), initial_width + 1, "Width should not change after failed add");


    // 3. 尝试添加长度不匹配的列 (应该失败)
    // 使用 new_from_options 因为输入是 Vec<Option<i32>>
    let series_wrong_len: Series<i32> = Series::new_from_options("col_e_short".into(), vec![Some(100), Some(200)]);
    match df.add_column(Box::new(series_wrong_len)) {
        Err(AxionError::MismatchedLengths { expected, found, name }) => {
            assert_eq!(expected, initial_height, "Error should report expected length (df height)");
            assert_eq!(found, 2, "Error should report found length (new series len)");
            assert_eq!(name, "col_e_short", "Error should report the name of the problematic series");
        }
        _ => panic!("Expected MismatchedLengths error when adding column with wrong length"),
    }
    assert_eq!(df.width(), initial_width + 1, "Width should not change after failed add due to length mismatch");

    Ok(())
}


#[test]
fn test_df_add_column_to_empty_dataframe() -> AxionResult<()> {
    let mut df = DataFrame::new(vec![])?; // 创建一个 0x0 的 DataFrame
    assert_eq!(df.shape(), (0, 0), "Initial empty DataFrame shape should be (0,0)");

    // 添加第一列，DataFrame 的高度应该由这一列决定
    // 使用 new_from_options 因为输入是 Vec<Option<i32>>
    let series1: Series<i32> = Series::new_from_options("first_col".into(), vec![Some(10), Some(20)]);
    df.add_column(Box::new(series1))?;
    assert_eq!(df.shape(), (2, 1), "Shape should be (2,1) after adding first column");
    assert_eq!(df.height(), 2, "Height should be 2");
    assert_eq!(df.width(), 1, "Width should be 1");
    assert_eq!(df.column("first_col")?.len(), 2);

    // 添加第二列，长度必须与现有高度匹配
    // 使用 new_from_options 因为输入是 Vec<Option<String>>
    let series2_ok: Series<String> = Series::new_from_options("second_col".into(), vec![Some("a".to_string()), Some("b".to_string())]);
    df.add_column(Box::new(series2_ok))?;
    assert_eq!(df.shape(), (2, 2), "Shape should be (2,2) after adding second column");

    // 尝试添加长度不匹配的列到已有数据的 DataFrame
    // 使用 new_from_options 因为输入是 Vec<Option<bool>>
    let series3_wrong_len: Series<bool> = Series::new_from_options("third_col_wrong".into(), vec![Some(true)]);
     match df.add_column(Box::new(series3_wrong_len)) {
        Err(AxionError::MismatchedLengths { expected, .. }) => {
            assert_eq!(expected, 2, "Expected length should be current df height");
        }
        _ => panic!("Expected MismatchedLengths error"),
    }
    Ok(())
}

#[test]
fn test_df_drop_column() -> AxionResult<()> {
    let mut df = create_sample_df_for_col_ops()?;
    let initial_width = df.width();

    // 1. 成功删除存在的列
    let dropped_col_c = df.drop_column("col_c")?;
    assert_eq!(df.width(), initial_width - 1, "Width should decrease by 1");
    assert!(df.column("col_c").is_err(), "Dropped column should not be accessible");
    assert_eq!(df.schema().get("col_c"), None, "Dropped column should be removed from schema");
    assert_eq!(dropped_col_c.name(), "col_c", "Returned series should have the correct name");
    assert_eq!(dropped_col_c.dtype(), DataType::Bool, "Returned series should have correct dtype");

    // 2. 尝试删除不存在的列
    match df.drop_column("non_existent_col") {
        Err(AxionError::ColumnNotFound(name)) => {
            assert_eq!(name, "non_existent_col", "Error should report the non-existent column name");
        }
        _ => panic!("Expected ColumnNotFound error when dropping non-existent column"),
    }
    assert_eq!(df.width(), initial_width - 1, "Width should not change after failed drop");

    // 3. 删除剩余所有列，检查 DataFrame 是否变为空
    let _ = df.drop_column("col_b")?;
    let _ = df.drop_column("col_a")?;
    assert_eq!(df.width(), 0, "Width should be 0 after dropping all columns");
    assert_eq!(df.height(), 0, "Height should be 0 after dropping all columns"); // 确认高度行为
    assert!(df.columns_names().is_empty(), "Column names should be empty");
    assert!(df.schema().is_empty(), "Schema should be empty");

    // 4. 尝试从已空的 DataFrame 删除列
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

    // 1. 成功重命名列
    let old_name = "col_b";
    let new_name = "col_b_renamed";
    df.rename_column(old_name, new_name)?;
    assert!(df.column(old_name).is_err(), "Old column name should not be accessible");
    let renamed_col = df.column(new_name)?; // 应该能找到新名称
    assert_eq!(renamed_col.name(), new_name, "Fetched column should have the new name");
    assert_eq!(renamed_col.dtype(), DataType::String, "Fetched column should retain its dtype");
    assert_eq!(df.schema().get(old_name), None, "Old name should be removed from schema");
    assert_eq!(df.schema().get(new_name), Some(&DataType::String), "New name should be in schema with correct type");
    // 检查内部 Series 的名称是否也已更新 (如果 SeriesTrait 允许访问 name 或 Series 有 name 方法)
    // 假设 df.column(new_name)?.name() 就能反映 Series 内部的名称

    // 2. 尝试将列重命名为已存在的其他列的名称 (应该失败)
    match df.rename_column("col_a", new_name) { // new_name ("col_b_renamed") 已经存在
        Err(AxionError::DuplicateColumnName(name)) => {
            assert_eq!(name, new_name, "Error should report the duplicate new name");
        }
        _ => panic!("Expected DuplicateColumnName error when renaming to an existing column name"),
    }

    // 3. 尝试重命名不存在的列 (应该失败)
    match df.rename_column("non_existent_col", "any_new_name") {
        Err(AxionError::ColumnNotFound(name)) => {
            assert_eq!(name, "non_existent_col", "Error should report the non-existent old name");
        }
        _ => panic!("Expected ColumnNotFound error when renaming a non-existent column"),
    }

    // 4. 尝试将列重命名为它当前的名称 (应该成功，无操作)
    df.rename_column("col_a", "col_a")?;
    assert!(df.column("col_a").is_ok(), "Column should still be accessible by its original name if renamed to itself");


    // 5. 确保重命名后，列的顺序保持不变
    let expected_names_after_rename = vec!["col_a", "col_b_renamed", "col_c"];
    assert_eq!(df.columns_names(), expected_names_after_rename, "Column order should be preserved after rename");


    Ok(())
}