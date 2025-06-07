use axion_data::{read_csv, AxionError, AxionResult, DataFrame, DataType, ReadCsvOptions, Series, WriteCsvOptions};
use tempfile::NamedTempFile;
use std::collections::HashMap;
use std::io::Write;

fn create_test_csv(content: &str) -> NamedTempFile {
    let mut file = NamedTempFile::new().unwrap();
    write!(file, "{}", content).unwrap();
    file
}

#[test]
fn test_read_simple_csv() -> AxionResult<()> {
    let content = "col_a,col_b,col_c\n\
                    1,x,true\n\
                    2,y,false\n\
                    3,z,true";
    let file = create_test_csv(content);
    let df = read_csv(file.path(), None)?;

    assert_eq!(df.width(), 3);
    assert_eq!(df.height(), 3);
    assert_eq!(df.columns_names(), vec!["col_a", "col_b", "col_c"]);

    let schema = df.schema();
    assert_eq!(schema.get("col_a"), Some(&DataType::Int64));
    assert_eq!(schema.get("col_b"), Some(&DataType::String)); 
    assert_eq!(schema.get("col_c"), Some(&DataType::Bool));   

    let col_a = df.column("col_a")?.as_any().downcast_ref::<Series<i64>>().unwrap();
    assert_eq!(col_a.get_opt(0), Some(Some(&1i64)));

    let col_b = df.column("col_b")?.as_any().downcast_ref::<Series<String>>().unwrap();
    assert_eq!(col_b.get_opt(1), Some(Some(&"y".to_string())));

    let col_c = df.column("col_c")?.as_any().downcast_ref::<Series<bool>>().unwrap();
    assert_eq!(col_c.get_opt(2), Some(Some(&true)));

    Ok(())
}

#[test]
fn test_read_csv_with_empty_fields() -> AxionResult<()> {
    let content = "name,value\n\
                    alpha,\n\
                    ,100\n\
                    gamma,200";
    let file = create_test_csv(content);
    let df = read_csv(file.path(), None)?;

    assert_eq!(df.width(), 2);
    assert_eq!(df.height(), 3);

    let schema = df.schema();
    assert_eq!(schema.get("name"), Some(&DataType::String));
    assert_eq!(schema.get("value"), Some(&DataType::Int64));

    let name_col = df.column("name")?.as_any().downcast_ref::<Series<String>>().unwrap();
    assert_eq!(name_col.get_opt(0), Some(Some(&"alpha".to_string())));
    assert_eq!(name_col.get_opt(1), Some(None));
    assert_eq!(name_col.get_opt(2), Some(Some(&"gamma".to_string())));

    let value_col = df.column("value")?.as_any().downcast_ref::<Series<i64>>().unwrap();
    assert_eq!(value_col.get_opt(0), Some(None));
    assert_eq!(value_col.get_opt(1), Some(Some(&100i64)));
    assert_eq!(value_col.get_opt(2), Some(Some(&200i64)));

    Ok(())
}

#[test]
fn test_read_csv_empty_file() -> AxionResult<()> { 
    let content = "";
    let file = create_test_csv(content);
    let result = read_csv(file.path(), None);
    assert!(result.is_ok());
    let df = result.unwrap();
    assert!(df.is_empty(), "DataFrame should be empty for an empty CSV file");
    assert_eq!(df.width(), 0);
    assert_eq!(df.height(), 0);
    Ok(())
}

#[test]
fn test_read_csv_no_records_with_header() -> AxionResult<()> {
    let content = "header1,header2";
    let file = create_test_csv(content);
    let df = read_csv(file.path(), None)?; 
    assert_eq!(df.width(), 2);
    assert_eq!(df.height(), 0);
    assert_eq!(df.columns_names(), vec!["header1", "header2"]);

    let schema = df.schema();
    assert_eq!(schema.get("header1"), Some(&DataType::String));
    assert_eq!(schema.get("header2"), Some(&DataType::String));
    Ok(())
}

#[test]
fn test_read_simple_csv_all_string() -> AxionResult<()> {
    let content = "col_a,col_b,col_c\n\
                    1,x,true\n\
                    2,y,false\n\
                    3,z,true";
    let file = create_test_csv(content);
    let options = ReadCsvOptions {
        infer_schema: false,
        ..Default::default()
    };
    let df = read_csv(file.path(), Some(options))?;

    assert_eq!(df.width(), 3);
    assert_eq!(df.height(), 3);
    assert_eq!(df.columns_names(), vec!["col_a", "col_b", "col_c"]);

    let schema = df.schema();
    assert_eq!(schema.get("col_a"), Some(&DataType::String)); 
    assert_eq!(schema.get("col_b"), Some(&DataType::String));
    assert_eq!(schema.get("col_c"), Some(&DataType::String));

    let col_a = df.column("col_a")?.as_any().downcast_ref::<Series<String>>().unwrap();
    assert_eq!(col_a.get_opt(0), Some(Some(&"1".to_string())));
    let col_b = df.column("col_b")?.as_any().downcast_ref::<Series<String>>().unwrap();
    assert_eq!(col_b.get_opt(1), Some(Some(&"y".to_string())));
    let col_c = df.column("col_c")?.as_any().downcast_ref::<Series<String>>().unwrap();
    assert_eq!(col_c.get_opt(2), Some(Some(&"true".to_string())));
    Ok(())
}

#[test]
fn test_read_csv_with_empty_fields_all_string() -> AxionResult<()> {
    let content = "name,value\n\
                    alpha,\n\
                    ,100\n\
                    gamma,200";
    let file = create_test_csv(content);
        let options = ReadCsvOptions {
        infer_schema: false,
        ..Default::default()
    };
    let df = read_csv(file.path(), Some(options))?;

    assert_eq!(df.width(), 2);
    assert_eq!(df.height(), 3);

    let schema = df.schema(); 
    assert_eq!(schema.get("name"), Some(&DataType::String));
    assert_eq!(schema.get("value"), Some(&DataType::String));


    let name_col = df.column("name")?.as_any().downcast_ref::<Series<String>>().unwrap();
    assert_eq!(name_col.get_opt(0), Some(Some(&"alpha".to_string())));
    assert_eq!(name_col.get_opt(1), Some(None));
    assert_eq!(name_col.get_opt(2), Some(Some(&"gamma".to_string())));

    let value_col = df.column("value")?.as_any().downcast_ref::<Series<String>>().unwrap();
    assert_eq!(value_col.get_opt(0), Some(None));
    assert_eq!(value_col.get_opt(1), Some(Some(&"100".to_string())));
    assert_eq!(value_col.get_opt(2), Some(Some(&"200".to_string())));
    Ok(())
}


#[test]
fn test_read_csv_infer_types_simple() -> AxionResult<()> {
    let content = "id,value,is_active,name\n\
                    1,10.5,true,Alice\n\
                    2,20.0,false,Bob\n\
                    ,15.5,t,Charlie\n\
                    4,,f,David";
    let file = create_test_csv(content);
    let options = ReadCsvOptions {
        infer_schema: true,
        ..Default::default()
    };
    let df = read_csv(file.path(), Some(options))?;

    assert_eq!(df.width(), 4);
    assert_eq!(df.height(), 4);
    assert_eq!(df.columns_names(), vec!["id", "value", "is_active", "name"]);

    let schema = df.schema();
    assert_eq!(schema.get("id"), Some(&DataType::Int64));
    assert_eq!(schema.get("value"), Some(&DataType::Float64));
    assert_eq!(schema.get("is_active"), Some(&DataType::Bool)); 
    assert_eq!(schema.get("name"), Some(&DataType::String)); 

    let id_col = df.column("id")?.as_any().downcast_ref::<Series<i64>>().unwrap();
    assert_eq!(id_col.get_opt(0), Some(Some(&1i64)));
    assert_eq!(id_col.get_opt(1), Some(Some(&2i64)));
    assert_eq!(id_col.get_opt(2), Some(None));
    assert_eq!(id_col.get_opt(3), Some(Some(&4i64)));

    let val_col = df.column("value")?.as_any().downcast_ref::<Series<f64>>().unwrap();
    assert_eq!(val_col.get_opt(0), Some(Some(&10.5f64)));
    assert_eq!(val_col.get_opt(1), Some(Some(&20.0f64)));
    assert_eq!(val_col.get_opt(2), Some(Some(&15.5f64)));
    assert_eq!(val_col.get_opt(3), Some(None));

    let active_col = df.column("is_active")?.as_any().downcast_ref::<Series<bool>>().unwrap(); // Series<bool>
    assert_eq!(active_col.get_opt(0), Some(Some(&true)));
    assert_eq!(active_col.get_opt(1), Some(Some(&false)));
    assert_eq!(active_col.get_opt(2), Some(Some(&true)));
    assert_eq!(active_col.get_opt(3), Some(Some(&false)));

    let name_col = df.column("name")?.as_any().downcast_ref::<Series<String>>().unwrap();
    assert_eq!(name_col.get_opt(0), Some(Some(&"Alice".to_string())));

    Ok(())
}

#[test]
fn test_read_csv_manual_dtypes() -> AxionResult<()> {
    let content = "col_a,col_b\n\
                    1,10.5\n\
                    2,20.0";
    let file = create_test_csv(content);
    let mut manual_dtypes_map = HashMap::new(); // Renamed to avoid conflict if you still have a local `manual_dtypes` variable
    manual_dtypes_map.insert("col_a".to_string(), DataType::String);
    manual_dtypes_map.insert("col_b".to_string(), DataType::Int64);

    let options = ReadCsvOptions {
        infer_schema: true,
        dtypes: Some(manual_dtypes_map),
        ..Default::default()
    };
    let df = read_csv(file.path(), Some(options))?;

    let schema = df.schema(); 
    assert_eq!(schema.get("col_a"), Some(&DataType::String));
    assert_eq!(schema.get("col_b"), Some(&DataType::Int64));

    let col_a = df.column("col_a")?.as_any().downcast_ref::<Series<String>>().unwrap();
    assert_eq!(col_a.get_opt(0), Some(Some(&"1".to_string())));

    let col_b = df.column("col_b")?.as_any().downcast_ref::<Series<i64>>().unwrap();
    assert_eq!(col_b.get_opt(0), Some(None));
    assert_eq!(col_b.get_opt(1), Some(None));

    Ok(())
}

#[test]
fn test_read_csv_no_header_infer_types() -> AxionResult<()> {
    let content = "1,Alice,true\n\
                    2,Bob,false";
    let file = create_test_csv(content);
    let options = ReadCsvOptions {
        has_header: false,
        infer_schema: true,
        ..Default::default()
    };
    let df = read_csv(file.path(), Some(options))?;

    assert_eq!(df.columns_names(), vec!["column_0", "column_1", "column_2"]);
    let schema = df.schema(); 
    assert_eq!(schema.get("column_0"), Some(&DataType::Int64));
    assert_eq!(schema.get("column_1"), Some(&DataType::String));
    assert_eq!(schema.get("column_2"), Some(&DataType::Bool));

    let col0 = df.column("column_0")?.as_any().downcast_ref::<Series<i64>>().unwrap();
    assert_eq!(col0.get_opt(0), Some(Some(&1i64)));
    Ok(())
}

    #[test]
fn test_read_csv_mismatched_columns() {
    let content = "h1,h2\n\
                    a,b\n\
                    c,d,e";
    let file = create_test_csv(content);
    let result = read_csv(file.path(), None);
    assert!(matches!(result, Err(AxionError::CsvError(_))), "Expected CsvError for mismatched columns, got {:?}", result);
}

#[test]
fn test_read_csv_skip_rows_and_comments() -> AxionResult<()> {
    let content = "# This is a comment line to be skipped\n\
                    # Another comment\n\
                    col_x,col_y\n\
                    # This comment is after the header but before data\n\
                    10,val1\n\
                    20,val2\n"; // <--- 移除了 "# Trailing comment"
    let file = create_test_csv(content);

    let options = ReadCsvOptions::builder()
        .skip_rows(2) // Skip the first two comment lines
        .comment_char(Some(b'#'))
        .with_header(true) // The line after skipped rows is the header
        .build();

    let df = read_csv(file.path(), Some(options))?;

    assert_eq!(df.width(), 2);
    assert_eq!(df.height(), 2);
    assert_eq!(df.columns_names(), vec!["col_x", "col_y"]);

    let schema = df.schema();
    assert_eq!(schema.get("col_x"), Some(&DataType::Int64));
    assert_eq!(schema.get("col_y"), Some(&DataType::String));

    let col_x = df.column("col_x")?.as_any().downcast_ref::<Series<i64>>().unwrap();
    assert_eq!(col_x.get_opt(0), Some(Some(&10i64)));
    assert_eq!(col_x.get_opt(1), Some(Some(&20i64)));

    let col_y = df.column("col_y")?.as_any().downcast_ref::<Series<String>>().unwrap();
    assert_eq!(col_y.get_opt(0), Some(Some(&"val1".to_string())));
    assert_eq!(col_y.get_opt(1), Some(Some(&"val2".to_string())));

    Ok(())
}

#[test]
fn test_read_csv_use_columns_basic() -> AxionResult<()> {
    let content = "id,name,value,active\n\
                    1,Alice,100,true\n\
                    2,Bob,200,false";
    let file = create_test_csv(content);
    let options = ReadCsvOptions::builder()
        .use_columns(vec!["name".to_string(), "active".to_string()])
        .build();
    let df = read_csv(file.path(), Some(options))?;

    assert_eq!(df.width(), 2);
    assert_eq!(df.height(), 2);
    assert_eq!(df.columns_names(), vec!["name", "active"]);

    let schema = df.schema();
    assert_eq!(schema.get("name"), Some(&DataType::String));
    assert_eq!(schema.get("active"), Some(&DataType::Bool));

    let name_col = df.column("name")?.as_any().downcast_ref::<Series<String>>().unwrap();
    assert_eq!(name_col.get_opt(0), Some(Some(&"Alice".to_string())));

    let active_col = df.column("active")?.as_any().downcast_ref::<Series<bool>>().unwrap();
    assert_eq!(active_col.get_opt(0), Some(Some(&true)));

    Ok(())
}

#[test]
fn test_read_csv_use_columns_non_existent() {
    let content = "id,name\n1,Alice";
    let file = create_test_csv(content);
    let options = ReadCsvOptions::builder()
        .use_columns(vec!["name".to_string(), "non_existent_col".to_string()])
        .build();
    let result = read_csv(file.path(), Some(options));
    assert!(matches!(result, Err(AxionError::CsvError(_))));
    if let Err(AxionError::CsvError(msg)) = result {
        assert!(msg.contains("non_existent_col"));
    }
}

#[test]
fn test_read_csv_na_values_custom() -> AxionResult<()> {
    let content = "col_int,col_str,col_float\n\
                    10,hello,1.1\n\
                    N/A,world,2.2\n\
                    30,MISSING,N/A\n\
                    ,present,3.3"; // Empty string is also a NA candidate if not overridden
    let file = create_test_csv(content);
    let mut na_set = std::collections::HashSet::new();
    na_set.insert("N/A".to_string());
    na_set.insert("MISSING".to_string());
    // We are not adding "" to na_set, so empty strings should be read as empty strings if infer_schema=false
    // or potentially None if infer_schema=true and the column becomes numeric.

    let options = ReadCsvOptions::builder()
        .na_values(Some(na_set))
        .build();
    let df = read_csv(file.path(), Some(options))?;

    assert_eq!(df.width(), 3);
    assert_eq!(df.height(), 4);

    let schema = df.schema();
    assert_eq!(schema.get("col_int"), Some(&DataType::Int64));
    assert_eq!(schema.get("col_str"), Some(&DataType::String));
    assert_eq!(schema.get("col_float"), Some(&DataType::Float64));

    let col_int = df.column("col_int")?.as_any().downcast_ref::<Series<i64>>().unwrap();
    assert_eq!(col_int.get_opt(0), Some(Some(&10i64)));
    assert_eq!(col_int.get_opt(1), Some(None)); // N/A
    assert_eq!(col_int.get_opt(2), Some(Some(&30i64)));
    assert_eq!(col_int.get_opt(3), Some(None)); // Empty string parsed as None for Int64

    let col_str = df.column("col_str")?.as_any().downcast_ref::<Series<String>>().unwrap();
    assert_eq!(col_str.get_opt(0), Some(Some(&"hello".to_string())));
    assert_eq!(col_str.get_opt(1), Some(Some(&"world".to_string())));
    assert_eq!(col_str.get_opt(2), Some(None)); // MISSING
    assert_eq!(col_str.get_opt(3), Some(Some(&"present".to_string()))); // Empty string is not in na_values, so it's "present"

    let col_float = df.column("col_float")?.as_any().downcast_ref::<Series<f64>>().unwrap();
    assert_eq!(col_float.get_opt(0), Some(Some(&1.1f64)));
    assert_eq!(col_float.get_opt(1), Some(Some(&2.2f64)));
    assert_eq!(col_float.get_opt(2), Some(None)); // N/A
    assert_eq!(col_float.get_opt(3), Some(Some(&3.3f64)));

    Ok(())
}

#[test]
fn test_write_simple_dataframe_to_csv_writer() -> AxionResult<()> {
    // 1. 创建 Series
    let s1 = Series::new_from_options("col_a".into(), vec![Some(1i64), Some(2), Some(3)]);
    let s2 = Series::new_from_options("col_b".into(), vec![Some("x".to_string()), Some("y".to_string()), None]);

    // 2. 创建 DataFrame
    let df = DataFrame::new(vec![
        Box::new(s1),
        Box::new(s2),
    ])?;

    // 3. 调用 to_csv_writer
    let mut buffer: Vec<u8> = Vec::new();
    df.to_csv_writer(&mut buffer, None)?; // 使用 to_csv_writer
    let csv_string = String::from_utf8(buffer)?; // 现在 ? 应该可以工作了 (假设 From impl 已添加)

    // 4. 定义期望的输出
    let expected_csv = "col_a,col_b\n\
                        1,x\n\
                        2,y\n\
                        3,\n"; 

    assert_eq!(csv_string, expected_csv);

    Ok(())
}

#[test]
fn test_write_dataframe_with_custom_options_to_csv_writer() -> AxionResult<()> {
    let s1 = Series::new_from_options("id".into(), vec![Some(10i64)]);
    let s2 = Series::new_from_options("value".into(), vec![Some(f64::NAN)]);
    let s3 = Series::new_from_options("name".into(), vec![None::<String>]);

    let df = DataFrame::new(vec![
        Box::new(s1),
        Box::new(s2),
        Box::new(s3),
    ])?;

    let options = WriteCsvOptions {
        has_header: true,
        delimiter: b';',
        na_rep: "NULL".to_string(),
        ..Default::default()
    };

    let mut buffer: Vec<u8> = Vec::new();
    df.to_csv_writer(&mut buffer, Some(options))?; // 使用 to_csv_writer
    let csv_string = String::from_utf8(buffer)?;
    
    let expected_csv = "id;value;name\n\
                        10;NaN;NULL\n"; 

    assert_eq!(csv_string, expected_csv);

    Ok(())
}

#[test]
fn test_write_dataframe_no_header_to_csv_writer() -> AxionResult<()> {
    let s1 = Series::new_from_options("col_a".into(), vec![Some(1i64), Some(2)]);
    let df = DataFrame::new(vec![Box::new(s1)])?;

    let options = WriteCsvOptions {
        has_header: false,
        ..Default::default()
    };
    let mut buffer: Vec<u8> = Vec::new();
    df.to_csv_writer(&mut buffer, Some(options))?; // 使用 to_csv_writer
    let csv_string = String::from_utf8(buffer)?;

    let expected_csv = "1\n\
                        2\n";
    assert_eq!(csv_string, expected_csv);
    Ok(())
}

#[test]
fn test_write_empty_dataframe_to_csv_writer() -> AxionResult<()> {
    let df_empty_no_cols = DataFrame::new_empty(); 
    
    let mut buffer: Vec<u8> = Vec::new();
    df_empty_no_cols.to_csv_writer(&mut buffer, Some(WriteCsvOptions { has_header: true, ..Default::default() }))?;
    let csv_string_with_header = String::from_utf8(buffer)?;
    assert_eq!(csv_string_with_header, ""); 

    buffer = Vec::new(); 
    df_empty_no_cols.to_csv_writer(&mut buffer, Some(WriteCsvOptions { has_header: false, ..Default::default() }))?;
    let csv_string_no_header = String::from_utf8(buffer)?;
    assert_eq!(csv_string_no_header, "");

    // 测试一个有列但没有行的 DataFrame
    // 假设 Series::new_empty 需要一个 DataType
    let s1_empty: Series<i64> = Series::new_empty("col1".into(), DataType::Int64); 
    let df_cols_no_rows = DataFrame::new(vec![Box::new(s1_empty)])?;
    
    buffer = Vec::new();
    df_cols_no_rows.to_csv_writer(&mut buffer, Some(WriteCsvOptions { has_header: true, ..Default::default() }))?;
    let csv_cols_no_rows_with_header = String::from_utf8(buffer)?;
    assert_eq!(csv_cols_no_rows_with_header, "col1\n");

    buffer = Vec::new();
    df_cols_no_rows.to_csv_writer(&mut buffer, Some(WriteCsvOptions { has_header: false, ..Default::default() }))?;
    let csv_cols_no_rows_no_header = String::from_utf8(buffer)?;
    assert_eq!(csv_cols_no_rows_no_header, "");

    Ok(())
}

// 还可以添加一个测试来实际使用 to_csv 写入到临时文件
#[test]
fn test_write_dataframe_to_actual_file() -> AxionResult<()> {
    let s1 = Series::new_from_options("col_a".into(), vec![Some(1i64), Some(2)]);
    let s2 = Series::new_from_options("col_b".into(), vec![Some("text1".to_string()), Some("text2".to_string())]);
    let df = DataFrame::new(vec![Box::new(s1), Box::new(s2)])?;

    let temp_file = NamedTempFile::new().map_err(|e| AxionError::IoError(e.to_string()))?;
    let file_path = temp_file.path().to_path_buf();

    df.to_csv(&file_path, None)?; // 使用 to_csv 写入到路径

    let content = std::fs::read_to_string(&file_path)
        .map_err(|e| AxionError::IoError(e.to_string()))?;
    
    let expected_csv = "col_a,col_b\n\
                        1,text1\n\
                        2,text2\n";
    assert_eq!(content, expected_csv);

    Ok(())
}