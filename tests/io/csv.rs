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

    assert_eq!(df.shape(), (3, 3));
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

    assert_eq!(df.shape(), (3, 2));

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
fn test_read_csv_edge_cases() -> AxionResult<()> {
    // 空文件
    let content = "";
    let file = create_test_csv(content);
    let result = read_csv(file.path(), None);
    assert!(result.is_ok());
    let df = result.unwrap();
    assert!(df.is_empty());
    assert_eq!(df.shape(), (0, 0));

    // 只有表头无数据
    let content = "header1,header2";
    let file = create_test_csv(content);
    let df = read_csv(file.path(), None)?; 
    assert_eq!(df.shape(), (0, 2));
    assert_eq!(df.columns_names(), vec!["header1", "header2"]);

    // 列数不匹配应该报错
    let content = "h1,h2\n\
                    a,b\n\
                    c,d,e";
    let file = create_test_csv(content);
    let result = read_csv(file.path(), None);
    assert!(matches!(result, Err(AxionError::CsvError(_))));

    Ok(())
}

#[test]
fn test_read_csv_schema_options() -> AxionResult<()> {
    let content = "col_a,col_b,col_c\n\
                    1,x,true\n\
                    2,y,false\n\
                    3,z,true";
    let file = create_test_csv(content);

    // 不推断类型，全部按字符串处理
    let options = ReadCsvOptions {
        infer_schema: false,
        ..Default::default()
    };
    let df = read_csv(file.path(), Some(options))?;

    let schema = df.schema();
    assert_eq!(schema.get("col_a"), Some(&DataType::String)); 
    assert_eq!(schema.get("col_b"), Some(&DataType::String));
    assert_eq!(schema.get("col_c"), Some(&DataType::String));

    let col_a = df.column("col_a")?.as_any().downcast_ref::<Series<String>>().unwrap();
    assert_eq!(col_a.get_opt(0), Some(Some(&"1".to_string())));
    let col_c = df.column("col_c")?.as_any().downcast_ref::<Series<String>>().unwrap();
    assert_eq!(col_c.get_opt(2), Some(Some(&"true".to_string())));

    Ok(())
}

#[test]
fn test_read_csv_type_inference() -> AxionResult<()> {
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

    assert_eq!(df.shape(), (4, 4));
    assert_eq!(df.columns_names(), vec!["id", "value", "is_active", "name"]);

    let schema = df.schema();
    assert_eq!(schema.get("id"), Some(&DataType::Int64));
    assert_eq!(schema.get("value"), Some(&DataType::Float64));
    assert_eq!(schema.get("is_active"), Some(&DataType::Bool)); 
    assert_eq!(schema.get("name"), Some(&DataType::String)); 

    let id_col = df.column("id")?.as_any().downcast_ref::<Series<i64>>().unwrap();
    assert_eq!(id_col.get_opt(0), Some(Some(&1i64)));
    assert_eq!(id_col.get_opt(2), Some(None));

    let val_col = df.column("value")?.as_any().downcast_ref::<Series<f64>>().unwrap();
    assert_eq!(val_col.get_opt(0), Some(Some(&10.5f64)));
    assert_eq!(val_col.get_opt(3), Some(None));

    let active_col = df.column("is_active")?.as_any().downcast_ref::<Series<bool>>().unwrap();
    assert_eq!(active_col.get_opt(0), Some(Some(&true)));
    assert_eq!(active_col.get_opt(2), Some(Some(&true)));

    Ok(())
}

#[test]
fn test_read_csv_manual_dtypes() -> AxionResult<()> {
    let content = "col_a,col_b\n\
                    1,10.5\n\
                    2,20.0";
    let file = create_test_csv(content);
    let mut manual_dtypes = HashMap::new();
    manual_dtypes.insert("col_a".to_string(), DataType::String);
    manual_dtypes.insert("col_b".to_string(), DataType::Int64);

    let options = ReadCsvOptions {
        infer_schema: true,
        dtypes: Some(manual_dtypes),
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
fn test_read_csv_no_header() -> AxionResult<()> {
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
fn test_read_csv_advanced_options() -> AxionResult<()> {
    // 测试跳过行和注释处理
    let content = "# This is a comment line to be skipped\n\
                    # Another comment\n\
                    col_x,col_y\n\
                    # This comment is after the header but before data\n\
                    10,val1\n\
                    20,val2\n";
    let file = create_test_csv(content);

    let options = ReadCsvOptions::builder()
        .skip_rows(2)
        .comment_char(Some(b'#'))
        .with_header(true)
        .build();

    let df = read_csv(file.path(), Some(options))?;

    assert_eq!(df.shape(), (2, 2));
    assert_eq!(df.columns_names(), vec!["col_x", "col_y"]);

    let col_x = df.column("col_x")?.as_any().downcast_ref::<Series<i64>>().unwrap();
    assert_eq!(col_x.get_opt(0), Some(Some(&10i64)));
    assert_eq!(col_x.get_opt(1), Some(Some(&20i64)));

    Ok(())
}

#[test]
fn test_read_csv_column_selection() -> AxionResult<()> {
    let content = "id,name,value,active\n\
                    1,Alice,100,true\n\
                    2,Bob,200,false";
    let file = create_test_csv(content);

    // 选择特定列
    let options = ReadCsvOptions::builder()
        .use_columns(vec!["name".to_string(), "active".to_string()])
        .build();
    let df = read_csv(file.path(), Some(options))?;

    assert_eq!(df.shape(), (2, 2));
    assert_eq!(df.columns_names(), vec!["name", "active"]);

    let name_col = df.column("name")?.as_any().downcast_ref::<Series<String>>().unwrap();
    assert_eq!(name_col.get_opt(0), Some(Some(&"Alice".to_string())));

    // 选择不存在的列应该报错
    let content = "id,name\n1,Alice";
    let file = create_test_csv(content);
    let options = ReadCsvOptions::builder()
        .use_columns(vec!["name".to_string(), "non_existent".to_string()])
        .build();
    let result = read_csv(file.path(), Some(options));
    assert!(matches!(result, Err(AxionError::CsvError(_))));

    Ok(())
}

#[test]
fn test_read_csv_na_values() -> AxionResult<()> {
    let content = "col_int,col_str,col_float\n\
                    10,hello,1.1\n\
                    N/A,world,2.2\n\
                    30,MISSING,N/A\n\
                    ,present,3.3";
    let file = create_test_csv(content);
    let mut na_set = std::collections::HashSet::new();
    na_set.insert("N/A".to_string());
    na_set.insert("MISSING".to_string());

    let options = ReadCsvOptions::builder()
        .na_values(Some(na_set))
        .build();
    let df = read_csv(file.path(), Some(options))?;

    assert_eq!(df.shape(), (4, 3));

    let col_int = df.column("col_int")?.as_any().downcast_ref::<Series<i64>>().unwrap();
    assert_eq!(col_int.get_opt(0), Some(Some(&10i64)));
    assert_eq!(col_int.get_opt(1), Some(None)); // N/A
    assert_eq!(col_int.get_opt(2), Some(Some(&30i64)));

    let col_str = df.column("col_str")?.as_any().downcast_ref::<Series<String>>().unwrap();
    assert_eq!(col_str.get_opt(0), Some(Some(&"hello".to_string())));
    assert_eq!(col_str.get_opt(2), Some(None)); // MISSING
    assert_eq!(col_str.get_opt(3), Some(Some(&"present".to_string())));

    let col_float = df.column("col_float")?.as_any().downcast_ref::<Series<f64>>().unwrap();
    assert_eq!(col_float.get_opt(0), Some(Some(&1.1f64)));
    assert_eq!(col_float.get_opt(2), Some(None)); // N/A

    Ok(())
}

#[test]
fn test_write_csv_basic() -> AxionResult<()> {
    let s1 = Series::new_from_options("col_a".into(), vec![Some(1i64), Some(2), Some(3)]);
    let s2 = Series::new_from_options("col_b".into(), vec![Some("x".to_string()), Some("y".to_string()), None]);

    let df = DataFrame::new(vec![Box::new(s1), Box::new(s2)])?;

    let mut buffer: Vec<u8> = Vec::new();
    df.to_csv_writer(&mut buffer, None)?;
    let csv_string = String::from_utf8(buffer)?;

    let expected_csv = "col_a,col_b\n\
                        1,x\n\
                        2,y\n\
                        3,\n"; 

    assert_eq!(csv_string, expected_csv);

    Ok(())
}

#[test]
fn test_write_csv_with_options() -> AxionResult<()> {
    let s1 = Series::new_from_options("id".into(), vec![Some(10i64)]);
    let s2 = Series::new_from_options("value".into(), vec![Some(f64::NAN)]);
    let s3 = Series::new_from_options("name".into(), vec![None::<String>]);

    let df = DataFrame::new(vec![Box::new(s1), Box::new(s2), Box::new(s3)])?;

    // 自定义选项
    let options = WriteCsvOptions {
        has_header: true,
        delimiter: b';',
        na_rep: "NULL".to_string(),
        ..Default::default()
    };

    let mut buffer: Vec<u8> = Vec::new();
    df.to_csv_writer(&mut buffer, Some(options))?;
    let csv_string = String::from_utf8(buffer)?;
    
    let expected_csv = "id;value;name\n\
                        10;NaN;NULL\n"; 

    assert_eq!(csv_string, expected_csv);

    // 测试无表头
    let s1 = Series::new_from_options("col_a".into(), vec![Some(1i64), Some(2)]);
    let df = DataFrame::new(vec![Box::new(s1)])?;

    let options = WriteCsvOptions {
        has_header: false,
        ..Default::default()
    };
    let mut buffer: Vec<u8> = Vec::new();
    df.to_csv_writer(&mut buffer, Some(options))?;
    let csv_string = String::from_utf8(buffer)?;

    let expected_csv = "1\n2\n";
    assert_eq!(csv_string, expected_csv);

    Ok(())
}

#[test]
fn test_write_csv_empty_dataframe() -> AxionResult<()> {
    // 完全空的DataFrame
    let df_empty = DataFrame::new_empty(); 
    
    let mut buffer: Vec<u8> = Vec::new();
    df_empty.to_csv_writer(&mut buffer, Some(WriteCsvOptions { has_header: true, ..Default::default() }))?;
    let csv_string = String::from_utf8(buffer)?;
    assert_eq!(csv_string, ""); 

    // 有列但无行的DataFrame
    let s1_empty: Series<i64> = Series::new_empty("col1".into(), DataType::Int64); 
    let df_cols_no_rows = DataFrame::new(vec![Box::new(s1_empty)])?;
    
    let mut buffer: Vec<u8> = Vec::new();
    df_cols_no_rows.to_csv_writer(&mut buffer, Some(WriteCsvOptions { has_header: true, ..Default::default() }))?;
    let csv_string = String::from_utf8(buffer)?;
    assert_eq!(csv_string, "col1\n");

    Ok(())
}

#[test]
fn test_csv_roundtrip() -> AxionResult<()> {
    let s1 = Series::new_from_options("col_a".into(), vec![Some(1i64), Some(2)]);
    let s2 = Series::new_from_options("col_b".into(), vec![Some("text1".to_string()), Some("text2".to_string())]);
    let df_original = DataFrame::new(vec![Box::new(s1), Box::new(s2)])?;

    let temp_file = NamedTempFile::new().map_err(|e| AxionError::IoError(e.to_string()))?;
    let file_path = temp_file.path().to_path_buf();

    // 写入文件
    df_original.to_csv(&file_path, None)?;

    // 读取文件
    let df_read = read_csv(&file_path, None)?;

    // 验证内容
    assert_eq!(df_read.shape(), df_original.shape());
    assert_eq!(df_read.columns_names(), df_original.columns_names());

    let col_a_read = df_read.column("col_a")?.as_any().downcast_ref::<Series<i64>>().unwrap();
    assert_eq!(col_a_read.get_opt(0), Some(Some(&1i64)));
    assert_eq!(col_a_read.get_opt(1), Some(Some(&2i64)));

    let col_b_read = df_read.column("col_b")?.as_any().downcast_ref::<Series<String>>().unwrap();
    assert_eq!(col_b_read.get_opt(0), Some(Some(&"text1".to_string())));
    assert_eq!(col_b_read.get_opt(1), Some(Some(&"text2".to_string())));

    Ok(())
}