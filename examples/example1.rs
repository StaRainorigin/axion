#![allow(unused_variables)]

use axion_data::*;

// ===== 1-2. 创建Series和DataFrame =====
#[test]
fn test_create_series() {
    // 常规创建
    let s1 = Series::new("numbers".to_string(), vec![1, 2, 3, 4, 5]);
    println!("Series s1: {:?}", s1);
    assert_eq!(s1.len(), 5);
    
    // 从 Option 类型创建
    let s2 = Series::new_from_options("with_nulls".to_string(), vec![
        Some(10), None, Some(20), Some(30)
    ]);
    println!("Series with nulls: {:?}", s2);
    assert_eq!(s2.len(), 4);
}

#[test]
fn test_create_dataframe() -> AxionResult<()> {
    let df = DataFrame::new(vec![
        Box::new(Series::new("Name".to_string(), vec!["Alice".to_string(), "Bob".to_string(), "Charlie".to_string()])),
        Box::new(Series::new("Age".to_string(), vec![25, 30, 28])),
        Box::new(Series::new("City".to_string(), vec!["New York".to_string(), "London".to_string(), "Paris".to_string()])),
    ])?;
    
    println!("DataFrame created: {} rows × {} columns", df.height(), df.width());
    println!("Column names: {:?}", df.columns_names());
    println!("Schema: {:?}", df.schema());
    assert_eq!(df.height(), 3);
    assert_eq!(df.width(), 3);
    Ok(())
}

// ===== 3. 读取数据 =====
#[test]
fn test_read_csv() -> AxionResult<()> {
    // 使用已实现的CSV读取功能
    let path = "data/train.csv";
    
    if std::path::Path::new(path).exists() {
        let df = read_csv(path, None)?;
        println!("Successfully read CSV: {} rows × {} columns", df.height(), df.width());
        println!("Columns: {:?}", df.columns_names());
        println!("Data types: {:?}", df.dtypes());
        println!("First 3 rows:\n{}", df.head(3));
    } else {
        println!("CSV file not found at: {}", path);
        // 创建测试CSV文件
        use std::io::Write;
        let content = "name,age,salary\nAlice,25,50000\nBob,30,60000\nCharlie,35,70000\n";
        std::fs::create_dir_all("data").ok();
        let mut file = std::fs::File::create("data/test.csv")?;
        file.write_all(content.as_bytes())?;
        
        let df = read_csv("data/test.csv", None)?;
        println!("Test CSV read successfully: {} rows × {} columns", df.height(), df.width());
    }
    Ok(())
}

// ===== 4. 查看数据 =====
#[test]
fn test_head_tail() -> AxionResult<()> {

    // let df = DataFrame::new(vec![
    //     Box::new(Series::new("Name".to_string(), vec!["Alice".to_string(), "Bob".to_string(), "Charlie".to_string(), "David".to_string(), "Eve".to_string()])),
    //     Box::new(Series::new("Age".to_string(), vec![25, 30, 28, 35, 22])),
    // ])?;

    let df = df!(
        "Name" => &["Alice", "Bob", "Charlie", "David", "Eve"],
        "Age" => &[25, 30, 28, 35, 22],
        "Salary" => &[50000.0, 60000.0, 55000.0, 70000.0, 45000.0]
    )?; // 使用宏创建DataFrame
    
    // 使用head和tail方法
    let head_df = df.head(3);
    let tail_df = df.tail(2);
    
    println!("DataFrame: {} rows", df.height());
    println!("Head (3 rows):\n{}", head_df);
    println!("Tail (2 rows):\n{}", tail_df);
    
    assert_eq!(head_df.height(), 3);
    assert_eq!(tail_df.height(), 2);
    Ok(())
}

#[test]
fn test_describe() -> AxionResult<()> {
    let df = DataFrame::new(vec![
        Box::new(Series::new("Age".to_string(), vec![25, 30, 28, 35, 22])),
        Box::new(Series::new("Salary".to_string(), vec![50000.0, 60000.0, 55000.0, 70000.0, 45000.0])),
    ])?;
    
    let age_col: &Series<i32> = df.downcast_column("Age")?;
    let salary_col: &Series<f64> = df.downcast_column("Salary")?;
    
    println!("Age statistics:");
    println!("  Count: {}", age_col.len());
    println!("  Mean: {:.2}", age_col.mean().unwrap_or(0.0));
    println!("  Min: {}", age_col.min().unwrap_or(0));
    println!("  Max: {}", age_col.max().unwrap_or(0));
    println!("  Sum: {}", age_col.sum().unwrap_or(0));
    
    println!("\nSalary statistics:");
    println!("  Mean: {:.2}", salary_col.mean().unwrap_or(0.0));
    println!("  Min: {:.2}", salary_col.min().unwrap_or(0.0));
    println!("  Max: {:.2}", salary_col.max().unwrap_or(0.0));
    println!("  Sum: {:.2}", salary_col.sum().unwrap_or(0.0));
    
    Ok(())
}

// ===== 5. 选择数据 =====
#[test]
fn test_select_operations() -> AxionResult<()> {
    let df = DataFrame::new(vec![
        Box::new(Series::new("Name".to_string(), vec!["Alice".to_string(), "Bob".to_string(), "Charlie".to_string()])),
        Box::new(Series::new("Age".to_string(), vec![25, 30, 28])),
        Box::new(Series::new("Salary".to_string(), vec![50000.0, 60000.0, 55000.0])),
    ])?;
    
    // 使用已实现的select方法
    let selected_df = df.select(&["Name", "Age"])?;
    println!("Selected columns: {} rows × {} columns", selected_df.height(), selected_df.width());
    println!("Selected DataFrame:\n{}", selected_df);
    
    // 使用已实现的drop方法
    let dropped_df = df.drop("Salary")?;
    println!("After dropping Salary: {} columns", dropped_df.width());
    println!("Dropped DataFrame:\n{}", dropped_df);
    
    assert_eq!(selected_df.width(), 2);
    assert_eq!(dropped_df.width(), 2);
    Ok(())
}

#[test]
fn test_column_access() -> AxionResult<()> {
    let df = DataFrame::new(vec![
        Box::new(Series::new("Name".to_string(), vec!["Alice".to_string(), "Bob".to_string()])),
        Box::new(Series::new("Age".to_string(), vec![25, 30])),
    ])?;
    
    // 使用已实现的column和downcast_column方法
    let name_col = df.column("Name")?;
    println!("Name column type: {:?}", name_col.dtype());
    
    let age_col: &Series<i32> = df.downcast_column("Age")?;
    println!("Age column values: {:?}", age_col.data);
    println!("First age value: {:?}", age_col.get(0));
    
    assert_eq!(age_col.len(), 2);
    Ok(())
}

// ===== 6. 过滤数据 =====
#[test]
fn test_filtering() -> AxionResult<()> {
    let df = DataFrame::new(vec![
        Box::new(Series::new("Name".to_string(), vec!["Alice".to_string(), "Bob".to_string(), "Charlie".to_string()])),
        Box::new(Series::new("Age".to_string(), vec![25, 30, 28])),
        Box::new(Series::new("Salary".to_string(), vec![50000.0, 60000.0, 55000.0])),
    ])?;
    
    // 创建过滤条件
    let age_col: &Series<i32> = df.downcast_column("Age")?;
    let age_mask = age_col.gt(26)?; // 年龄大于26
    println!("Age > 26 mask: {:?}", age_mask.data);
    
    // 使用已实现的filter方法
    let filtered_df = df.filter(&age_mask)?;
    println!("Filtered DataFrame (Age > 26):\n{}", filtered_df);
    println!("Filtered rows: {}", filtered_df.height());
    
    // 测试并行过滤
    let par_filtered_df = df.par_filter(&age_mask)?;
    println!("Parallel filtered DataFrame:\n{}", par_filtered_df);
    
    assert_eq!(filtered_df.height(), par_filtered_df.height());
    Ok(())
}

#[test]
fn test_series_comparisons() -> AxionResult<()> {
    let series = Series::new("values".to_string(), vec![1, 5, 3, 8, 2]);
    
    // 测试各种比较操作
    let gt_mask = series.gt(3)?;
    let lt_mask = series.lt(5)?;
    let eq_mask = series.eq(5)?;
    let ge_mask = series.gte(3)?;
    let le_mask = series.lte(3)?;
    
    println!("Original: {:?}", series.data);
    println!("Values > 3: {:?}", gt_mask.data);
    println!("Values < 5: {:?}", lt_mask.data);
    println!("Values == 5: {:?}", eq_mask.data);
    println!("Values >= 3: {:?}", ge_mask.data);
    println!("Values <= 3: {:?}", le_mask.data);
    
    Ok(())
}

// ===== 7. 空值处理 =====
#[test]
fn test_null_handling() -> AxionResult<()> {
    let series_with_nulls = Series::new_from_options("values".to_string(), vec![
        Some(10), None, Some(20), None, Some(30)
    ]);
    
    // 使用已实现的空值检测方法
    let null_mask = series_with_nulls.is_null();
    let not_null_mask = series_with_nulls.not_null();
    
    println!("Original series: {:?}", series_with_nulls.data);
    println!("Null mask: {:?}", null_mask.data);
    println!("Not null mask: {:?}", not_null_mask.data);
    
    // 使用已实现的fill_null方法
    let filled_series = series_with_nulls.fill_null(0);
    println!("After filling nulls with 0: {:?}", filled_series.data);
    
    // 测试有效值迭代器
    println!("Valid values:");
    for value in series_with_nulls.iter_valid() {
        println!("  {}", value);
    }
    
    Ok(())
}

// ===== 8. 数学运算 =====
#[test]
fn test_arithmetic_operations() -> AxionResult<()> {
    let series1 = Series::new("a".to_string(), vec![1, 2, 3, 4]);
    let series2 = Series::new("b".to_string(), vec![5, 6, 7, 8]);
    
    // Series间运算
    let sum_result = &series1 + &series2;
    let sub_result = &series2 - &series1;
    let mul_result = &series1 * &series2;
    let div_result = &series2 / &series1;
    
    println!("Series 1: {:?}", series1.data);
    println!("Series 2: {:?}", series2.data);
    println!("Addition: {:?}", sum_result.data);
    println!("Subtraction: {:?}", sub_result.data);
    println!("Multiplication: {:?}", mul_result.data);
    println!("Division: {:?}", div_result.data);
    
    // 与标量运算
    let scalar_mul = &series1 * 2;
    let scalar_add = &series1 + 10;
    
    println!("Series * 2: {:?}", scalar_mul.data);
    println!("Series + 10: {:?}", scalar_add.data);
    
    Ok(())
}

// ===== 9. 字符串操作 =====
#[test]
fn test_string_operations() -> AxionResult<()> {
    let string_series = Series::new("names".to_string(), vec![
        "alice".to_string(),
        "bob".to_string(),
        "charlie".to_string()
    ]);
    
    // 使用已实现的字符串访问器
    let str_accessor = string_series.str();
    
    // 测试字符串长度
    // let lengths = str_accessor.len();
    // println!("Original strings: {:?}", string_series.data);
    // println!("String lengths: {:?}", lengths.data);
    
    // 测试大小写转换
    // let uppercase = str_accessor.to_uppercase();
    // let lowercase = str_accessor.to_lowercase();
    // println!("Uppercase: {:?}", uppercase.data);
    // println!("Lowercase: {:?}", lowercase.data);
    
    // 测试字符串包含
    // let contains_a = str_accessor.contains("a");
    // println!("Contains 'a': {:?}", contains_a.data);
    
    // 测试前缀和后缀
    // let starts_with_a = str_accessor.starts_with("a");
    // let ends_with_e = str_accessor.ends_with("e");
    // println!("Starts with 'a': {:?}", starts_with_a.data);
    // println!("Ends with 'e': {:?}", ends_with_e.data);
    
    Ok(())
}

// ===== 10. 函数式编程 =====
#[test]
fn test_apply_operations() -> AxionResult<()> {
    let series = Series::new("numbers".to_string(), vec![1, 2, 3, 4, 5]);
    
    // 测试apply方法
    let squared = series.apply(|opt_val| {
        opt_val.map(|&x| x * x)
    });
    
    println!("Original: {:?}", series.data);
    println!("Squared: {:?}", squared.data);
    
    // 测试并行apply
    let par_cubed = series.par_apply(|opt_val| {
        opt_val.map(|&x| x * x * x)
    });
    
    println!("Parallel cubed: {:?}", par_cubed.data);
    
    // 测试复杂的apply操作
    let complex_transform = series.apply(|opt_val| {
        opt_val.map(|&x| if x % 2 == 0 { x * 10 } else { x })
    });
    
    println!("Complex transform (even * 10): {:?}", complex_transform.data);
    
    assert_eq!(squared.len(), series.len());
    assert_eq!(par_cubed.len(), series.len());
    Ok(())
}

// ===== 11. 连接操作 =====
#[test]
fn test_join_operations() -> AxionResult<()> {
    let left_df = DataFrame::new(vec![
        Box::new(Series::new("key".to_string(), vec!["A".to_string(), "B".to_string(), "C".to_string()])),
        Box::new(Series::new("left_value".to_string(), vec![1, 2, 3])),
    ])?;
    
    let right_df = DataFrame::new(vec![
        Box::new(Series::new("key".to_string(), vec!["B".to_string(), "C".to_string(), "D".to_string()])),
        Box::new(Series::new("right_value".to_string(), vec![20, 30, 40])),
    ])?;
    
    println!("Left DataFrame:\n{}", left_df);
    println!("Right DataFrame:\n{}", right_df);
    
    // 测试内连接
    let inner_joined = left_df.inner_join(&right_df, "key", "key")?;
    println!("Inner Join Result:\n{}", inner_joined);
    
    // 测试左连接
    let left_joined = left_df.left_join(&right_df, "key", "key")?;
    println!("Left Join Result:\n{}", left_joined);
    
    // 测试右连接
    let right_joined = left_df.right_join(&right_df, "key", "key")?;
    println!("Right Join Result:\n{}", right_joined);
    
    // 测试外连接
    let outer_joined = left_df.outer_join(&right_df, "key", "key")?;
    println!("Outer Join Result:\n{}", outer_joined);
    
    Ok(())
}

// ===== 12. 分组操作 =====
#[test]
fn test_groupby_operations() -> AxionResult<()> {
    let df = DataFrame::new(vec![
        Box::new(Series::new("category".to_string(), vec!["A".to_string(), "B".to_string(), "A".to_string(), "B".to_string(), "A".to_string()])),
        Box::new(Series::new("value".to_string(), vec![10, 20, 15, 25, 12])),
        Box::new(Series::new("score".to_string(), vec![85.5, 92.0, 78.5, 95.0, 88.0])),
    ])?;
    
    println!("Original DataFrame:\n{}", df);
    
    // // 创建分组对象
    // let grouped = df.groupby(vec!["category".to_string()])?;
    
    // // 测试聚合操作
    // let sum_result = grouped.sum()?;
    // println!("Group Sum:\n{}", sum_result);
    
    // let mean_result = grouped.mean()?;
    // println!("Group Mean:\n{}", mean_result);
    
    // let count_result = grouped.count()?;
    // println!("Group Count:\n{}", count_result);
    
    // let min_result = grouped.min()?;
    // println!("Group Min:\n{}", min_result);
    
    // let max_result = grouped.max()?;
    // println!("Group Max:\n{}", max_result);
    
    Ok(())
}

// ===== 13. 排序 =====
#[test]
fn test_sorting() -> AxionResult<()> {
    let series = Series::new("values".to_string(), vec![30, 10, 25, 5, 20]);
    
    println!("Original series: {:?}", series.data);
    
    // 测试升序排序
    let mut ascending_series = series.clone();
    ascending_series.sort(false); // false = ascending
    println!("Ascending sort: {:?}", ascending_series.data);
    
    // 测试降序排序
    let mut descending_series = series.clone();
    descending_series.sort(true); // true = descending
    println!("Descending sort: {:?}", descending_series.data);
    
    // 测试排序状态检查
    println!("Is sorted after ascending: {}", ascending_series.is_sorted());
    
    Ok(())
}

// // ===== 14. 类型转换 =====
// #[test]
// fn test_type_casting() -> AxionResult<()> {
//     let int_series = Series::new("integers".to_string(), vec![1, 2, 3, 4, 5]);
    
//     // 转换为浮点数
//     let float_series = int_series.cast::<f64>()?;
//     println!("Original integers: {:?}", int_series.data);
//     println!("Cast to floats: {:?}", float_series.data);
    
//     // 测试字符串系列
//     let string_series = Series::new("strings".to_string(), vec!["1".to_string(), "2".to_string(), "3".to_string()]);
    
//     // 可以尝试转换为整数（如果实现了的话）
//     println!("String series: {:?}", string_series.data);
    
//     // assert_eq!(float_series.len(), int_series.len());
//     println!("Float series length: {}", float_series.len());
//     println!("String series length: {}", string_series.len());

//     Ok(())
// }

// ===== 15. 性能测试 =====
#[test]
fn test_performance() -> AxionResult<()> {
    use std::time::Instant;
    
    println!("=== Performance Test ===");
    
    // 创建大型Series
    let start = Instant::now();
    let large_series = Series::new("large".to_string(), (0..1_000_000).collect::<Vec<i32>>());
    let creation_time = start.elapsed();
    println!("Created 1M element series in: {:?}", creation_time);
    
    // 测试普通apply
    let start = Instant::now();
    let _result1 = large_series.apply(|opt_val| {
        opt_val.map(|&x| x * 2 + 1)
    });
    let apply_time = start.elapsed();
    println!("Sequential apply time: {:?}", apply_time);
    
    // 测试并行apply
    let start = Instant::now();
    let _result2 = large_series.par_apply(|opt_val| {
        opt_val.map(|&x| x * 2 + 1)
    });
    let par_apply_time = start.elapsed();
    println!("Parallel apply time: {:?}", par_apply_time);
    
    if apply_time > par_apply_time {
        println!("Speedup: {:.2}x", apply_time.as_secs_f64() / par_apply_time.as_secs_f64());
    }
    
    // 测试过滤性能
    let start = Instant::now();
    let mask = large_series.gt(500_000)?;
    let mask_time = start.elapsed();
    println!("Mask creation time: {:?}", mask_time);
    
    Ok(())
}

// ===== 16. CSV高级操作 =====
#[test]
fn test_csv_options() -> AxionResult<()> {
    // 创建测试CSV文件
    let content = "# This is a comment\nname,age,salary\nAlice,25,50000\nBob,30,60000\nCharlie,35,70000\n";
    std::fs::create_dir_all("data").ok();
    std::fs::write("data/test_with_comments.csv", content)?;
    
    // 使用自定义选项读取
    let options = ReadCsvOptions::builder()
        .with_header(true)
        .skip_rows(1) // 跳过注释行
        .build();
    
    let df = read_csv("data/test_with_comments.csv", Some(options))?;
    println!("CSV with options:\n{}", df);
    println!("Columns: {:?}", df.columns_names());
    println!("Data types: {:?}", df.dtypes());
    
    Ok(())
}

// ===== 17. DataFrame操作综合测试 =====
#[test]
fn test_dataframe_comprehensive() -> AxionResult<()> {
    let mut df = DataFrame::new_empty();
    
    // 添加列
    df.add_column(Box::new(Series::new("ID".to_string(), vec![1, 2, 3, 4, 5])))?;
    df.add_column(Box::new(Series::new("Name".to_string(), vec!["Alice".to_string(), "Bob".to_string(), "Charlie".to_string(), "David".to_string(), "Eve".to_string()])))?;
    df.add_column(Box::new(Series::new("Age".to_string(), vec![25, 30, 28, 35, 22])))?;
    df.add_column(Box::new(Series::new("Salary".to_string(), vec![50000.0, 60000.0, 55000.0, 70000.0, 45000.0])))?;
    
    println!("Initial DataFrame:\n{}", df);
    
    // 重命名列
    df.rename_column("ID", "EmployeeID")?;
    println!("After renaming ID to EmployeeID:\n{}", df);
    
    // 删除列
    let dropped_col = df.drop_column("Salary")?;
    println!("Dropped column: {}", dropped_col.name());
    println!("After dropping Salary:\n{}", df);
    
    // 检查DataFrame状态
    println!("Is empty: {}", df.is_empty());
    println!("Shape: {} × {}", df.height(), df.width());
    
    Ok(())
}

// ===== 18. 复杂查询测试 =====
#[test]
fn test_complex_queries() -> AxionResult<()> {
    // let df = DataFrame::new(vec![
    //     Box::new(Series::new("Department".to_string(), vec!["IT".to_string(), "HR".to_string(), "IT".to_string(), "Finance".to_string(), "HR".to_string()])),
    //     Box::new(Series::new("Name".to_string(), vec!["Alice".to_string(), "Bob".to_string(), "Charlie".to_string(), "David".to_string(), "Eve".to_string()])),
    //     Box::new(Series::new("Age".to_string(), vec![25, 30, 28, 35, 22])),
    //     Box::new(Series::new("Salary".to_string(), vec![50000.0, 60000.0, 55000.0, 70000.0, 45000.0])),
    //     Box::new(Series::new("Experience".to_string(), vec![2, 5, 3, 8, 1])),
    // ])?;
    
    let df = df!(
        "Department" => ["IT", "HR", "IT", "Finance", "HR"],
        "Name" => ["Alice", "Bob", "Charlie", "David", "Eve"],
        "Age" => [25, 30, 28, 35, 22],
        "Salary" => [50000.0, 60000.0, 55000.0, 70000.0, 45000.0],
        "Experience" => [2, 5, 3, 8, 1]
    )?;

    println!("Employee DataFrame:\n{}", df);
    
    // 复杂过滤：年龄大于25且工资大于50000
    let age_col: &Series<i32> = df.downcast_column("Age")?;
    let salary_col: &Series<f64> = df.downcast_column("Salary")?;
    
    let age_mask = age_col.gt(25)?;
    let salary_mask = salary_col.gt(50000.0)?;
    
    // 需要实现逻辑运算，这里先用单个条件
    let filtered_by_age = df.filter(&age_mask)?;
    println!("Employees with Age > 25:\n{}", filtered_by_age);
    
    let filtered_by_salary = df.filter(&salary_mask)?;
    println!("Employees with Salary > 50000:\n{}", filtered_by_salary);
    
    // 选择特定列的组合
    let summary = df.select(&["Name", "Department", "Salary"])?;
    println!("Employee Summary:\n{}", summary);
    
    Ok(())
}

// 主函数保持简单
fn main() -> AxionResult<()> {
    println!("🚀 Axion数据处理库功能演示");
    println!("运行 `cargo test` 查看所有测试结果");
    
    // 可以在这里运行一个简单的演示
    let df = DataFrame::new(vec![
        Box::new(Series::new("Name".to_string(), vec!["Alice".to_string(), "Bob".to_string()])),
        Box::new(Series::new("Age".to_string(), vec![25, 30])),
    ])?;
    
    println!("示例DataFrame:\n{}", df);
    println!("形状: {} × {}", df.height(), df.width());
    
    Ok(())
}