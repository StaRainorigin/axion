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
    println!("包含空值的Series: {:?}", s2);
    assert_eq!(s2.len(), 4);
}

#[test]
fn test_create_dataframe() -> AxionResult<()> {
    let df = DataFrame::new(vec![
        Box::new(Series::new("Name".to_string(), vec!["Alice".to_string(), "Bob".to_string(), "Charlie".to_string()])),
        Box::new(Series::new("Age".to_string(), vec![25, 30, 28])),
        Box::new(Series::new("City".to_string(), vec!["New York".to_string(), "London".to_string(), "Paris".to_string()])),
    ])?;
    
    println!("已创建DataFrame: {} 行 × {} 列", df.height(), df.width());
    println!("列名: {:?}", df.columns_names());
    println!("Schema: {:?}", df.schema());
    assert_eq!(df.height(), 3);
    assert_eq!(df.width(), 3);
    Ok(())
}

// ===== 3. 读取数据 =====
#[test]
fn test_read_csv() -> AxionResult<()> {
    // CSV读取
    let path = "data/train.csv";
    
    if std::path::Path::new(path).exists() {
        let df = read_csv(path, None)?;
        println!("成功读取CSV: {} 行 × {} 列", df.height(), df.width());
        println!("列名: {:?}", df.columns_names());
        println!("数据类型: {:?}", df.dtypes());
        println!("前3行:\n{}", df.head(3));
    } else {
        println!("未找到CSV文件: {}", path);

        // 创建测试CSV文件
        use std::io::Write;
        let content = "name,age,salary\nAlice,25,50000\nBob,30,60000\nCharlie,35,70000\n";
        std::fs::create_dir_all("data").ok();
        let mut file = std::fs::File::create("data/test.csv")?;
        file.write_all(content.as_bytes())?;
        
        let df = read_csv("data/test.csv", None)?;
        println!("测试CSV读取成功: {} 行 × {} 列", df.height(), df.width());
    }
    Ok(())
}

// ===== 4. 查看数据 =====
#[test]
fn test_head_tail() -> AxionResult<()> {
    let df = df!(
        "Name" => &["Alice", "Bob", "Charlie", "David", "Eve"],
        "Age" => &[25, 30, 28, 35, 22],
        "Salary" => &[50000.0, 60000.0, 55000.0, 70000.0, 45000.0]
    )?; // 使用宏创建DataFrame
    
    // 使用head和tail方法
    let head_df = df.head(3);
    let tail_df = df.tail(2);
    
    println!("DataFrame: {} 行", df.height());
    println!("前3行:\n{}", head_df);
    println!("后2行:\n{}", tail_df);
    
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
    
    println!("年龄统计:");
    println!("  计数: {}", age_col.len());
    println!("  平均值: {:.2}", age_col.mean().unwrap_or(0.0));
    println!("  最小值: {}", age_col.min().unwrap_or(0));
    println!("  最大值: {}", age_col.max().unwrap_or(0));
    println!("  总和: {}", age_col.sum().unwrap_or(0));
    
    println!("\n工资统计:");
    println!("  平均值: {:.2}", salary_col.mean().unwrap_or(0.0));
    println!("  最小值: {:.2}", salary_col.min().unwrap_or(0.0));
    println!("  最大值: {:.2}", salary_col.max().unwrap_or(0.0));
    println!("  总和: {:.2}", salary_col.sum().unwrap_or(0.0));
    
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
    
    // 使用select方法
    let selected_df = df.select(&["Name", "Age"])?;
    println!("选择的列: {} 行 × {} 列", selected_df.height(), selected_df.width());
    println!("选择后的DataFrame:\n{}", selected_df);
    
    // 使用drop方法
    let dropped_df = df.drop("Salary")?;
    println!("删除Salary后: {} 列", dropped_df.width());
    println!("删除后的DataFrame:\n{}", dropped_df);
    
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
    println!("Name列类型: {:?}", name_col.dtype());
    
    let age_col: &Series<i32> = df.downcast_column("Age")?;
    println!("Age列值: {:?}", age_col.data);
    println!("第一个年龄值: {:?}", age_col.get(0));
    
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
    let age_mask = age_col.gt(26)?;
    println!("年龄 > 26的掩码: {:?}", age_mask.data);
    
    // 使用filter方法
    let filtered_df = df.filter(&age_mask)?;
    println!("过滤后的DataFrame (年龄 > 26):\n{}", filtered_df);
    println!("过滤后行数: {}", filtered_df.height());
    
    // 测试并行过滤
    let par_filtered_df = df.par_filter(&age_mask)?;
    println!("并行过滤后的DataFrame:\n{}", par_filtered_df);
    
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
    
    println!("原始数据: {:?}", series.data);
    println!("值 > 3: {:?}", gt_mask.data);
    println!("值 < 5: {:?}", lt_mask.data);
    println!("值 == 5: {:?}", eq_mask.data);
    println!("值 >= 3: {:?}", ge_mask.data);
    println!("值 <= 3: {:?}", le_mask.data);
    
    Ok(())
}

// ===== 7. 空值处理 =====
#[test]
fn test_null_handling() -> AxionResult<()> {
    let series_with_nulls = Series::new_from_options("values".to_string(), vec![
        Some(10), None, Some(20), None, Some(30)
    ]);
    
    // 使用空值检测方法
    let null_mask = series_with_nulls.is_null();
    let not_null_mask = series_with_nulls.not_null();
    
    println!("原始数列: {:?}", series_with_nulls.data);
    println!("空值掩码: {:?}", null_mask.data);
    println!("非空值掩码: {:?}", not_null_mask.data);
    
    // 使用fill_null方法
    let filled_series = series_with_nulls.fill_null(0);
    println!("用0填充空值后: {:?}", filled_series.data);
    
    // 测试有效值迭代器
    println!("有效值:");
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
    
    println!("数列1: {:?}", series1.data);
    println!("数列2: {:?}", series2.data);
    println!("加法: {:?}", sum_result.data);
    println!("减法: {:?}", sub_result.data);
    println!("乘法: {:?}", mul_result.data);
    println!("除法: {:?}", div_result.data);
    
    // 与标量运算
    let scalar_mul = &series1 * 2;
    let scalar_add = &series1 + 10;
    
    println!("数列 * 2: {:?}", scalar_mul.data);
    println!("数列 + 10: {:?}", scalar_add.data);
    
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
    Ok(())
}

// ===== 10. 函数式编程 =====
#[test]
fn test_apply_operations() -> AxionResult<()> {
    let series = Series::new("numbers".to_string(), vec![1, 2, 3, 4, 5]);
    
    // apply方法
    let squared = series.apply(|opt_val| {
        opt_val.map(|&x| x * x)
    });
    
    println!("原始数据: {:?}", series.data);
    println!("平方: {:?}", squared.data);
    
    // 并行apply
    let par_cubed = series.par_apply(|opt_val| {
        opt_val.map(|&x| x * x * x)
    });
    
    println!("并行立方: {:?}", par_cubed.data);
    
    // 复杂的apply操作
    let complex_transform = series.apply(|opt_val| {
        opt_val.map(|&x| if x % 2 == 0 { x * 10 } else { x })
    });
    
    println!("复杂变换 (偶数 * 10): {:?}", complex_transform.data);
    
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
    
    println!("左表DataFrame:\n{}", left_df);
    println!("右表DataFrame:\n{}", right_df);
    
    // 内连接
    let inner_joined = left_df.inner_join(&right_df, "key", "key")?;
    println!("内连接结果:\n{}", inner_joined);
    
    // 左连接
    let left_joined = left_df.left_join(&right_df, "key", "key")?;
    println!("左连接结果:\n{}", left_joined);
    
    // 右连接
    let right_joined = left_df.right_join(&right_df, "key", "key")?;
    println!("右连接结果:\n{}", right_joined);
    
    // 外连接
    let outer_joined = left_df.outer_join(&right_df, "key", "key")?;
    println!("外连接结果:\n{}", outer_joined);
    
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
    
    println!("原始DataFrame:\n{}", df);
    
    // 创建分组对象
    let grouped = df.groupby(&["category"])?;
    
    // 测试聚合操作
    let sum_result = grouped.sum()?;
    println!("分组求和:\n{}", sum_result);
    
    let mean_result = grouped.mean()?;
    println!("分组平均值:\n{}", mean_result);
    
    let count_result = grouped.count()?;
    println!("分组计数:\n{}", count_result);
    
    let min_result = grouped.min()?;
    println!("分组最小值:\n{}", min_result);
    
    let max_result = grouped.max()?;
    println!("分组最大值:\n{}", max_result);
    
    Ok(())
}

// ===== 13. 排序 =====
#[test]
fn test_sorting() -> AxionResult<()> {
    let series = Series::new("values".to_string(), vec![30, 10, 25, 5, 20]);
    
    println!("原始数列: {:?}", series.data);
    
    // 测试升序排序
    let mut ascending_series = series.clone();
    ascending_series.sort(false); // false = ascending
    println!("升序排序: {:?}", ascending_series.data);
    
    // 测试降序排序
    let mut descending_series = series.clone();
    descending_series.sort(true); // true = descending
    println!("降序排序: {:?}", descending_series.data);
    
    // 测试排序状态检查
    println!("升序排序后是否已排序: {}", ascending_series.is_sorted());
    
    Ok(())
}

// ===== 14. CSV高级操作 =====
#[test]
fn test_csv_options() -> AxionResult<()> {
    // 创建CSV文件
    let content = "# This is a comment\nname,age,salary\nAlice,25,50000\nBob,30,60000\nCharlie,35,70000\n";
    std::fs::create_dir_all("data").ok();
    std::fs::write("data/test_with_comments.csv", content)?;
    
    // 使用自定义选项读取
    let options = ReadCsvOptions::builder()
        .with_header(true)
        .skip_rows(1) // 跳过注释行
        .build();
    
    let df = read_csv("data/test_with_comments.csv", Some(options))?;
    println!("带选项的CSV:\n{}", df);
    println!("列名: {:?}", df.columns_names());
    println!("数据类型: {:?}", df.dtypes());
    
    Ok(())
}

// ===== 15. DataFrame操作综合测试 =====
#[test]
fn test_dataframe_comprehensive() -> AxionResult<()> {
    let mut df = DataFrame::new_empty();
    
    // 添加列
    df.add_column(Box::new(Series::new("ID".to_string(), vec![1, 2, 3, 4, 5])))?;
    df.add_column(Box::new(Series::new("Name".to_string(), vec!["Alice".to_string(), "Bob".to_string(), "Charlie".to_string(), "David".to_string(), "Eve".to_string()])))?;
    df.add_column(Box::new(Series::new("Age".to_string(), vec![25, 30, 28, 35, 22])))?;
    df.add_column(Box::new(Series::new("Salary".to_string(), vec![50000.0, 60000.0, 55000.0, 70000.0, 45000.0])))?;
    
    println!("初始DataFrame:\n{}", df);
    
    // 重命名列
    df.rename_column("ID", "EmployeeID")?;
    println!("将ID重命名为EmployeeID后:\n{}", df);
    
    // 删除列
    let dropped_col = df.drop_column("Salary")?;
    println!("删除的列: {}", dropped_col.name());
    println!("删除Salary后:\n{}", df);
    
    // 检查DataFrame状态
    println!("是否为空: {}", df.is_empty());
    println!("形状: {} × {}", df.height(), df.width());
    
    Ok(())
}

// ===== 16. 复杂查询测试 =====
#[test]
fn test_complex_queries() -> AxionResult<()> {
    
    let df = df!(
        "Department" => ["IT", "HR", "IT", "Finance", "HR"],
        "Name" => ["Alice", "Bob", "Charlie", "David", "Eve"],
        "Age" => [25, 30, 28, 35, 22],
        "Salary" => [50000.0, 60000.0, 55000.0, 70000.0, 45000.0],
        "Experience" => [2, 5, 3, 8, 1]
    )?;

    println!("员工DataFrame:\n{}", df);
    
    // 复杂过滤：年龄大于25且工资大于50000
    let age_col: &Series<i32> = df.downcast_column("Age")?;
    let salary_col: &Series<f64> = df.downcast_column("Salary")?;
    
    let age_mask = age_col.gt(25)?;
    let salary_mask = salary_col.gt(50000.0)?;
    
    // 需要实现逻辑运算，这里用单个条件
    let filtered_by_age = df.filter(&age_mask)?;
    println!("年龄 > 25的员工:\n{}", filtered_by_age);
    
    let filtered_by_salary = df.filter(&salary_mask)?;
    println!("工资 > 50000的员工:\n{}", filtered_by_salary);
    
    // 选择特定列的组合
    let summary = df.select(&["Name", "Department", "Salary"])?;
    println!("员工摘要:\n{}", summary);
    
    Ok(())
}

// 主函数
fn main() -> AxionResult<()> {
    println!("Axion数据处理库功能演示");
    println!("运行 `cargo test` 查看所有测试结果");
    
    let df = DataFrame::new(vec![
        Box::new(Series::new("Name".to_string(), vec!["Alice".to_string(), "Bob".to_string()])),
        Box::new(Series::new("Age".to_string(), vec![25, 30])),
    ])?;
    
    println!("示例DataFrame:\n{}", df);
    println!("形状: {} × {}", df.height(), df.width());
    
    Ok(())
}