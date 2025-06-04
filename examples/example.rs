use axion::{
    read_csv,    // 假设返回 AxionResult<DataFrame>
    // DataFrame,   // axion::DataFrame
    Series,      // axion::Series, 用于创建掩码等
    AxionResult, // axion::AxionResult
    // AxionError, // 如果需要特定的错误处理
    // DataType, // 如果需要显式类型处理
};
// use axion::SeriesTrait; // 如果直接使用 SeriesTrait 的方法

fn main() -> AxionResult<()> {
    let path = "data/train.csv"; // 确保 "data/train.csv" 文件存在于项目根目录下的 data 文件夹中
    println!("尝试从路径读取 CSV: {}", path);

    // 确保 data 目录和 train.csv 文件存在
    // 如果文件不在预期的位置，read_csv 可能会失败
    // 建议在运行前检查文件路径
    if !std::path::Path::new(path).exists() {
        eprintln!("错误: CSV 文件未找到于 '{}'", path);
        eprintln!("请确保 'data/train.csv' 文件存在于正确的位置。");
        // 返回一个错误或 panic，取决于你希望如何处理
        return Err(axion::AxionError::IoError(
            format!("CSV file not found at '{}'", path),
        ));
    }

    let df = match read_csv(path, None) {
        Ok(dataframe) => dataframe,
        Err(e) => {
            eprintln!("读取 CSV 文件时发生错误: {:?}", e);
            eprintln!("请检查文件格式和内容是否正确。");
            return Err(e);
        }
    };
    println!("成功读取 {} 行和 {} 列。", df.height(), df.width());

    // --- 1. 查看数据基本信息 ---
    println!("\n--- 1. 数据基本信息 ---");
    println!("数据总行数: {}", df.height());
    println!("列名: {:?}", df.columns_names());
    println!("数据类型: {:?}", df.dtypes()); // 假设 dtypes() 返回 Vec<DataType> 或类似
    println!("前 5 行:\n{}", df.head(5));    // 假设 head() 返回一个新的 DataFrame

    // --- 2. 数据筛选与过滤 ---
    println!("\n--- 2. 数据筛选与过滤 ---");

    // 筛选所有幸存者 (Survived == 1)
    // 假设 "Survived" 列在 read_csv 后是 i64 或类似整数类型。
    // 您可能需要根据 read_csv 的实际行为调整这里的类型。
    match df.downcast_column::<i64>("Survived") {
        Ok(survived_col) => {
            // 修改点：apply 只接收闭包，且闭包返回 Option<bool>
            let survived_mask: Series<bool> = survived_col.apply(|opt_val| {
                Some(opt_val.is_some_and(|val| *val == 1)) // 将 bool 结果包裹在 Some() 中
            });
            // 如果需要，可以在这里给 survived_mask 命名，例如:
            // let survived_mask = survived_mask.rename("survived_mask".to_string());
            match df.par_filter(&survived_mask) { // 展示并行过滤
                Ok(df_survivors) => {
                    println!("幸存者人数: {}", df_survivors.height());
                    println!("幸存者 (前 5 行):\n{}", df_survivors.head(5));
                }
                Err(e) => eprintln!("筛选幸存者时出错: {:?}", e),
            }
        }
        Err(e) => eprintln!("获取 'Survived' 列失败 (可能类型不匹配或列不存在): {:?}", e),
    }


    // 筛选女性乘客 (Sex == "female")
    // 假设 "Sex" 列是 String 类型。
    match df.downcast_column::<String>("Sex") {
        Ok(sex_col) => {
            // 修改点：apply 只接收闭包，且闭包返回 Option<bool>
            let female_mask: Series<bool> = sex_col.apply(|opt_sex| {
                Some(opt_sex.is_some_and(|sex| sex == "female")) // 将 bool 结果包裹在 Some() 中
            });
            // 如果需要，可以在这里给 female_mask 命名
            // let female_mask = female_mask.rename("female_mask".to_string());
            match df.filter(&female_mask) {
                Ok(df_females) => {
                    println!("女性乘客人数: {}", df_females.height());
                    println!("女性乘客 (前 5 行):\n{}", df_females.head(5));
                }
                Err(e) => eprintln!("筛选女性乘客时出错: {:?}", e),
            }
        }
        Err(e) => eprintln!("获取 'Sex' 列失败 (可能类型不匹配或列不存在): {:?}", e),
    }


    // --- 3. 新增/变换列 ---
    println!("\n--- 3. 新增/变换列 ---");
    // 增加一列“IsChild” (Age < 18)，处理潜在的 Age 空值
    // 假设 "Age" 列是 Option<f64> 类型 (CSV 读取时可能包含空值，且年龄可以是小数)
    match df.downcast_column::<f64>("Age") {
        Ok(age_col_opt_f64) => {
            let is_child_series: Series<bool> = age_col_opt_f64.apply(|opt_age_val_ref| {
                // 假设 apply 传递 Option<&Option<f64>> 或 Option<f64_val>
                // 如果是 Option<&Option<f64>>:
                // opt_age_val_ref.and_then(|inner_opt_ref| *inner_opt_ref).map_or(false, |age_val| age_val < 18.0)
                // 如果是 Option<f64_val> (更简单):
                Some(opt_age_val_ref.is_some_and(|age_val| *age_val < 18.0))
            });
            let mut df_with_child = df.clone(); // 克隆以实现不可变风格的添加列
            match df_with_child.add_column(Box::new(is_child_series)) {
                Ok(_) => println!("新增 IsChild 列后的 DataFrame (前 5 行):\n{}", df_with_child.head(5)),
                Err(e) => eprintln!("添加 'IsChild' 列时出错: {:?}", e),
            }
        }
        Err(e) => eprintln!("获取 'Age' 列失败 (可能类型不匹配或列不存在): {:?}", e),
    }


    // --- 4. 分组与聚合 ---
    println!("\n--- 4. 分组与聚合 ---");

    // 按性别统计幸存率 (Survived 列需要是数值类型)
    // 为进行平均值计算，最好将 Survived 列转换为 f64。
    if let Ok(survived_col_for_agg) = df.downcast_column::<i64>("Survived") { // 或原始类型
        let survived_as_f64_series = survived_col_for_agg.apply(|opt_val| {
            opt_val.map(|v| *v as f64) // 将 i64 转换为 f64
        });

        let mut df_for_groupby = df.clone();
        // 替换/添加 Survived_f64 列用于聚合
        if df_for_groupby.column("Survived_f64").is_err() {
            if df_for_groupby.column("Survived").is_ok() {
                 if let Err(e) = df_for_groupby.drop_column("Survived") {
                    eprintln!("移除旧 'Survived' 列失败: {:?}", e);
                 }
            }
            if let Err(e) = df_for_groupby.add_column(Box::new(survived_as_f64_series)) {
                eprintln!("添加 'Survived_f64' 列失败: {:?}", e);
            }
        }

        println!("按性别统计平均幸存值 (幸存率):");
        match df_for_groupby.groupby(&["Sex"]) {
            Ok(grouped_by_sex) => {
                match grouped_by_sex.mean() { // 假设 GroupBy 有 mean() 方法
                    Ok(sex_survival_agg) => {
                        if let Ok(selected_agg) = sex_survival_agg.select(&["Sex", "Survived_f64"]) {
                             println!("{}", selected_agg);
                        } else {
                            eprintln!("选择聚合结果列失败。");
                            println!("完整聚合结果:\n{}", sex_survival_agg);
                        }
                    }
                    Err(e) => eprintln!("聚合错误 (Sex, Survived_f64 mean): {:?}", e),
                }
            }
            Err(e) => eprintln!("按 Sex 分组错误: {:?}", e),
        }
    } else {
        eprintln!("无法获取 'Survived' 列进行聚合。");
    }


    // 按 Pclass 统计平均票价 "Fare"
    // 假设 "Fare" 列是 f64 类型
    println!("按 Pclass 统计平均票价:");
    match df.groupby(&["Pclass"]) {
        Ok(grouped_by_pclass) => {
            match grouped_by_pclass.mean() {
                Ok(pclass_fare_agg) => {
                     if let Ok(selected_agg) = pclass_fare_agg.select(&["Pclass", "Fare"]) {
                        println!("{}", selected_agg);
                    } else {
                        eprintln!("选择聚合结果列失败。");
                        println!("完整聚合结果:\n{}", pclass_fare_agg);
                    }
                }
                Err(e) => eprintln!("聚合错误 (Pclass, Fare mean): {:?}", e),
            }
        }
        Err(e) => eprintln!("按 Pclass 分组错误: {:?}", e),
    }

    // --- 5. 选择部分列 ---
    println!("\n--- 5. 选择部分列 ---");
    match df.select(&["Name", "Age", "Sex", "Survived"]) {
        Ok(selected_df) => {
            println!("选择 Name, Age, Sex, Survived (前 5 行):\n{}", selected_df.head(5));
        }
        Err(e) => eprintln!("选择列时出错: {:?}", e),
    }


    // --- 6. 排序 ---
    println!("\n--- 6. 排序 ---");
    // 按 "Fare" 降序, "Age" 升序
    // 确保 Age 列的比较能正确处理 None 值
    match df.sort(&["Fare", "Age"], &[true, false]) { // true for descending, false for ascending
        Ok(sorted_df) => {
            println!("按 Fare 降序、Age 升序排序 (前 5 行):\n{}", sorted_df.head(5));
        }
        Err(e) => eprintln!("排序时出错: {:?}", e),
    }


    // --- 7. 结合多条件筛选 (女性幸存者) ---
    println!("\n--- 7. 结合多条件筛选 ---");
    // 重新获取掩码，以防之前的 df 实例已更改或作用域问题
    if let Ok(sex_col_for_multi) = df.downcast_column::<String>("Sex") {
        let female_mask_multi: Series<bool> = sex_col_for_multi.apply(|opt_sex| {
            Some(opt_sex.is_some_and(|sex| sex == "female"))
        });

        match df.filter(&female_mask_multi) {
            Ok(df_females_for_multi) => {
                // 现在从 df_females_for_multi (只包含女性乘客的DataFrame) 中获取 "Survived" 列
                match df_females_for_multi.downcast_column::<i64>("Survived") {
                    Ok(survived_col_from_females) => { // 这是女性乘客的 "Survived" 列
                        let survived_mask_for_females: Series<bool> = survived_col_from_females.apply(|opt_val| {
                            Some(opt_val.is_some_and(|val| *val == 1))
                        });

                        // 使用针对女性乘客的幸存掩码来筛选女性乘客 DataFrame
                        match df_females_for_multi.filter(&survived_mask_for_females) {
                            Ok(df_female_survivors_sequential) => {
                                println!("女性幸存者人数 (序贯筛选): {}", df_female_survivors_sequential.height());
                                println!("女性幸存者 (序贯筛选, 前 5 行):\n{}", df_female_survivors_sequential.head(5));
                            }
                            Err(e) => eprintln!("序贯筛选 (survived from females) 时出错: {:?}", e),
                        }
                    }
                    Err(e) => eprintln!("多条件筛选：无法从女性乘客 DataFrame 中获取 'Survived' 列: {:?}", e),
                }
            }
            Err(e) => eprintln!("序贯筛选 (female) 时出错: {:?}", e),
        }
    } else {
        eprintln!("多条件筛选：无法获取 'Sex' 列。");
    }


    // (to_csv 方法未在 core.rs 中提供，故省略)
    // (年龄分箱 (cut) 功能未在 Series 中提供，故省略)

    println!("\n--- Axion DataFrame 演示完成 ---");

    Ok(())
}