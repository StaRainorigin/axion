/// 创建 DataFrame 的便捷宏
/// 
/// 提供了一种简洁的语法来创建 DataFrame，支持多种使用模式。
/// 
/// # 语法
/// 
/// - `df!("列名": 类型 => 数据, ...)` - 带显式类型声明
/// - `df!("列名" => 数据, ...)` - 自动类型推断
/// - `df!()` - 创建空 DataFrame
/// 
/// # 示例
/// 
/// ```rust
/// use axion::df;
/// 
/// // 类型推断
/// let df1 = df! {
///     "name" => vec!["Alice", "Bob"],
///     "age" => vec![25, 30]
/// }?;
/// 
/// // 显式类型
/// let df2 = df! {
///     "id": i32 => vec![1, 2],
///     "score": f64 => vec![95.5, 87.2]
/// }?;
/// 
/// // 空 DataFrame
/// let empty = df!()?;
/// ```
/// 
/// # 返回值
/// 
/// 返回 `AxionResult<DataFrame>`
/// 
/// # 错误
/// 
/// 当列长度不匹配或存在重复列名时返回错误
#[macro_export]
macro_rules! df {
    // 匹配带显式类型的所有列
    ($($name:literal : $type:ty => $data:expr),+ $(,)?) => {
        (|| -> $crate::error::AxionResult<$crate::dataframe::DataFrame> {
            let mut cols: Vec<Box<dyn $crate::series::SeriesTrait>> = Vec::new();
            $(
                // 使用提供的显式类型
                let series = $crate::series::IntoSeriesBox::<$type>::into_series_box($data, $name.to_string())?;
                cols.push(series);
            )+
            $crate::dataframe::DataFrame::new(cols)
        })()
    };

    // 匹配不带显式类型的所有列（使用类型推断）
    ($($name:literal => $data:expr),+ $(,)?) => {
        (|| -> $crate::error::AxionResult<$crate::dataframe::DataFrame> {
            let mut cols: Vec<Box<dyn $crate::series::SeriesTrait>> = Vec::new();
            $(
                // 依赖类型推断
                let series = $crate::series::IntoSeriesBox::into_series_box($data, $name.to_string())?;
                cols.push(series);
            )+
            $crate::dataframe::DataFrame::new(cols)
        })()
    };

    // 匹配空 DataFrame 定义（可选）
    () => {
        (|| -> $crate::error::AxionResult<$crate::dataframe::DataFrame> {
            $crate::dataframe::DataFrame::new(Vec::new())
        })()
    };
}