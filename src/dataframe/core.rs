use crate::series::{SeriesTrait, Series};
use crate::dtype::{DataType, DataTypeTrait};
use crate::error::{AxionError, AxionResult};
use super::groupby::GroupBy;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Debug};
use std::cmp::Ordering;
use rayon::prelude::*;
use crate::io::csv::WriteCsvOptions;
use csv;
use std::io::Write;
use std::fs::File;
use std::path::Path;

/// 高性能数据处理框架的核心数据结构 DataFrame。
/// 
/// DataFrame 是一个二维表格数据结构，类似于电子表格或数据库表，
/// 由多个具有相同长度的列（Series）组成。每列可以包含不同类型的数据。
/// 
/// # 特性
/// 
/// - **类型安全**: 使用 Rust 的类型系统确保数据类型安全
/// - **高性能**: 利用 Rayon 实现并行处理
/// - **内存高效**: 零拷贝操作和智能内存管理
/// - **丰富的操作**: 支持过滤、连接、分组、排序等操作
/// 
/// # 示例
/// 
/// ```rust
/// use axion::dataframe::DataFrame;
/// use axion::series::Series;
/// 
/// // 创建一个简单的 DataFrame
/// let name_series = Series::new("姓名".to_string(), vec!["张三", "李四", "王五"]);
/// let age_series = Series::new("年龄".to_string(), vec![25, 30, 35]);
/// 
/// let df = DataFrame::new(vec![
///     Box::new(name_series),
///     Box::new(age_series),
/// ])?;
/// 
/// println!("{}", df);
/// ```
#[derive(Clone)]
pub struct DataFrame {
    /// DataFrame 的行数
    height: usize,
    /// 存储所有列的向量，每列都是一个实现了 SeriesTrait 的对象
    pub columns: Vec<Box<dyn SeriesTrait>>,
    /// 列名到数据类型的映射，用于快速查找和验证
    schema: HashMap<String, DataType>,
}

impl DataFrame {
    /// 从列向量创建新的 DataFrame。
    ///
    /// # 参数
    /// 
    /// * `columns` - 实现了 `SeriesTrait` 的列向量
    ///
    /// # 返回值
    /// 
    /// 成功时返回新创建的 DataFrame，失败时返回错误
    ///
    /// # 错误
    /// 
    /// * `AxionError::MismatchedLengths` - 当列长度不一致时
    /// * `AxionError::DuplicateColumnName` - 当存在重复列名时
    ///
    /// # 示例
    /// 
    /// ```rust
    /// let columns = vec![
    ///     Box::new(Series::new("A".to_string(), vec![1, 2, 3])),
    ///     Box::new(Series::new("B".to_string(), vec![4, 5, 6])),
    /// ];
    /// let df = DataFrame::new(columns)?;
    /// ```
    pub fn new(columns: Vec<Box<dyn SeriesTrait>>) -> AxionResult<Self> {
        let height = columns.first().map_or(0, |col| col.len());
        let mut schema = HashMap::with_capacity(columns.len());

        for col in &columns {
            if col.len() != height {
                return Err(AxionError::MismatchedLengths {
                    expected: height,
                    found: col.len(),
                    name: col.name().to_string(),
                });
            }
            if schema.insert(col.name().to_string(), col.dtype()).is_some() {
                return Err(AxionError::DuplicateColumnName(col.name().to_string()));
            }
        }

        Ok(DataFrame { height, columns, schema })
    }

    /// 创建一个空的 DataFrame。
    /// 
    /// # 返回值
    /// 
    /// 返回一个没有行和列的 DataFrame
    /// 
    /// # 示例
    /// 
    /// ```rust
    /// let empty_df = DataFrame::new_empty();
    /// assert_eq!(empty_df.shape(), (0, 0));
    /// ```
    pub fn new_empty() -> Self {
        DataFrame {
            height: 0,
            columns: Vec::new(),
            schema: HashMap::new(),
        }
    }

    /// 获取 DataFrame 的形状（行数，列数）。
    /// 
    /// # 返回值
    /// 
    /// 返回一个元组 `(行数, 列数)`
    /// 
    /// # 示例
    /// 
    /// ```rust
    /// let (rows, cols) = df.shape();
    /// println!("DataFrame 有 {} 行 {} 列", rows, cols);
    /// ```
    pub fn shape(&self) -> (usize, usize) {
        (self.height, self.columns.len())
    }

    /// 获取 DataFrame 的行数。
    pub fn height(&self) -> usize {
        self.height
    }

    /// 获取 DataFrame 的列数。
    pub fn width(&self) -> usize {
        self.columns.len()
    }

    /// 获取所有列名的向量。
    /// 
    /// # 返回值
    /// 
    /// 返回包含所有列名的字符串切片向量
    pub fn columns_names(&self) -> Vec<&str> {
        self.columns.iter().map(|col| col.name()).collect()
    }

    /// 获取所有列的数据类型。
    /// 
    /// # 返回值
    /// 
    /// 返回包含所有列数据类型的向量
    pub fn dtypes(&self) -> Vec<DataType> {
        self.columns.iter().map(|col| col.dtype()).collect()
    }

    /// 获取 DataFrame 的模式（列名到数据类型的映射）。
    pub fn schema(&self) -> &HashMap<String, DataType> {
        &self.schema
    }

    /// 根据列名获取列的引用。
    ///
    /// # 参数
    /// 
    /// * `name` - 要查找的列名
    ///
    /// # 返回值
    /// 
    /// 成功时返回列的引用，失败时返回 `ColumnNotFound` 错误
    pub fn column(&self, name: &str) -> AxionResult<&dyn SeriesTrait> {
        self.columns
            .iter()
            .find(|col| col.name() == name)
            .map(|col| col.as_ref())
            .ok_or_else(|| AxionError::ColumnNotFound(name.to_string()))
    }

    /// 根据列名获取列的可变引用。
    pub fn column_mut<'a>(&'a mut self, name: &str) -> AxionResult<&'a mut dyn SeriesTrait> {
        self.columns
            .iter_mut()
            .find(|col| col.name() == name)
            .map(|col| col.as_mut() as &mut dyn SeriesTrait)
            .ok_or_else(|| AxionError::ColumnNotFound(name.to_string()))
    }

    /// 根据索引获取列的引用。
    ///
    /// # 参数
    /// 
    /// * `index` - 列的索引位置
    pub fn column_at(&self, index: usize) -> AxionResult<&dyn SeriesTrait> {
        self.columns
            .get(index)
            .map(|col| col.as_ref())
            .ok_or_else(|| AxionError::ColumnNotFound(format!("index {}", index)))
    }

    /// 根据索引获取列的可变引用。
    pub fn column_at_mut(&mut self, index: usize) -> AxionResult<&mut dyn SeriesTrait> {
        self.columns
            .get_mut(index)
            .map(|col| col.as_mut() as &mut dyn SeriesTrait)
            .ok_or_else(|| AxionError::ColumnNotFound(format!("index {}", index)))
    }

    /// 向 DataFrame 添加一个新列。
    ///
    /// # 参数
    /// 
    /// * `series` - 要添加的列，必须实现 `SeriesTrait`
    ///
    /// # 错误
    /// 
    /// * `AxionError::MismatchedLengths` - 新列长度与现有行数不匹配
    /// * `AxionError::DuplicateColumnName` - 列名已存在
    ///
    /// # 示例
    /// 
    /// ```rust
    /// let new_col = Series::new("新列".to_string(), vec![1, 2, 3]);
    /// df.add_column(Box::new(new_col))?;
    /// ```
    pub fn add_column(&mut self, series: Box<dyn SeriesTrait>) -> AxionResult<()> {
        if self.columns.is_empty() && self.height == 0 {
            self.height = series.len();
        } else if series.len() != self.height {
            return Err(AxionError::MismatchedLengths {
                expected: self.height,
                found: series.len(),
                name: series.name().to_string(),
            });
        }

        if self.schema.contains_key(series.name()) {
            return Err(AxionError::DuplicateColumnName(series.name().to_string()));
        }

        self.schema.insert(series.name().to_string(), series.dtype());
        self.columns.push(series);
        Ok(())
    }

    /// 从 DataFrame 中删除指定列。
    ///
    /// # 参数
    /// 
    /// * `name` - 要删除的列名
    ///
    /// # 返回值
    /// 
    /// 返回被删除的列
    ///
    /// # 错误
    /// 
    /// * `AxionError::ColumnNotFound` - 指定列不存在
    pub fn drop_column(&mut self, name: &str) -> AxionResult<Box<dyn SeriesTrait>> {
        let position = self.columns.iter().position(|col| col.name() == name);

        if let Some(pos) = position {
            self.schema.remove(name);
            let removed_col = self.columns.remove(pos);
            if self.columns.is_empty() {
                self.height = 0;
            }
            Ok(removed_col)
        } else {
            Err(AxionError::ColumnNotFound(name.to_string()))
        }
    }

    /// 重命名 DataFrame 中的列。
    ///
    /// # 参数
    /// 
    /// * `old_name` - 当前列名
    /// * `new_name` - 新列名
    ///
    /// # 错误
    /// 
    /// * `AxionError::ColumnNotFound` - 原列名不存在
    /// * `AxionError::DuplicateColumnName` - 新列名已存在
    pub fn rename_column(&mut self, old_name: &str, new_name: &str) -> AxionResult<()> {
        if old_name == new_name {
            return Ok(());
        }

        if !self.schema.contains_key(old_name) {
            return Err(AxionError::ColumnNotFound(old_name.to_string()));
        }

        if self.schema.contains_key(new_name) {
            return Err(AxionError::DuplicateColumnName(new_name.to_string()));
        }

        let dtype = self.schema.remove(old_name).unwrap();
        self.schema.insert(new_name.to_string(), dtype);

        for col in self.columns.iter_mut() {
            if col.name() == old_name {
                col.rename(new_name);
                break;
            }
        }
        Ok(())
    }

    /// 将列向下转型为特定类型的 Series。
    ///
    /// # 类型参数
    /// 
    /// * `T` - 目标数据类型
    ///
    /// # 参数
    /// 
    /// * `name` - 列名
    ///
    /// # 返回值
    /// 
    /// 成功时返回指定类型的 Series 引用
    pub fn downcast_column<T>(&self, name: &str) -> AxionResult<&Series<T>>
    where
        T: DataTypeTrait + 'static,
        Series<T>: 'static,
    {
        let series_trait = self.column(name)?;
        series_trait
            .as_any()
            .downcast_ref::<Series<T>>()
            .ok_or_else(|| AxionError::TypeMismatch {
                expected: T::DTYPE,
                found: series_trait.dtype(),
                name: name.to_string(),
            })
    }

    /// 检查 DataFrame 是否为空。
    /// 
    /// # 返回值
    /// 
    /// 如果没有行或没有列则返回 true
    pub fn is_empty(&self) -> bool {
        self.height == 0 || self.columns.is_empty()
    }

    /// 获取 DataFrame 的前 n 行。
    ///
    /// # 参数
    /// 
    /// * `n` - 要获取的行数
    ///
    /// # 返回值
    /// 
    /// 返回包含前 n 行的新 DataFrame
    pub fn head(&self, n: usize) -> DataFrame {
        let n = std::cmp::min(n, self.height);
        if n == self.height {
            return self.clone();
        }
        let new_columns = self.columns.iter().map(|col| col.slice(0, n)).collect();
        DataFrame::new(new_columns).unwrap_or_else(|_| {
            DataFrame::new(vec![]).unwrap()
        })
    }

    /// 获取 DataFrame 的后 n 行。
    ///
    /// # 参数
    /// 
    /// * `n` - 要获取的行数
    pub fn tail(&self, n: usize) -> DataFrame {
        let n = std::cmp::min(n, self.height);
        if n == self.height {
            return self.clone();
        }
        let offset = self.height - n;
        let new_columns = self.columns.iter().map(|col| col.slice(offset, n)).collect();
        DataFrame::new(new_columns).unwrap_or_else(|_| {
            DataFrame::new(vec![]).unwrap()
        })
    }

    /// 选择指定的列创建新的 DataFrame。
    ///
    /// # 参数
    /// 
    /// * `names` - 要选择的列名数组
    ///
    /// # 返回值
    /// 
    /// 返回只包含指定列的新 DataFrame
    pub fn select(&self, names: &[&str]) -> AxionResult<DataFrame> {
        let mut new_columns = Vec::with_capacity(names.len());
        for name in names {
            let col = self.column(name)?;
            new_columns.push(col.clone_box());
        }
        DataFrame::new(new_columns)
    }

    /// 删除指定列后创建新的 DataFrame。
    ///
    /// # 参数
    /// 
    /// * `name_to_drop` - 要删除的列名
    pub fn drop(&self, name_to_drop: &str) -> AxionResult<DataFrame> {
        if !self.schema.contains_key(name_to_drop) {
            return Err(AxionError::ColumnNotFound(name_to_drop.to_string()));
        }

        let new_columns = self.columns
            .iter()
            .filter(|col| col.name() != name_to_drop)
            .map(|col| col.clone_box())
            .collect();

        DataFrame::new(new_columns)
    }

    /// 根据布尔掩码过滤 DataFrame 行。
    ///
    /// # 参数
    /// 
    /// * `mask` - 布尔类型的 Series，true 表示保留该行
    ///
    /// # 返回值
    /// 
    /// 返回过滤后的新 DataFrame
    ///
    /// # 错误
    /// 
    /// * `AxionError::MismatchedLengths` - 掩码长度与 DataFrame 行数不匹配
    pub fn filter(&self, mask: &Series<bool>) -> AxionResult<DataFrame> {
        if mask.len() != self.height {
            return Err(AxionError::MismatchedLengths {
                expected: self.height,
                found: mask.len(),
                name: "过滤掩码".to_string(),
            });
        }

        let mut filtered_columns = Vec::with_capacity(self.columns.len());
        for col in &self.columns {
            let filtered_col = col.filter(mask)?;
            filtered_columns.push(filtered_col);
        }

        DataFrame::new(filtered_columns)
    }

    /// 并行过滤 DataFrame 行，提供更好的性能。
    ///
    /// 该方法使用 Rayon 并行处理每一列的过滤操作，
    /// 在处理大型数据集时能显著提升性能。
    ///
    /// # 参数
    /// 
    /// * `mask` - 布尔类型的 Series，true 表示保留该行
    ///
    /// # 返回值
    /// 
    /// 返回过滤后的新 DataFrame
    pub fn par_filter(&self, mask: &Series<bool>) -> AxionResult<DataFrame> {
        if mask.len() != self.height {
            return Err(AxionError::MismatchedLengths {
                expected: self.height,
                found: mask.len(),
                name: "过滤掩码".to_string(),
            });
        }
        if self.is_empty() {
            return Ok(self.clone());
        }
        if mask.is_empty() && self.height > 0 {
             return Err(AxionError::MismatchedLengths {
                expected: self.height,
                found: mask.len(),
                name: "非空DataFrame的过滤掩码".to_string(),
            });
        }
        if mask.is_empty() && self.height == 0 {
            return Ok(self.clone());
        }

        let new_columns_results: Vec<AxionResult<Box<dyn SeriesTrait>>> = self
            .columns
            .par_iter()
            .map(|col| col.filter(mask))
            .collect();

        let mut new_columns = Vec::with_capacity(new_columns_results.len());
        for result in new_columns_results {
            new_columns.push(result?);
        }

        DataFrame::new(new_columns)
    }

    /// 内连接操作。
    ///
    /// 只保留两个 DataFrame 中连接键都存在的行。
    ///
    /// # 参数
    /// 
    /// * `right` - 右侧 DataFrame
    /// * `left_on` - 左侧连接键列名
    /// * `right_on` - 右侧连接键列名
    ///
    /// # 返回值
    /// 
    /// 返回连接后的新 DataFrame
    pub fn inner_join(
        &self,
        right: &DataFrame,
        left_on: &str,
        right_on: &str,
    ) -> AxionResult<DataFrame> {
        let left_key_col: &Series<String> = self.downcast_column(left_on).map_err(|e| match e {
            AxionError::ColumnNotFound(_) => AxionError::ColumnNotFound(format!("左侧连接键列 '{}'", left_on)),
            AxionError::TypeMismatch { expected: _, found, name } => AxionError::JoinKeyTypeError {
                side: "左侧".to_string(),
                name,
                expected: DataType::String,
                found,
            },
            other => other,
        })?;
        let right_key_col: &Series<String> = right.downcast_column(right_on).map_err(|e| match e {
            AxionError::ColumnNotFound(_) => AxionError::ColumnNotFound(format!("右侧连接键列 '{}'", right_on)),
            AxionError::TypeMismatch { expected: _, found, name } => AxionError::JoinKeyTypeError {
                side: "右侧".to_string(),
                name,
                expected: DataType::String,
                found,
            },
            other => other,
        })?;

        let mut right_indices_map: HashMap<&Option<String>, Vec<usize>> = HashMap::new();
        for (idx, opt_key) in right_key_col.data_internal().iter().enumerate() {
            right_indices_map.entry(opt_key).or_default().push(idx);
        }

        let mut join_indices: Vec<(usize, usize)> = Vec::new();
        for (left_idx, left_opt_key) in left_key_col.data_internal().iter().enumerate() {
            if let Some(right_indices) = right_indices_map.get(left_opt_key) {
                for &right_idx in right_indices {
                    join_indices.push((left_idx, right_idx));
                }
            }
        }

        let (left_result_indices, right_result_indices): (Vec<usize>, Vec<usize>) =
            join_indices.into_iter().unzip();

        let mut result_columns: Vec<Box<dyn SeriesTrait>> =
            Vec::with_capacity(self.width() + right.width() - 1);
        let mut left_column_names: HashSet<String> = HashSet::with_capacity(self.width());

        for col in &self.columns {
            let taken_left_col = col.take_indices(&left_result_indices)?;
            left_column_names.insert(taken_left_col.name().to_string());
            result_columns.push(taken_left_col);
        }

        for col in &right.columns {
            if col.name() != right_on {
                let original_right_name = col.name();
                let mut taken_right_col = col.take_indices(&right_result_indices)?;

                if left_column_names.contains(original_right_name) {
                    let new_name = format!("{}_right", original_right_name);
                    taken_right_col.rename(&new_name);
                    result_columns.push(taken_right_col);
                } else {
                    result_columns.push(taken_right_col);
                }
            }
        }

        DataFrame::new(result_columns)
    }

    /// 左连接操作。
    ///
    /// 保留左侧 DataFrame 的所有行，如果右侧没有匹配则填充空值。
    pub fn left_join(
        &self,
        right: &DataFrame,
        left_on: &str,
        right_on: &str,
    ) -> AxionResult<DataFrame> {
        let left_key_col: &Series<String> = self
            .downcast_column(left_on)
            .map_err(|e| match e {
                AxionError::ColumnNotFound(_) => AxionError::ColumnNotFound(format!("left key column '{}'", left_on)),
                AxionError::TypeMismatch { expected: _, found, name } => AxionError::JoinKeyTypeError {
                    side: "left".to_string(), name, expected: DataType::String, found,
                },
                other => other,
            })?;

        let right_key_col: &Series<String> = right
            .downcast_column(right_on)
            .map_err(|e| match e {
                AxionError::ColumnNotFound(_) => AxionError::ColumnNotFound(format!("right key column '{}'", right_on)),
                AxionError::TypeMismatch { expected: _, found, name } => AxionError::JoinKeyTypeError {
                    side: "right".to_string(), name, expected: DataType::String, found,
                },
                other => other,
            })?;

        let mut right_indices_map: HashMap<&Option<String>, Vec<usize>> = HashMap::new();
        for (idx, opt_key) in right_key_col.data_internal().iter().enumerate() {
            right_indices_map.entry(opt_key).or_default().push(idx);
        }

        let mut join_indices: Vec<(usize, Option<usize>)> = Vec::new();
        for (left_idx, left_opt_key) in left_key_col.data_internal().iter().enumerate() {
            if let Some(right_indices) = right_indices_map.get(left_opt_key) {
                for &right_idx in right_indices {
                    join_indices.push((left_idx, Some(right_idx)));
                }
            } else {
                join_indices.push((left_idx, None));
            }
        }

        let (left_result_indices, right_result_indices): (Vec<usize>, Vec<Option<usize>>) =
            join_indices.into_iter().unzip();

        let mut result_columns: Vec<Box<dyn SeriesTrait>> =
            Vec::with_capacity(self.width() + right.width() - 1);
        let mut left_column_names: HashSet<String> = HashSet::with_capacity(self.width());

        for col in &self.columns {
            let taken_left_col = col.take_indices(&left_result_indices)?;
            left_column_names.insert(taken_left_col.name().to_string());
            result_columns.push(taken_left_col);
        }

        for col in &right.columns {
            if col.name() != right_on {
                let original_right_name = col.name();
                let mut taken_right_col = col.take_indices_option(&right_result_indices)?;

                if left_column_names.contains(original_right_name) {
                    let new_name = format!("{}_right", original_right_name);
                    taken_right_col.rename(&new_name);
                    result_columns.push(taken_right_col);
                } else {
                    result_columns.push(taken_right_col);
                }
            }
        }

        DataFrame::new(result_columns)
    }

    /// 右连接操作。
    ///
    /// 保留右侧 DataFrame 的所有行，如果左侧没有匹配则填充空值。
    pub fn right_join(
        &self,
        right: &DataFrame,
        left_on: &str,
        right_on: &str,
    ) -> AxionResult<DataFrame> {
        let left_key_col: &Series<String> = self
            .downcast_column(left_on)
            .map_err(|e| match e {
                AxionError::ColumnNotFound(_) => AxionError::ColumnNotFound(format!("left key column '{}'", left_on)),
                AxionError::TypeMismatch { expected: _, found, name } => AxionError::JoinKeyTypeError {
                    side: "left".to_string(), name, expected: DataType::String, found,
                },
                other => other,
            })?;

        let right_key_col: &Series<String> = right
            .downcast_column(right_on)
            .map_err(|e| match e {
                AxionError::ColumnNotFound(_) => AxionError::ColumnNotFound(format!("right key column '{}'", right_on)),
                AxionError::TypeMismatch { expected: _, found, name } => AxionError::JoinKeyTypeError {
                    side: "right".to_string(), name, expected: DataType::String, found,
                },
                other => other,
            })?;

        let mut left_indices_map: HashMap<&Option<String>, Vec<usize>> = HashMap::new();
        for (idx, opt_key) in left_key_col.data_internal().iter().enumerate() {
            left_indices_map.entry(opt_key).or_default().push(idx);
        }

        let mut join_indices: Vec<(Option<usize>, usize)> = Vec::new();
        for (right_idx, right_opt_key) in right_key_col.data_internal().iter().enumerate() {
            if let Some(left_indices) = left_indices_map.get(right_opt_key) {
                for &left_idx in left_indices {
                    join_indices.push((Some(left_idx), right_idx));
                }
            } else {
                join_indices.push((None, right_idx));
            }
        }

        let (left_result_indices, right_result_indices): (Vec<Option<usize>>, Vec<usize>) =
            join_indices.into_iter().unzip();

        let mut result_columns: Vec<Box<dyn SeriesTrait>> =
            Vec::with_capacity(self.width() + right.width() - 1);
        let mut right_column_names: HashSet<String> = HashSet::with_capacity(right.width());

        for col in &right.columns {
            let taken_right_col = col.take_indices(&right_result_indices)?;
            right_column_names.insert(taken_right_col.name().to_string());
            result_columns.push(taken_right_col);
        }

        for col in &self.columns {
            if col.name() != left_on {
                let original_left_name = col.name();
                let mut taken_left_col = col.take_indices_option(&left_result_indices)?;

                if right_column_names.contains(original_left_name) {
                    let new_name = format!("{}_left", original_left_name);
                    taken_left_col.rename(&new_name);
                    result_columns.push(taken_left_col);
                } else {
                    result_columns.push(taken_left_col);
                }
            }
        }

        DataFrame::new(result_columns)
    }

    /// 外连接操作。
    ///
    /// 保留两个 DataFrame 的所有行，没有匹配的地方填充空值。
    pub fn outer_join(
        &self,
        right: &DataFrame,
        left_on: &str,
        right_on: &str,
    ) -> AxionResult<DataFrame> {
        let left_key_col: &Series<String> = self
            .downcast_column(left_on)
            .map_err(|e| match e {
                AxionError::ColumnNotFound(_) => AxionError::ColumnNotFound(format!("left key column '{}'", left_on)),
                AxionError::TypeMismatch { expected: _, found, name } => AxionError::JoinKeyTypeError {
                    side: "left".to_string(),
                    name,
                    expected: DataType::String,
                    found,
                },
                other => other,
            })?;
        let right_key_col: &Series<String> = right
            .downcast_column(right_on)
            .map_err(|e| match e {
                AxionError::ColumnNotFound(_) => AxionError::ColumnNotFound(format!("right key column '{}'", right_on)),
                AxionError::TypeMismatch { expected: _, found, name } => AxionError::JoinKeyTypeError {
                    side: "right".to_string(),
                    name,
                    expected: DataType::String,
                    found,
                },
                other => other,
            })?;

        let mut right_indices_map: HashMap<&Option<String>, Vec<usize>> = HashMap::new();
        for (idx, opt_key) in right_key_col.data_internal().iter().enumerate() {
            right_indices_map.entry(opt_key).or_default().push(idx);
        }

        let mut join_indices: Vec<(Option<usize>, Option<usize>)> = Vec::new();
        let mut used_right_indices: HashSet<usize> = HashSet::new();

        for (left_idx, left_opt_key) in left_key_col.data_internal().iter().enumerate() {
            if let Some(right_indices) = right_indices_map.get(left_opt_key) {
                for &right_idx in right_indices {
                    join_indices.push((Some(left_idx), Some(right_idx)));
                    used_right_indices.insert(right_idx);
                }
            } else {
                join_indices.push((Some(left_idx), None));
            }
        }

        for (right_idx, _right_opt_key) in right_key_col.data_internal().iter().enumerate() {
            if !used_right_indices.contains(&right_idx) {
                join_indices.push((None, Some(right_idx)));
            }
        }

        let (left_result_indices, right_result_indices): (Vec<Option<usize>>, Vec<Option<usize>>) =
            join_indices.into_iter().unzip();

        let mut result_columns: Vec<Box<dyn SeriesTrait>> =
            Vec::with_capacity(self.width() + right.width() - 1);
        let mut left_column_names: HashSet<String> = HashSet::with_capacity(self.width());

        for col in &self.columns {
            let taken_left_col = col.take_indices_option(&left_result_indices)?;
            left_column_names.insert(taken_left_col.name().to_string());
            result_columns.push(taken_left_col);
        }

        for col in &right.columns {
            if col.name() != right_on {
                let original_right_name = col.name();
                let mut taken_right_col = col.take_indices_option(&right_result_indices)?;

                if left_column_names.contains(original_right_name) {
                    let new_name = format!("{}_right", original_right_name);
                    taken_right_col.rename(&new_name);
                    result_columns.push(taken_right_col);
                } else {
                    result_columns.push(taken_right_col);
                }
            }
        }

        DataFrame::new(result_columns)
    }

    /// 创建分组操作对象。
    ///
    /// # 参数
    /// 
    /// * `keys` - 用于分组的列名数组
    ///
    /// # 返回值
    /// 
    /// 返回 GroupBy 对象，可用于执行聚合操作
    ///
    /// # 示例
    /// 
    /// ```rust
    /// let grouped = df.groupby(&["类别"])?;
    /// let result = grouped.sum()?;
    /// ```
    pub fn groupby<'a>(&'a self, keys: &[&str]) -> AxionResult<GroupBy<'a>> {
        let key_strings: Vec<String> = keys.iter().map(|s| s.to_string()).collect();
        GroupBy::new(self, key_strings)
    }

    /// 对 DataFrame 进行排序。
    ///
    /// # 参数
    /// 
    /// * `by` - 用于排序的列名数组
    /// * `descending` - 对应每列的排序方向，true 为降序，false 为升序
    ///
    /// # 返回值
    /// 
    /// 返回排序后的新 DataFrame
    ///
    /// # 错误
    /// 
    /// * `AxionError::InvalidArgument` - 列名数组和排序方向数组长度不匹配
    /// * `AxionError::UnsupportedOperation` - 尝试对不支持排序的数据类型进行排序
    ///
    /// # 示例
    /// 
    /// ```rust
    /// // 按年龄升序，姓名降序排序
    /// let sorted_df = df.sort(&["年龄", "姓名"], &[false, true])?;
    /// ```
    pub fn sort(&self, by: &[&str], descending: &[bool]) -> AxionResult<DataFrame> {
        if by.is_empty() {
            return Ok(self.clone());
        }
        if by.len() != descending.len() {
            return Err(AxionError::InvalidArgument(
                "排序键数量和降序标志数量必须匹配".to_string(),
            ));
        }

        let mut sort_key_columns: Vec<&dyn SeriesTrait> = Vec::with_capacity(by.len());
        for key_name in by {
            let col = self.column(key_name)?;
            if let DataType::List(_) = col.dtype() {
                return Err(AxionError::UnsupportedOperation(format!(
                    "列 '{}' 的 List 类型不支持排序", key_name
                )));
            }
            sort_key_columns.push(col);
        }

        let height = self.height();
        let mut indices: Vec<usize> = (0..height).collect();

        indices.sort_unstable_by(|&a_idx, &b_idx| {
            for (i, key_col) in sort_key_columns.iter().enumerate() {
                let order = key_col.compare_row(a_idx, b_idx);
                let current_order = if descending[i] { order.reverse() } else { order };

                if current_order != Ordering::Equal {
                    return current_order;
                }
            }
            Ordering::Equal
        });

        let mut sorted_columns: Vec<Box<dyn SeriesTrait>> = Vec::with_capacity(self.columns.len());
        for col in &self.columns {
            let sorted_col = col.take_indices(&indices)?;
            sorted_columns.push(sorted_col);
        }

        DataFrame::new(sorted_columns)
    }

    /// 将 DataFrame 导出为 CSV 文件。
    ///
    /// # 参数
    /// 
    /// * `filepath` - 输出文件路径
    /// * `options` - 可选的 CSV 写入配置
    ///
    /// # 错误
    /// 
    /// * `AxionError::IoError` - 文件创建或写入失败
    ///
    /// # 示例
    /// 
    /// ```rust
    /// use axion::io::csv::WriteCsvOptions;
    /// 
    /// // 使用默认配置导出
    /// df.to_csv("output.csv", None)?;
    /// 
    /// // 使用自定义配置导出
    /// let options = WriteCsvOptions {
    ///     has_header: true,
    ///     delimiter: b';',
    ///     ..Default::default()
    /// };
    /// df.to_csv("output.csv", Some(options))?;
    /// ```
    pub fn to_csv(&self, filepath: impl AsRef<Path>, options: Option<WriteCsvOptions>) -> AxionResult<()> {
        let path_ref = filepath.as_ref();
        let mut file_writer = File::create(path_ref)
            .map_err(|e| AxionError::IoError(format!("无法创建或打开文件 {:?}: {}", path_ref, e)))?;
        self.to_csv_writer(&mut file_writer, options)
    }

    /// 将 DataFrame 写入到实现了 Write trait 的写入器中。
    ///
    /// 这是核心的 CSV 写入逻辑。
    ///
    /// # 参数
    /// 
    /// * `writer` - 实现了 `std::io::Write` 的写入器
    /// * `options` - 可选的 CSV 写入配置
    pub fn to_csv_writer<W: Write>(&self, writer: &mut W, options: Option<WriteCsvOptions>) -> AxionResult<()> {
        let opts = options.unwrap_or_default();

        let mut csv_builder = csv::WriterBuilder::new();
        csv_builder.delimiter(opts.delimiter);

        csv_builder.quote_style(match opts.quote_style {
            crate::io::csv::QuoteStyle::Always => csv::QuoteStyle::Always,
            crate::io::csv::QuoteStyle::Necessary => csv::QuoteStyle::Necessary,
            crate::io::csv::QuoteStyle::Never => csv::QuoteStyle::Never,
            crate::io::csv::QuoteStyle::NonNumeric => csv::QuoteStyle::NonNumeric,
        });

        if opts.line_terminator == "\r\n" {
            csv_builder.terminator(csv::Terminator::CRLF);
        } else if opts.line_terminator == "\n" {
            csv_builder.terminator(csv::Terminator::Any(b'\n'));
        } else if opts.line_terminator.len() == 1 {
            csv_builder.terminator(csv::Terminator::Any(opts.line_terminator.as_bytes()[0]));
        } else {
            return Err(AxionError::CsvError(format!(
                "不支持的行终止符: {:?}",
                opts.line_terminator
            )));
        }

        let mut csv_writer = csv_builder.from_writer(writer);

        if opts.has_header && self.width() > 0 {
            if let Err(e) = csv_writer.write_record(self.columns_names()) {
                 return Err(AxionError::from(e));
            }
        }

        if self.width() > 0 {
            let mut record_buffer: Vec<String> = Vec::with_capacity(self.width());
            for row_idx in 0..self.height() {
                record_buffer.clear();
                for col_idx in 0..self.width() {
                    let series = self.column_at(col_idx)?;
                     let value_to_write: String;

                     if series.is_null_at(row_idx) {
                         value_to_write = opts.na_rep.clone();
                     } else {
                         match series.get_str(row_idx) {
                                Some(s_val) => {
                                    value_to_write = s_val;
                                }
                                None => {
                                    return Err(AxionError::InternalError(format!(
                                        "无法获取位置 ({}, {}) 的字符串表示，列名: '{}'",
                                        row_idx, col_idx, series.name()
                                    )));
                                }
                            }
                        }
                     record_buffer.push(value_to_write);
                }
                if let Err(e) = csv_writer.write_record(&record_buffer) {
                    return Err(AxionError::from(e));
                }
            }
        }

        if let Err(e) = csv_writer.flush() {
            return Err(AxionError::from(e));
        }

        Ok(())
    }
}

impl PartialEq for DataFrame {
    fn eq(&self, other: &Self) -> bool {
        if self.shape() != other.shape() {
            return false;
        }

        if self.columns_names() != other.columns_names() {
            return false;
        }

        for col_name in self.columns_names() {
            let self_col = self.column(col_name).unwrap();
            let other_col = other.column(col_name).unwrap();

            if format!("{:?}", self_col) != format!("{:?}", other_col) {
                return false;
            }
        }

        true
    }
}

impl Debug for DataFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DataFrame")
            .field("height", &self.height)
            .field("columns_count", &self.columns.len())
            .field("schema", &self.schema)
            .finish()
    }
}

impl fmt::Display for DataFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            return writeln!(f, "DataFrame (0x0)");
        }

        const MAX_ROWS_TO_PRINT: usize = 10;
        const MIN_COL_WIDTH: usize = 5;
        const NULL_STR: &str = "null";

        let height = self.height();
        let width = self.width();
        let num_rows_to_print = std::cmp::min(height, MAX_ROWS_TO_PRINT);

        let col_names = self.columns_names();
        let dtypes: Vec<String> = self.dtypes().iter().map(|dt| format!("{:?}", dt)).collect();

        let mut col_widths: Vec<usize> = Vec::with_capacity(width);
        for i in 0..width {
            let name_len = col_names[i].len();
            let type_len = dtypes[i].len();
            let mut max_data_len = MIN_COL_WIDTH;

            for row_idx in 0..num_rows_to_print {
                if let Some(val_str) = self.columns[i].get_str(row_idx) {
                    max_data_len = std::cmp::max(max_data_len, val_str.len());
                } else {
                    max_data_len = std::cmp::max(max_data_len, NULL_STR.len());
                }
            }
            col_widths.push(std::cmp::max(MIN_COL_WIDTH, std::cmp::max(name_len, std::cmp::max(type_len, max_data_len))));
        }

        write!(f, "+")?;
        for w in &col_widths { write!(f, "{:-<width$}+", "", width = w + 2)?; }
        writeln!(f)?;

        write!(f, "|")?;
        for (i, name) in col_names.iter().enumerate() {
            write!(f, " {:<width$} |", name, width = col_widths[i])?;
        }
        writeln!(f)?;

        write!(f, "|")?;
        for w in &col_widths { write!(f, "{:-<width$}|", "", width = w + 2)?; }
        writeln!(f)?;

        write!(f, "|")?;
        for (i, dtype_str) in dtypes.iter().enumerate() {
            write!(f, " {:<width$} |", dtype_str, width = col_widths[i])?;
        }
        writeln!(f)?;

        write!(f, "+")?;
        for w in &col_widths { write!(f, "{:=<width$}+", "", width = w + 2)?; }
        writeln!(f)?;

        for row_idx in 0..num_rows_to_print {
            write!(f, "|")?;
            for (col_idx, col) in self.columns.iter().enumerate() {
                let val_str = col.get_str(row_idx).unwrap_or_else(|| NULL_STR.to_string());
                write!(f, " {:<width$} |", val_str, width = col_widths[col_idx])?;
            }
            writeln!(f)?;
            write!(f, "+")?;
            for w in &col_widths { write!(f, "{:-<width$}+", "", width = w + 2)?; }
            writeln!(f)?;
        }

        if height > num_rows_to_print {
            writeln!(f, "... (还有 {} 行)", height - num_rows_to_print)?;
        }

        Ok(())
    }
}

// #[macro_export]
// macro_rules! df {
//     ( $( $col_name:literal : $col_type:ty => $col_data:expr ),* $(,)? ) => {
//         {
//             let mut columns: Vec<Box<dyn $crate::series::SeriesTrait>> = Vec::new();
//             $(
//                 let series = $crate::series::Series::<$col_type>::new($col_name.into(), $col_data);
//                 columns.push(Box::new(series));
//             )*
//             $crate::dataframe::DataFrame::new(columns)
//         }
//     };

//     ( $( $col_name:literal => $col_data:expr ),* $(,)? ) => {
//         {
//             use $crate::series::IntoSeriesBox;
//             let mut columns: Vec<Box<dyn $crate::series::SeriesTrait>> = Vec::new();
//             $(
//                 let boxed_series = ($col_data).into_series_box($col_name.into());
//                 columns.push(boxed_series);
//             )*
//             $crate::dataframe::DataFrame::new(columns)
//         }
//     };
// }

