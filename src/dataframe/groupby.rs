use crate::dataframe::DataFrame;
use crate::error::{AxionError, AxionResult};
use crate::series::{SeriesTrait, Series};
use crate::dtype::{DataType, DataTypeTrait};
use std::collections::HashMap;
use std::any::Any;
use num_traits::Float;
use std::fmt::Debug;

use super::types::{GroupKeyValue, GroupKey};

/// 根据数据类型创建空的 Series
fn create_empty_series_from_dtype(name: String, dtype: DataType) -> AxionResult<Box<dyn SeriesTrait>> {
    match dtype {
        DataType::Int8 => Ok(Box::new(Series::<i8>::new_empty(name, dtype))),
        DataType::Int16 => Ok(Box::new(Series::<i16>::new_empty(name, dtype))),
        DataType::Int32 => Ok(Box::new(Series::<i32>::new_empty(name, dtype))),
        DataType::Int64 => Ok(Box::new(Series::<i64>::new_empty(name, dtype))),
        DataType::UInt8 => Ok(Box::new(Series::<u8>::new_empty(name, dtype))),
        DataType::UInt16 => Ok(Box::new(Series::<u16>::new_empty(name, dtype))),
        DataType::UInt32 => Ok(Box::new(Series::<u32>::new_empty(name, dtype))),
        DataType::UInt64 => Ok(Box::new(Series::<u64>::new_empty(name, dtype))),
        DataType::Float32 => Ok(Box::new(Series::<f32>::new_empty(name, dtype))),
        DataType::Float64 => Ok(Box::new(Series::<f64>::new_empty(name, dtype))),
        DataType::String => Ok(Box::new(Series::<String>::new_empty(name, dtype))),
        DataType::Bool => Ok(Box::new(Series::<bool>::new_empty(name, dtype))),
        _ => Err(AxionError::UnsupportedOperation(format!("无法为数据类型 {:?} 创建空 Series", dtype))),
    }
}

/// 聚合值枚举，用于表示分组聚合操作的结果
#[derive(Debug, Clone, PartialEq)]
enum AggValue {
    Int8(Option<i8>), 
    Int16(Option<i16>), 
    Int32(Option<i32>), 
    Int64(Option<i64>),
    UInt8(Option<u8>), 
    UInt16(Option<u16>), 
    UInt32(Option<u32>), 
    UInt64(Option<u64>),
    Float32(Option<f32>), 
    Float64(Option<f64>),
    String(Option<String>), 
    Bool(Option<bool>),
    None, // 表示组内全为 null 或类型不匹配
}

impl AggValue {
    /// 从 Option<T> 创建 AggValue
    fn from_option<T: 'static + Clone + Debug>(opt_val: Option<T>) -> Self {
        match opt_val {
            Some(val) => {
                let any_val = &val as &dyn Any;
                if let Some(v) = any_val.downcast_ref::<i8>() { AggValue::Int8(Some(*v)) }
                else if let Some(v) = any_val.downcast_ref::<i16>() { AggValue::Int16(Some(*v)) }
                else if let Some(v) = any_val.downcast_ref::<i32>() { AggValue::Int32(Some(*v)) }
                else if let Some(v) = any_val.downcast_ref::<i64>() { AggValue::Int64(Some(*v)) }
                else if let Some(v) = any_val.downcast_ref::<u8>() { AggValue::UInt8(Some(*v)) }
                else if let Some(v) = any_val.downcast_ref::<u16>() { AggValue::UInt16(Some(*v)) }
                else if let Some(v) = any_val.downcast_ref::<u32>() { AggValue::UInt32(Some(*v)) }
                else if let Some(v) = any_val.downcast_ref::<u64>() { AggValue::UInt64(Some(*v)) }
                else if let Some(v) = any_val.downcast_ref::<f32>() { AggValue::Float32(Some(*v)) }
                else if let Some(v) = any_val.downcast_ref::<f64>() { AggValue::Float64(Some(*v)) }
                else if let Some(v) = any_val.downcast_ref::<String>() { AggValue::String(Some(v.clone())) }
                else if let Some(v) = any_val.downcast_ref::<bool>() { AggValue::Bool(Some(*v)) }
                else {
                    eprintln!("警告: AggValue::from_option 遇到了未预期的类型: {:?}", std::any::type_name::<T>());
                    AggValue::None
                }
            }
            None => AggValue::None,
        }
    }
}

/// 计算最小值/最大值的泛型函数
fn calculate_min_max<T>(
    series_trait: &dyn SeriesTrait,
    indices: &[usize],
    find_min: bool,
) -> AxionResult<AggValue>
where
    T: DataTypeTrait + PartialOrd + Clone + Debug + 'static,
{
    let series = series_trait.as_any().downcast_ref::<Series<T>>()
        .ok_or_else(|| AxionError::InternalError(format!("无法将 Series 向下转型为预期类型 {:?}", std::any::type_name::<T>())))?;
    
    let mut current_agg: Option<T> = None;
    for &idx in indices {
        if let Some(val_ref) = series.get(idx) {
             match current_agg.as_ref() {
                 Some(agg_val_ref) => {
                     if find_min { 
                         if val_ref < agg_val_ref { 
                             current_agg = Some(val_ref.clone()); 
                         } 
                     } else if val_ref > agg_val_ref { 
                         current_agg = Some(val_ref.clone()); 
                     }
                 }
                 None => { 
                     current_agg = Some(val_ref.clone()); 
                 }
             }
        }
    }
    Ok(AggValue::from_option(current_agg))
}

/// 计算浮点数最小值/最大值的泛型函数（处理 NaN）
fn calculate_min_max_float<T>(
    series_trait: &dyn SeriesTrait,
    indices: &[usize],
    find_min: bool,
) -> AxionResult<AggValue>
where
    T: DataTypeTrait + Float + Clone + Debug + 'static,
{
     let series = series_trait.as_any().downcast_ref::<Series<T>>()
        .ok_or_else(|| AxionError::InternalError(format!("无法将 Series 向下转型为预期浮点类型 {:?}", std::any::type_name::<T>())))?;
    
    let mut current_agg: Option<T> = None;
    for &idx in indices {
        if let Some(val_ref) = series.get(idx) {
            if val_ref.is_nan() { 
                continue; 
            }
            match current_agg.as_ref() {
                 Some(agg_val_ref) => {
                     if find_min { 
                         if val_ref < agg_val_ref { 
                             current_agg = Some(*val_ref); 
                         } 
                     } else if val_ref > agg_val_ref { 
                         current_agg = Some(*val_ref); 
                     }
                 }
                 None => { 
                     current_agg = Some(*val_ref); 
                 }
             }
        }
    }
    Ok(AggValue::from_option(current_agg))
}

/// 根据数据类型分发最小值/最大值计算的宏
macro_rules! dispatch_min_max {
    ($series_trait:expr, $dtype:expr, $indices:expr, $find_min:expr) => {
        match $dtype {
            DataType::Int8 => calculate_min_max::<i8>($series_trait, $indices, $find_min),
            DataType::Int16 => calculate_min_max::<i16>($series_trait, $indices, $find_min),
            DataType::Int32 => calculate_min_max::<i32>($series_trait, $indices, $find_min),
            DataType::Int64 => calculate_min_max::<i64>($series_trait, $indices, $find_min),
            DataType::UInt8 => calculate_min_max::<u8>($series_trait, $indices, $find_min),
            DataType::UInt16 => calculate_min_max::<u16>($series_trait, $indices, $find_min),
            DataType::UInt32 => calculate_min_max::<u32>($series_trait, $indices, $find_min),
            DataType::UInt64 => calculate_min_max::<u64>($series_trait, $indices, $find_min),
            DataType::Float32 => calculate_min_max_float::<f32>($series_trait, $indices, $find_min),
            DataType::Float64 => calculate_min_max_float::<f64>($series_trait, $indices, $find_min),
            DataType::String => calculate_min_max::<String>($series_trait, $indices, $find_min),
            DataType::Bool => calculate_min_max::<bool>($series_trait, $indices, $find_min),
            _ => Err(AxionError::UnsupportedOperation(format!("数据类型 {:?} 不支持 Min/Max 操作", $dtype))),
        }
    };
}

/// 表示分组操作的中间状态。
/// 
/// 持有对原始 DataFrame 的引用和计算出的分组索引。
/// 可以在此基础上执行各种聚合操作，如计数、求和、平均值等。
/// 
/// # 示例
/// 
/// ```rust
/// let grouped = df.groupby(&["类别"])?;
/// let count_result = grouped.count()?;
/// let sum_result = grouped.sum()?;
/// let mean_result = grouped.mean()?;
/// ```
#[derive(Debug)]
pub struct GroupBy<'a> {
    /// 原始 DataFrame 的引用
    df: &'a DataFrame,
    /// 用于分组的列名
    keys: Vec<String>,
    /// 从分组键值到行索引的映射
    groups: HashMap<GroupKey, Vec<usize>>,
}

impl<'a> GroupBy<'a> {
    /// 创建新的 GroupBy 对象（内部使用，由 DataFrame::groupby 调用）
    /// 
    /// 根据提供的键计算分组成员关系。
    ///
    /// # 参数
    /// 
    /// * `df` - 要分组的 DataFrame 引用
    /// * `keys` - 用于分组的列名向量
    ///
    /// # 返回值
    /// 
    /// 返回新创建的 GroupBy 对象
    ///
    /// # 错误
    /// 
    /// * `AxionError::ColumnNotFound` - 指定的分组列不存在
    /// * `AxionError::UnsupportedOperation` - 列的数据类型不支持分组
    pub(crate) fn new(df: &'a DataFrame, keys: Vec<String>) -> AxionResult<Self> {
        let mut key_cols: Vec<&dyn SeriesTrait> = Vec::with_capacity(keys.len());
        for key_name in &keys {
            let col = df.column(key_name)?;
            key_cols.push(col);
            match col.dtype() {
                DataType::Int32 | DataType::String | DataType::Bool => {},
                unsupported_dtype => {
                    return Err(AxionError::UnsupportedOperation(format!(
                        "列 '{}' 的数据类型 {:?} 不支持分组操作",
                        key_name, unsupported_dtype
                    )));
                }
            }
        }

        let mut groups: HashMap<GroupKey, Vec<usize>> = HashMap::new();
        for row_idx in 0..df.height() {
            let mut current_key: GroupKey = Vec::with_capacity(keys.len());
            let mut has_null = false;

            for key_col in &key_cols {
                let key_value = match key_col.dtype() {
                    DataType::Int32 => {
                        let series = key_col.as_any().downcast_ref::<Series<i32>>().unwrap();
                        match series.get(row_idx) {
                            Some(v) => GroupKeyValue::Int(*v),
                            None => { has_null = true; break; }
                        }
                    }
                    DataType::String => {
                        let series = key_col.as_any().downcast_ref::<Series<String>>().unwrap();
                        match series.get(row_idx) {
                            Some(v) => GroupKeyValue::Str(v.clone()),
                            None => { has_null = true; break; }
                        }
                    }
                    DataType::Bool => {
                        let series = key_col.as_any().downcast_ref::<Series<bool>>().unwrap();
                        match series.get(row_idx) {
                            Some(v) => GroupKeyValue::Bool(*v),
                            None => { has_null = true; break; }
                        }
                    }
                    _ => unreachable!("类型检查后遇到不支持的分组类型"),
                };
                current_key.push(key_value);
            }

            if !has_null {
                groups.entry(current_key).or_default().push(row_idx);
            }
        }

        Ok(Self { df, keys, groups })
    }

    /// 计算每个组的行数。
    ///
    /// # 返回值
    /// 
    /// 返回包含分组键和对应计数的新 DataFrame
    ///
    /// # 示例
    /// 
    /// ```rust
    /// let grouped = df.groupby(&["类别"])?;
    /// let count_df = grouped.count()?;
    /// ```
    pub fn count(&self) -> AxionResult<DataFrame> {
        let groups = &self.groups;

        if groups.is_empty() {
            let mut output_columns: Vec<Box<dyn SeriesTrait>> = Vec::with_capacity(self.keys.len() + 1);
            for key_name in &self.keys {
                let original_key_col = self.df.column(key_name)?;
                let dtype = original_key_col.dtype();
                let empty_key_series = create_empty_series_from_dtype(key_name.clone(), dtype)?;
                output_columns.push(empty_key_series);
            }
            let empty_count_series = Series::<u32>::new_empty("count".into(), DataType::UInt32);
            output_columns.push(Box::new(empty_count_series));
            return DataFrame::new(output_columns);
        }

        let mut key_data_vecs: Vec<Box<dyn std::any::Any>> = Vec::with_capacity(self.keys.len());
        let mut key_dtypes: Vec<DataType> = Vec::with_capacity(self.keys.len());
        for key_name in &self.keys {
             let original_key_col = self.df.column(key_name)?;
             let dtype = original_key_col.dtype();
             key_dtypes.push(dtype.clone());
             match dtype {
                 DataType::Int32 => key_data_vecs.push(Box::new(Vec::<Option<i32>>::new())),
                 DataType::String => key_data_vecs.push(Box::new(Vec::<Option<String>>::new())),
                 DataType::Bool => key_data_vecs.push(Box::new(Vec::<Option<bool>>::new())),
                 DataType::UInt32 => key_data_vecs.push(Box::new(Vec::<Option<u32>>::new())),
                 _ => return Err(AxionError::UnsupportedOperation(format!(
                     "列 '{}' 的数据类型 {:?} 不支持分组操作", key_name, dtype
                 ))),
             }
        }
        let mut count_data_vec = Vec::<u32>::with_capacity(groups.len());

        for (key, indices) in groups.iter() {
            let key_values = key.iter();

            for (i, group_key_value) in key_values.enumerate() {
                match key_dtypes[i] {
                    DataType::Int32 => {
                        if let Some(vec) = key_data_vecs[i].downcast_mut::<Vec<Option<i32>>>() {
                            if let GroupKeyValue::Int(val) = group_key_value {
                                vec.push(Some(*val));
                            } else { vec.push(None); }
                        }
                    }
                    DataType::String => {
                         if let Some(vec) = key_data_vecs[i].downcast_mut::<Vec<Option<String>>>() {
                            if let GroupKeyValue::Str(val) = group_key_value {
                                vec.push(Some(val.clone()));
                            } else { vec.push(None); }
                        }
                    }
                    DataType::Bool => {
                         if let Some(vec) = key_data_vecs[i].downcast_mut::<Vec<Option<bool>>>() {
                            if let GroupKeyValue::Bool(val) = group_key_value {
                                vec.push(Some(*val));
                            } else { vec.push(None); }
                        }
                    }
                     DataType::UInt32 => {
                         if let Some(vec) = key_data_vecs[i].downcast_mut::<Vec<Option<u32>>>() {
                             vec.push(None);
                         }
                    }
                    _ => {}
                }
            }
            count_data_vec.push(indices.len() as u32);
        }

        let mut final_columns: Vec<Box<dyn SeriesTrait>> = Vec::with_capacity(self.keys.len() + 1);
        for (i, key_name) in self.keys.iter().enumerate() {
            let boxed_any = &key_data_vecs[i];

            let final_key_series: Box<dyn SeriesTrait> = match key_dtypes[i] {
                 DataType::Int32 => {
                     let data_vec_ref = boxed_any.downcast_ref::<Vec<Option<i32>>>().unwrap();
                     Box::new(Series::new_from_options(key_name.clone(), data_vec_ref.clone()))
                 }
                 DataType::String => {
                     let data_vec_ref = boxed_any.downcast_ref::<Vec<Option<String>>>().unwrap();
                     Box::new(Series::new_from_options(key_name.clone(), data_vec_ref.clone()))
                 }
                 DataType::Bool => {
                     let data_vec_ref = boxed_any.downcast_ref::<Vec<Option<bool>>>().unwrap();
                     Box::new(Series::new_from_options(key_name.clone(), data_vec_ref.clone()))
                 }
                 DataType::UInt32 => {
                     let data_vec_ref = boxed_any.downcast_ref::<Vec<Option<u32>>>().unwrap();
                     Box::new(Series::new_from_options(key_name.clone(), data_vec_ref.clone()))
                 }
                 _ => unreachable!(),
            };
            final_columns.push(final_key_series);
        }
        final_columns.push(Box::new(Series::new("count".into(), count_data_vec)));

        DataFrame::new(final_columns)
    }

    /// 计算每个组中数值列的和。
    ///
    /// 非数值列（不是分组键的列）将被忽略。
    /// 组内的 null 值在求和时被忽略（空组或全 null 组的和为 0）。
    ///
    /// # 返回值
    /// 
    /// 返回包含分组键和对应求和结果的新 DataFrame
    pub fn sum(&self) -> AxionResult<DataFrame> {
        let groups = &self.groups;

        let value_col_names: Vec<String> = self.df.columns_names()
            .into_iter()
            .filter(|name| !self.keys.iter().any(|k| k == *name))
            .filter(|name| {
                if let Ok(col) = self.df.column(name) {
                    matches!(col.dtype(), DataType::Int32 | DataType::UInt32 | DataType::Float32 | DataType::Float64)
                } else {
                    false
                }
            })
            .map(|name| name.to_string())
            .collect();

        if groups.is_empty() {
            let mut output_columns: Vec<Box<dyn SeriesTrait>> = Vec::with_capacity(self.keys.len() + value_col_names.len());
            for key_name in &self.keys {
                let original_key_col = self.df.column(key_name)?;
                let dtype = original_key_col.dtype();
                let empty_key_series = create_empty_series_from_dtype(key_name.clone(), dtype)?;
                output_columns.push(empty_key_series);
            }
            for value_col_name in &value_col_names {
                let original_value_col = self.df.column(value_col_name)?;
                let dtype = original_value_col.dtype();
                let empty_sum_series = create_empty_series_from_dtype(value_col_name.clone(), dtype)?;
                output_columns.push(empty_sum_series);
            }
            return DataFrame::new(output_columns);
        }

        let mut key_data_vecs: Vec<Box<dyn std::any::Any>> = Vec::with_capacity(self.keys.len());
        let mut key_dtypes: Vec<DataType> = Vec::with_capacity(self.keys.len());
        for key_name in &self.keys {
             let original_key_col = self.df.column(key_name)?;
             let dtype = original_key_col.dtype();
             key_dtypes.push(dtype.clone());
             match dtype {
                 DataType::Int32 => key_data_vecs.push(Box::new(Vec::<Option<i32>>::new())),
                 DataType::String => key_data_vecs.push(Box::new(Vec::<Option<String>>::new())),
                 DataType::Bool => key_data_vecs.push(Box::new(Vec::<Option<bool>>::new())),
                 _ => return Err(AxionError::UnsupportedOperation(format!(
                     "列 '{}' 的数据类型 {:?} 不支持分组操作", key_name, dtype
                 ))),
             }
        }

        let mut sum_data_vecs: Vec<Box<dyn std::any::Any>> = Vec::with_capacity(value_col_names.len());
        let mut sum_dtypes: Vec<DataType> = Vec::with_capacity(value_col_names.len());
        for value_col_name in &value_col_names {
            let original_value_col = self.df.column(value_col_name)?;
            let dtype = original_value_col.dtype();
            sum_dtypes.push(dtype.clone());
            match dtype {
                DataType::Int32 => sum_data_vecs.push(Box::new(Vec::<Option<i32>>::new())),
                DataType::UInt32 => sum_data_vecs.push(Box::new(Vec::<Option<u32>>::new())),
                DataType::Float32 => sum_data_vecs.push(Box::new(Vec::<Option<f32>>::new())),
                DataType::Float64 => sum_data_vecs.push(Box::new(Vec::<Option<f64>>::new())),
                _ => unreachable!(),
            }
        }

        for (key, indices) in groups.iter() {
            let key_values = key.iter();
            for (i, group_key_value) in key_values.enumerate() {
                 match key_dtypes[i] {
                    DataType::Int32 => if let Some(vec) = key_data_vecs[i].downcast_mut::<Vec<Option<i32>>>() { if let GroupKeyValue::Int(val) = group_key_value { vec.push(Some(*val)); } else { vec.push(None); } },
                    DataType::String => if let Some(vec) = key_data_vecs[i].downcast_mut::<Vec<Option<String>>>() { if let GroupKeyValue::Str(val) = group_key_value { vec.push(Some(val.clone())); } else { vec.push(None); } },
                    DataType::Bool => if let Some(vec) = key_data_vecs[i].downcast_mut::<Vec<Option<bool>>>() { if let GroupKeyValue::Bool(val) = group_key_value { vec.push(Some(*val)); } else { vec.push(None); } },
                    _ => {}
                }
            }

            for (j, value_col_name) in value_col_names.iter().enumerate() {
                let value_col = self.df.column(value_col_name)?;

                match sum_dtypes[j] {
                    DataType::Int32 => {
                        let series = value_col.as_any().downcast_ref::<Series<i32>>().unwrap();
                        let mut current_sum: Option<i32> = None;
                        for &idx in indices {
                            if let Some(val) = series.get(idx) {
                                current_sum = Some(current_sum.unwrap_or(0).saturating_add(*val));
                            }
                        }
                        if let Some(vec) = sum_data_vecs[j].downcast_mut::<Vec<Option<i32>>>() {
                            vec.push(current_sum);
                        }
                    }
                    DataType::UInt32 => {
                        let series = value_col.as_any().downcast_ref::<Series<u32>>().unwrap();
                        let mut current_sum: Option<u32> = None;
                        for &idx in indices {
                            if let Some(val) = series.get(idx) {
                                current_sum = Some(current_sum.unwrap_or(0).saturating_add(*val));
                            }
                        }
                        if let Some(vec) = sum_data_vecs[j].downcast_mut::<Vec<Option<u32>>>() {
                            vec.push(current_sum);
                        }
                    }
                    DataType::Float32 => {
                        let series = value_col.as_any().downcast_ref::<Series<f32>>().unwrap();
                        let mut current_sum: Option<f32> = None;
                        for &idx in indices {
                            if let Some(val) = series.get(idx) {
                                if val.is_nan() { continue; }
                                current_sum = Some(current_sum.unwrap_or(0.0) + *val);
                            }
                        }
                        if let Some(vec) = sum_data_vecs[j].downcast_mut::<Vec<Option<f32>>>() {
                            vec.push(current_sum);
                        }
                    }
                    DataType::Float64 => {
                        let series = value_col.as_any().downcast_ref::<Series<f64>>().unwrap();
                        let mut current_sum: Option<f64> = None;
                        for &idx in indices {
                            if let Some(val) = series.get(idx) {
                                if val.is_nan() { continue; }
                                current_sum = Some(current_sum.unwrap_or(0.0) + *val);
                            }
                        }
                        if let Some(vec) = sum_data_vecs[j].downcast_mut::<Vec<Option<f64>>>() {
                            vec.push(current_sum);
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }

        let mut final_columns: Vec<Box<dyn SeriesTrait>> = Vec::with_capacity(self.keys.len() + value_col_names.len());
        for (i, key_name) in self.keys.iter().enumerate() {
            let boxed_any = &key_data_vecs[i];
            let final_key_series: Box<dyn SeriesTrait> = match key_dtypes[i] {
                 DataType::Int32 => Box::new(Series::new_from_options(key_name.clone(), boxed_any.downcast_ref::<Vec<Option<i32>>>().unwrap().clone())),
                 DataType::String => Box::new(Series::new_from_options(key_name.clone(), boxed_any.downcast_ref::<Vec<Option<String>>>().unwrap().clone())),
                 DataType::Bool => Box::new(Series::new_from_options(key_name.clone(), boxed_any.downcast_ref::<Vec<Option<bool>>>().unwrap().clone())),
                 _ => unreachable!(),
            };
            final_columns.push(final_key_series);
        }
        for (j, value_col_name) in value_col_names.iter().enumerate() {
            let boxed_any = &sum_data_vecs[j];
            let final_sum_series: Box<dyn SeriesTrait> = match sum_dtypes[j] {
                 DataType::Int32 => Box::new(Series::new_from_options(value_col_name.clone(), boxed_any.downcast_ref::<Vec<Option<i32>>>().unwrap().clone())),
                 DataType::UInt32 => Box::new(Series::new_from_options(value_col_name.clone(), boxed_any.downcast_ref::<Vec<Option<u32>>>().unwrap().clone())),
                 DataType::Float32 => Box::new(Series::new_from_options(value_col_name.clone(), boxed_any.downcast_ref::<Vec<Option<f32>>>().unwrap().clone())),
                 DataType::Float64 => Box::new(Series::new_from_options(value_col_name.clone(), boxed_any.downcast_ref::<Vec<Option<f64>>>().unwrap().clone())),
                 _ => unreachable!(),
            };
            final_columns.push(final_sum_series);
        }

        DataFrame::new(final_columns)
    }

    /// 计算每个组中数值列的平均值。
    ///
    /// 非数值列（不是分组键的列）将被忽略。
    /// 组内的 null 值在计算时被忽略（空组或全 null 组的平均值为 null）。
    ///
    /// # 返回值
    /// 
    /// 返回包含分组键和对应平均值的新 DataFrame，平均值列的类型为 f64
    pub fn mean(&self) -> AxionResult<DataFrame> {
        let groups = &self.groups;

        let value_col_names: Vec<String> = self.df.columns_names()
            .into_iter()
            .filter(|name| !self.keys.iter().any(|k| k == *name))
            .filter(|name| {
                if let Ok(col) = self.df.column(name) {
                    col.dtype().is_numeric()
                } else {
                    false
                }
            })
            .map(|name| name.to_string())
            .collect();

        if groups.is_empty() {
            let mut output_columns: Vec<Box<dyn SeriesTrait>> = Vec::with_capacity(self.keys.len() + value_col_names.len());
            for key_name in &self.keys {
                let original_key_col = self.df.column(key_name)?;
                let dtype = original_key_col.dtype();
                let empty_key_series = create_empty_series_from_dtype(key_name.clone(), dtype)?;
                output_columns.push(empty_key_series);
            }
            for value_col_name in &value_col_names {
                 let empty_mean_series = Series::<f64>::new_empty(value_col_name.clone(), DataType::Float64);
                 output_columns.push(Box::new(empty_mean_series));
            }
            return DataFrame::new(output_columns);
        }

        let mut key_data_vecs: Vec<Box<dyn std::any::Any>> = Vec::with_capacity(self.keys.len());
        let mut key_dtypes: Vec<DataType> = Vec::with_capacity(self.keys.len());
        for key_name in &self.keys {
             let original_key_col = self.df.column(key_name)?;
             let dtype = original_key_col.dtype();
             key_dtypes.push(dtype.clone());
             match dtype {
                 DataType::Int32 => key_data_vecs.push(Box::new(Vec::<Option<i32>>::new())),
                 DataType::String => key_data_vecs.push(Box::new(Vec::<Option<String>>::new())),
                 DataType::Bool => key_data_vecs.push(Box::new(Vec::<Option<bool>>::new())),
                 _ => return Err(AxionError::UnsupportedOperation(format!(
                     "列 '{}' 的数据类型 {:?} 不支持分组操作", key_name, dtype
                 ))),
             }
        }

        let mut mean_data_vecs: Vec<Box<dyn std::any::Any>> = Vec::with_capacity(value_col_names.len());
        for _ in &value_col_names {
            mean_data_vecs.push(Box::new(Vec::<Option<f64>>::new()));
        }

        for (key, indices) in groups.iter() {
            let key_values = key.iter();
            for (i, group_key_value) in key_values.enumerate() {
                 match key_dtypes[i] {
                    DataType::Int32 => if let Some(vec) = key_data_vecs[i].downcast_mut::<Vec<Option<i32>>>() { if let GroupKeyValue::Int(val) = group_key_value { vec.push(Some(*val)); } else { vec.push(None); } },
                    DataType::String => if let Some(vec) = key_data_vecs[i].downcast_mut::<Vec<Option<String>>>() { if let GroupKeyValue::Str(val) = group_key_value { vec.push(Some(val.clone())); } else { vec.push(None); } },
                    DataType::Bool => if let Some(vec) = key_data_vecs[i].downcast_mut::<Vec<Option<bool>>>() { if let GroupKeyValue::Bool(val) = group_key_value { vec.push(Some(*val)); } else { vec.push(None); } },
                    _ => {}
                }
            }

            for (j, value_col_name) in value_col_names.iter().enumerate() {
                let value_col = self.df.column(value_col_name)?;
                let mut current_sum: f64 = 0.0;
                let mut current_count: u32 = 0;

                for &idx in indices {
                    if let Some(value_f64) = value_col.get_as_f64(idx)? {
                        if !value_f64.is_nan() {
                            current_sum += value_f64;
                            current_count += 1;
                        }
                    }
                }

                let mean_value = if current_count > 0 {
                    Some(current_sum / current_count as f64)
                } else {
                    None
                };

                if let Some(vec) = mean_data_vecs[j].downcast_mut::<Vec<Option<f64>>>() {
                    vec.push(mean_value);
                }
            }
        }

        let mut final_columns: Vec<Box<dyn SeriesTrait>> = Vec::with_capacity(self.keys.len() + value_col_names.len());
        for (i, key_name) in self.keys.iter().enumerate() {
            let boxed_any = &key_data_vecs[i];
            let final_key_series: Box<dyn SeriesTrait> = match key_dtypes[i] {
                 DataType::Int32 => Box::new(Series::new_from_options(key_name.clone(), boxed_any.downcast_ref::<Vec<Option<i32>>>().unwrap().clone())),
                 DataType::String => Box::new(Series::new_from_options(key_name.clone(), boxed_any.downcast_ref::<Vec<Option<String>>>().unwrap().clone())),
                 DataType::Bool => Box::new(Series::new_from_options(key_name.clone(), boxed_any.downcast_ref::<Vec<Option<bool>>>().unwrap().clone())),
                 _ => unreachable!(),
            };
            final_columns.push(final_key_series);
        }
        for (j, value_col_name) in value_col_names.iter().enumerate() {
            let boxed_any = &mean_data_vecs[j];
            let final_mean_series = Box::new(Series::new_from_options(
                value_col_name.clone(),
                boxed_any.downcast_ref::<Vec<Option<f64>>>().unwrap().clone()
            ));
            final_columns.push(final_mean_series);
        }

        DataFrame::new(final_columns)
    }

    /// 计算每个组中可比较列的最小值。
    ///
    /// 非可比较列（如 List）和分组键列将被忽略。
    /// 计算中会忽略 null 值。
    /// 结果列的类型将与原始列相同。
    ///
    /// # 返回值
    /// 
    /// 返回包含分组键和对应最小值的新 DataFrame
    pub fn min(&self) -> AxionResult<DataFrame> {
        self.aggregate_min_max(true)
    }

    /// 计算每个组中可比较列的最大值。
    ///
    /// 非可比较列（如 List）和分组键列将被忽略。
    /// 计算中会忽略 null 值。
    /// 结果列的类型将与原始列相同。
    ///
    /// # 返回值
    /// 
    /// 返回包含分组键和对应最大值的新 DataFrame
    pub fn max(&self) -> AxionResult<DataFrame> {
        self.aggregate_min_max(false)
    }

    /// 内部辅助函数，处理 min 和 max 的通用逻辑
    fn aggregate_min_max(&self, find_min: bool) -> AxionResult<DataFrame> {
        let groups = &self.groups;

        let value_col_names: Vec<String> = self.df.columns_names()
            .into_iter()
            .filter(|name| !self.keys.iter().any(|k| k == *name))
            .filter(|name| {
                if let Ok(col) = self.df.column(name) {
                    matches!(col.dtype(),
                        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
                        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
                        DataType::Float32 | DataType::Float64 |
                        DataType::String |
                        DataType::Bool
                    )
                } else {
                    false
                }
            })
            .map(|name| name.to_string())
            .collect();

        if groups.is_empty() {
            let mut output_columns: Vec<Box<dyn SeriesTrait>> = Vec::with_capacity(self.keys.len() + value_col_names.len());
            for key_name in &self.keys {
                let original_key_col = self.df.column(key_name)?;
                let dtype = original_key_col.dtype();
                let empty_key_series = create_empty_series_from_dtype(key_name.clone(), dtype)?;
                output_columns.push(empty_key_series);
            }
            for value_col_name in &value_col_names {
                let original_value_col = self.df.column(value_col_name)?;
                let dtype = original_value_col.dtype();
                let empty_agg_series = create_empty_series_from_dtype(value_col_name.clone(), dtype)?;
                output_columns.push(empty_agg_series);
            }
            return DataFrame::new(output_columns);
        }

        let mut key_data_vecs: Vec<Box<dyn std::any::Any>> = Vec::with_capacity(self.keys.len());
        let mut key_dtypes: Vec<DataType> = Vec::with_capacity(self.keys.len());
        for key_name in &self.keys {
             let original_key_col = self.df.column(key_name)?;
             let dtype = original_key_col.dtype();
             key_dtypes.push(dtype.clone());
             match dtype {
                 DataType::Int32 => key_data_vecs.push(Box::new(Vec::<Option<i32>>::new())),
                 DataType::String => key_data_vecs.push(Box::new(Vec::<Option<String>>::new())),
                 DataType::Bool => key_data_vecs.push(Box::new(Vec::<Option<bool>>::new())),
                 _ => return Err(AxionError::UnsupportedOperation(format!(
                     "列 '{}' 的数据类型 {:?} 不支持分组操作", key_name, dtype
                 ))),
             }
        }

        let mut agg_data_vecs: Vec<Box<dyn std::any::Any>> = Vec::with_capacity(value_col_names.len());
        let mut agg_dtypes: Vec<DataType> = Vec::with_capacity(value_col_names.len());
        for value_col_name in &value_col_names {
            let original_value_col = self.df.column(value_col_name)?;
            let dtype = original_value_col.dtype();
            agg_dtypes.push(dtype.clone());
            match dtype {
                DataType::Int8 => agg_data_vecs.push(Box::new(Vec::<Option<i8>>::new())),
                DataType::Int16 => agg_data_vecs.push(Box::new(Vec::<Option<i16>>::new())),
                DataType::Int32 => agg_data_vecs.push(Box::new(Vec::<Option<i32>>::new())),
                DataType::Int64 => agg_data_vecs.push(Box::new(Vec::<Option<i64>>::new())),
                DataType::UInt8 => agg_data_vecs.push(Box::new(Vec::<Option<u8>>::new())),
                DataType::UInt16 => agg_data_vecs.push(Box::new(Vec::<Option<u16>>::new())),
                DataType::UInt32 => agg_data_vecs.push(Box::new(Vec::<Option<u32>>::new())),
                DataType::UInt64 => agg_data_vecs.push(Box::new(Vec::<Option<u64>>::new())),
                DataType::Float32 => agg_data_vecs.push(Box::new(Vec::<Option<f32>>::new())),
                DataType::Float64 => agg_data_vecs.push(Box::new(Vec::<Option<f64>>::new())),
                DataType::String => agg_data_vecs.push(Box::new(Vec::<Option<String>>::new())),
                DataType::Bool => agg_data_vecs.push(Box::new(Vec::<Option<bool>>::new())),
                _ => unreachable!("应该只包含之前过滤的可比较类型"),
            }
        }

        for (key, indices) in groups.iter() {
            let key_values = key.iter();
            for (i, group_key_value) in key_values.enumerate() {
                 match key_dtypes[i] {
                    DataType::Int32 => if let Some(vec) = key_data_vecs[i].downcast_mut::<Vec<Option<i32>>>() { if let GroupKeyValue::Int(val) = group_key_value { vec.push(Some(*val)); } else { vec.push(None); } },
                    DataType::String => if let Some(vec) = key_data_vecs[i].downcast_mut::<Vec<Option<String>>>() { if let GroupKeyValue::Str(val) = group_key_value { vec.push(Some(val.clone())); } else { vec.push(None); } },
                    DataType::Bool => if let Some(vec) = key_data_vecs[i].downcast_mut::<Vec<Option<bool>>>() { if let GroupKeyValue::Bool(val) = group_key_value { vec.push(Some(*val)); } else { vec.push(None); } },
                    _ => {}
                }
            }

            for (j, value_col_name) in value_col_names.iter().enumerate() {
                let value_col = self.df.column(value_col_name)?;

                let agg_value = dispatch_min_max!(
                    value_col,
                    &agg_dtypes[j],
                    indices,
                    find_min
                )?;

                let boxed_any = &mut agg_data_vecs[j];
                match agg_dtypes[j] {
                    DataType::Int8 => if let Some(vec) = boxed_any.downcast_mut::<Vec<Option<i8>>>() { if let AggValue::Int8(opt_val) = agg_value { vec.push(opt_val); } else { vec.push(None); } },
                    DataType::Int16 => if let Some(vec) = boxed_any.downcast_mut::<Vec<Option<i16>>>() { if let AggValue::Int16(opt_val) = agg_value { vec.push(opt_val); } else { vec.push(None); } },
                    DataType::Int32 => if let Some(vec) = boxed_any.downcast_mut::<Vec<Option<i32>>>() { if let AggValue::Int32(opt_val) = agg_value { vec.push(opt_val); } else { vec.push(None); } },
                    DataType::Int64 => if let Some(vec) = boxed_any.downcast_mut::<Vec<Option<i64>>>() { if let AggValue::Int64(opt_val) = agg_value { vec.push(opt_val); } else { vec.push(None); } },
                    DataType::UInt8 => if let Some(vec) = boxed_any.downcast_mut::<Vec<Option<u8>>>() { if let AggValue::UInt8(opt_val) = agg_value { vec.push(opt_val); } else { vec.push(None); } },
                    DataType::UInt16 => if let Some(vec) = boxed_any.downcast_mut::<Vec<Option<u16>>>() { if let AggValue::UInt16(opt_val) = agg_value { vec.push(opt_val); } else { vec.push(None); } },
                    DataType::UInt32 => if let Some(vec) = boxed_any.downcast_mut::<Vec<Option<u32>>>() { if let AggValue::UInt32(opt_val) = agg_value { vec.push(opt_val); } else { vec.push(None); } },
                    DataType::UInt64 => if let Some(vec) = boxed_any.downcast_mut::<Vec<Option<u64>>>() { if let AggValue::UInt64(opt_val) = agg_value { vec.push(opt_val); } else { vec.push(None); } },
                    DataType::Float32 => if let Some(vec) = boxed_any.downcast_mut::<Vec<Option<f32>>>() { if let AggValue::Float32(opt_val) = agg_value { vec.push(opt_val); } else { vec.push(None); } },
                    DataType::Float64 => if let Some(vec) = boxed_any.downcast_mut::<Vec<Option<f64>>>() { if let AggValue::Float64(opt_val) = agg_value { vec.push(opt_val); } else { vec.push(None); } },
                    DataType::String => if let Some(vec) = boxed_any.downcast_mut::<Vec<Option<String>>>() { if let AggValue::String(opt_val) = agg_value { vec.push(opt_val); } else { vec.push(None); } },
                    DataType::Bool => if let Some(vec) = boxed_any.downcast_mut::<Vec<Option<bool>>>() { if let AggValue::Bool(opt_val) = agg_value { vec.push(opt_val); } else { vec.push(None); } },
                    _ => unreachable!(),
                }
            }
        }

        let mut final_columns: Vec<Box<dyn SeriesTrait>> = Vec::with_capacity(self.keys.len() + value_col_names.len());
        for (i, key_name) in self.keys.iter().enumerate() {
            let boxed_any = &key_data_vecs[i];
            let final_key_series: Box<dyn SeriesTrait> = match key_dtypes[i] {
                 DataType::Int32 => Box::new(Series::new_from_options(key_name.clone(), boxed_any.downcast_ref::<Vec<Option<i32>>>().unwrap().clone())),
                 DataType::String => Box::new(Series::new_from_options(key_name.clone(), boxed_any.downcast_ref::<Vec<Option<String>>>().unwrap().clone())),
                 DataType::Bool => Box::new(Series::new_from_options(key_name.clone(), boxed_any.downcast_ref::<Vec<Option<bool>>>().unwrap().clone())),
                 _ => unreachable!(),
            };
            final_columns.push(final_key_series);
        }
        for (j, value_col_name) in value_col_names.iter().enumerate() {
            let boxed_any = &agg_data_vecs[j];
            let final_agg_series: Box<dyn SeriesTrait> = match agg_dtypes[j] {
                 DataType::Int8 => Box::new(Series::new_from_options(value_col_name.clone(), boxed_any.downcast_ref::<Vec<Option<i8>>>().unwrap().clone())),
                 DataType::Int16 => Box::new(Series::new_from_options(value_col_name.clone(), boxed_any.downcast_ref::<Vec<Option<i16>>>().unwrap().clone())),
                 DataType::Int32 => Box::new(Series::new_from_options(value_col_name.clone(), boxed_any.downcast_ref::<Vec<Option<i32>>>().unwrap().clone())),
                 DataType::Int64 => Box::new(Series::new_from_options(value_col_name.clone(), boxed_any.downcast_ref::<Vec<Option<i64>>>().unwrap().clone())),
                 DataType::UInt8 => Box::new(Series::new_from_options(value_col_name.clone(), boxed_any.downcast_ref::<Vec<Option<u8>>>().unwrap().clone())),
                 DataType::UInt16 => Box::new(Series::new_from_options(value_col_name.clone(), boxed_any.downcast_ref::<Vec<Option<u16>>>().unwrap().clone())),
                 DataType::UInt32 => Box::new(Series::new_from_options(value_col_name.clone(), boxed_any.downcast_ref::<Vec<Option<u32>>>().unwrap().clone())),
                 DataType::UInt64 => Box::new(Series::new_from_options(value_col_name.clone(), boxed_any.downcast_ref::<Vec<Option<u64>>>().unwrap().clone())),
                 DataType::Float32 => Box::new(Series::new_from_options(value_col_name.clone(), boxed_any.downcast_ref::<Vec<Option<f32>>>().unwrap().clone())),
                 DataType::Float64 => Box::new(Series::new_from_options(value_col_name.clone(), boxed_any.downcast_ref::<Vec<Option<f64>>>().unwrap().clone())),
                 DataType::String => Box::new(Series::new_from_options(value_col_name.clone(), boxed_any.downcast_ref::<Vec<Option<String>>>().unwrap().clone())),
                 DataType::Bool => Box::new(Series::new_from_options(value_col_name.clone(), boxed_any.downcast_ref::<Vec<Option<bool>>>().unwrap().clone())),
                 _ => unreachable!(),
            };
            final_columns.push(final_agg_series);
        }

        DataFrame::new(final_columns)
    }
}