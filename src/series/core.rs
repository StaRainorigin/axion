//! Series 核心实现模块
//! 
//! 提供 Series 数据结构的完整实现，包括基本操作、数学运算、比较操作等。

use crate::dtype::{DataType, DataTypeTrait}; 
use crate::error::{AxionError, CastError, AxionResult}; 
use super::interface::SeriesTrait;
use super::ops::{SeriesArithScalar, SeriesCompareScalar, SeriesCompare, SeriesCompareSeries, SeriesArithSeries}; 
use super::string::StringAccessor;

use std::cmp::Ordering;
use std::fmt::{self, Debug, Display};
use std::any::Any;
use num_traits::{Float, Zero, ToPrimitive}; 
use std::iter::{FromIterator, Sum};
use std::ops::{Add, Sub, Mul, Div, Rem, Deref};

use rayon::prelude::*;

/// Series 元数据标志，用于优化性能
#[derive(Debug, Clone, Copy, Default)]
pub struct SeriesFlags {
    /// 标记 Series 是否已排序 (升序)
    is_sorted_ascending: bool,
    /// 标记 Series 是否已排序 (降序)
    is_sorted_descending: bool,
}

impl SeriesFlags {
    /// 检查是否按升序排序
    pub fn is_sorted_ascending(&self) -> bool {
        self.is_sorted_ascending
    }

    /// 检查是否按降序排序
    pub fn is_sorted_descending(&self) -> bool {
        self.is_sorted_descending
    }

    /// 检查是否已排序 (任意方向)
    pub fn is_sorted(&self) -> bool {
        self.is_sorted_ascending || self.is_sorted_descending
    }

    /// 设置排序标志
    fn set_sorted(&mut self, ascending: bool, descending: bool) {
        self.is_sorted_ascending = ascending;
        self.is_sorted_descending = descending;
    }

    /// 清除排序标志
    fn clear_sorted(&mut self) {
        self.is_sorted_ascending = false;
        self.is_sorted_descending = false;
    }
}

/// 数据源转换 trait，定义如何将数据源转换为 Series 内部格式
pub trait IntoSeriesData<T>
where
    T: DataTypeTrait + Clone + Debug,
    Self: Sized,
{
    /// 消耗数据源，返回 Vec<T> 和对应的 DataType
    fn into_series_data(self) -> (Vec<T>, DataType);
}

/// 为 Vec<T> 实现数据源转换
impl<T> IntoSeriesData<T> for Vec<T>
where
    T: DataTypeTrait + Clone + Debug,
{
    fn into_series_data(self) -> (Vec<T>, DataType) {
        let dtype = T::DTYPE;
        (self, dtype)
    }
}

/// 为切片引用实现数据源转换
impl<T> IntoSeriesData<T> for &[T]
where
    T: DataTypeTrait + Clone + Debug,
{
    fn into_series_data(self) -> (Vec<T>, DataType) {
        let dtype = T::DTYPE;
        (self.to_vec(), dtype)
    }
}

/// 为固定大小数组引用实现数据源转换
impl<T, const N: usize> IntoSeriesData<T> for &[T; N]
where
    T: DataTypeTrait + Clone + Debug,
{
    fn into_series_data(self) -> (Vec<T>, DataType) {
        let dtype = T::DTYPE;
        (self.to_vec(), dtype)
    }
}

/// 为固定大小数组实现数据源转换
impl<T, const N: usize> IntoSeriesData<T> for [T; N]
where
    T: DataTypeTrait + Clone + Debug,
{
    fn into_series_data(self) -> (Vec<T>, DataType) {
        let dtype = T::DTYPE;
        (self.to_vec(), dtype)
    }
}

/// 为字符串字面量数组引用实现转换为 String Series
impl<'a, const N: usize> IntoSeriesData<String> for &'a [&'a str; N] {
    fn into_series_data(self) -> (Vec<String>, DataType) {
        let data = self.iter().map(|s| s.to_string()).collect::<Vec<String>>();
        (data, DataType::String)
    }
}

/// 为字符串字面量 Vec 实现转换为 String Series
impl IntoSeriesData<String> for Vec<&str> {
    fn into_series_data(self) -> (Vec<String>, DataType) {
        let data = self.into_iter().map(|s| s.to_string()).collect::<Vec<String>>();
        (data, DataType::String)
    }
}

/// 为字符串字面量切片实现转换为 String Series
impl<'a> IntoSeriesData<String> for &'a [&'a str] {
    fn into_series_data(self) -> (Vec<String>, DataType) {
        let data = self.iter().map(|s| s.to_string()).collect::<Vec<String>>();
        (data, DataType::String)
    }
}

/// Series 核心数据结构
/// 
/// 一维的类型化数据序列，支持空值处理和高效计算。
/// 类似于 pandas 的 Series 或 R 的向量。
pub struct Series<T>
{
    /// Series 的名称标识符
    name: String,
    /// 数据类型信息
    dtype: DataType,
    /// 实际数据存储，使用 Option<T> 支持空值
    pub data: Vec<Option<T>>,
    /// 元数据标志，用于性能优化
    flags: SeriesFlags,
}

/// 手动实现 Clone (当 T 实现 Clone 时)
impl<T: Clone> Clone for Series<T> {
    fn clone(&self) -> Self {
        Series {
            name: self.name.clone(),
            dtype: self.dtype.clone(),
            data: self.data.clone(),
            flags: self.flags,
        }
    }
}

/// Debug 实现，显示 Series 的关键信息
impl<T> Debug for Series<T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Series")
            .field("name", &self.name)
            .field("dtype", &self.dtype)
            .field("len", &self.len())
            .field("flags", &self.flags)
            .finish()
    }
}

/// Display 实现，以数组格式显示数据
impl<T> Display for Series<T>
where
    T: Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[")?;

        let mut first = true;
        for opt_val in &self.data {
            if !first {
                write!(f, ", ")?;
            }
            match opt_val {
                Some(value) => write!(f, "{}", value)?,
                None => write!(f, "null")?,
            }
            first = false;
        }

        write!(f, "]")
    }
}

/// Default 实现，创建空的 Series
impl<T> Default for Series<T> {
    fn default() -> Self {
        Series {
            name: String::new(),
            dtype: DataType::Null,
            data: Vec::new(),
            flags: SeriesFlags::default(),
        }
    }
}

/// Deref 实现，允许像 Vec<Option<T>> 一样访问
impl<T> Deref for Series<T> {
    type Target = Vec<Option<T>>;

    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<T> Series<T> {
    /// 从数据源创建新的 Series，所有值标记为非空
    pub fn new<D>(name: String, data_source: D) -> Self
    where
        T: DataTypeTrait + Clone + Debug + Send + Sync + 'static,
        D: IntoSeriesData<T>,
    {
        let (data, dtype) = data_source.into_series_data();
        let data_opts = data.into_iter().map(Some).collect();
        Series { name, dtype, data: data_opts, flags: SeriesFlags::default() }
    }

    /// 创建指定类型的空 Series
    pub fn new_empty(name: String, dtype: DataType) -> Self {
        Series { name, dtype, data: Vec::new(), flags: SeriesFlags::default() }
    }

    /// 从可选值向量创建 Series，支持显式空值
    pub fn new_from_options(name: String, data: Vec<Option<T>>) -> Self
    where
        T: DataTypeTrait,
    {
        let dtype = T::DTYPE;
        Series { name, dtype, data, flags: SeriesFlags::default() }
    }

    /// 清空 Series 的所有数据
    pub fn clear(&mut self) {
        self.data.clear();
        self.dtype = DataType::Null;
        self.flags = SeriesFlags::default();
    }

    /// 向 Series 末尾添加一个元素
    pub fn push(&mut self, value: Option<T>) where T: DataTypeTrait {
        if self.dtype == DataType::Null {
            if let Some(ref v) = value {
                self.dtype = v.as_dtype();
            }
        }
        self.flags.clear_sorted();
        self.data.push(value);
    }

    // === 基本属性访问方法 ===

    /// 获取 Series 的名称
    pub fn name(&self) -> &str {
        &self.name
    }

    /// 获取 Series 的数据类型
    pub fn dtype(&self) -> DataType {
        self.dtype.clone()
    }

    /// 获取 Series 的长度（包括空值）
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// 检查 Series 是否为空
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// 获取指定索引处的值引用（跳过空值）
    pub fn get(&self, index: usize) -> Option<&T> {
        self.data.get(index).and_then(|opt_val| opt_val.as_ref())
    }

    /// 返回迭代器，遍历所有元素（包括空值）
    pub fn iter(&self) -> impl Iterator<Item = Option<&T>> + '_ {
        self.data.iter().map(|opt_val| opt_val.as_ref())
    }

    /// 返回迭代器，只遍历有效值（跳过空值）
    pub fn iter_valid(&self) -> impl Iterator<Item = &T> + '_ {
        self.data.iter().filter_map(|opt_val| opt_val.as_ref())
    }

    /// 返回迭代器，遍历有效值的拥有副本
    pub fn iter_valid_owned(&self) -> impl Iterator<Item = T> + '_ where T: Clone {
        self.data.iter().filter_map(|opt_val| opt_val.clone())
    }

    /// 获取内部数据向量的引用
    pub fn data_internal(&self) -> &Vec<Option<T>> {
        &self.data
    }

    /// 消费 Series，返回内部数据向量
    pub fn take_inner(self) -> Vec<Option<T>> {
        self.data
    }

    /// 修改 Series 的名称
    pub fn rename(&mut self, name: String) {
        self.name = name;
    }

    /// 返回带有新名称的 Series（消费原 Series）
    pub fn with_name(mut self, name: String) -> Self {
        self.name = name;
        self
    }

    // === 排序状态查询方法 ===

    /// 检查是否按升序排序
    pub fn is_sorted_ascending(&self) -> bool {
        self.flags.is_sorted_ascending()
    }

    /// 检查是否按降序排序
    pub fn is_sorted_descending(&self) -> bool {
        self.flags.is_sorted_descending()
    }

    /// 检查是否已排序（任意方向）
    pub fn is_sorted(&self) -> bool {
        self.flags.is_sorted()
    }

    /// 设置排序标志（内部使用）
    pub fn set_sorted_flag(&mut self, ascending: bool, descending: bool) {
        self.flags.set_sorted(ascending, descending);
    }

    /// 获取标志副本
    pub fn get_flags(&self) -> SeriesFlags {
        self.flags
    }

    /// 获取指定索引处的完整 Option 引用
    /// 
    /// 区分索引越界和空值：
    /// - `Some(Some(&T))` - 有值
    /// - `Some(None)` - 空值  
    /// - `None` - 索引越界
    pub fn get_opt(&self, index: usize) -> Option<Option<&T>> {
        self.data.get(index).map(|opt_t| opt_t.as_ref())
    }

    /// 就地排序 Series
    /// 
    /// # 参数
    /// * `reverse` - false 为升序，true 为降序
    pub fn sort(&mut self, reverse: bool)
    where
        T: Ord,
    {
        let compare_options = |a: &Option<T>, b: &Option<T>| -> Ordering {
            match (a, b) {
                (Some(va), Some(vb)) => va.cmp(vb),
                (None, None) => Ordering::Equal,
                (None, Some(_)) => Ordering::Less,
                (Some(_), None) => Ordering::Greater,
            }
        };

        if reverse {
            self.data.sort_by(|a, b| compare_options(b, a));
            self.flags.set_sorted(false, true);
        } else {
            self.data.sort_by(compare_options);
            self.flags.set_sorted(true, false);
        }
    }

    /// 类型转换（实验性功能）
    /// 
    /// 目前只支持 f64 到 f64 的转换
    pub fn cast<NewType>(&self) -> AxionResult<Series<NewType>>
    where
        T: Clone + 'static,
        NewType: DataTypeTrait + Clone + Debug + Default + 'static,
    {
        let target_dtype = NewType::default().as_dtype();

        let mut new_data = Vec::with_capacity(self.len());
        for opt_val in self.data.iter() {
            match opt_val {
                Some(val) => {
                    if std::any::TypeId::of::<T>() == std::any::TypeId::of::<f64>()
                        && std::any::TypeId::of::<NewType>() == std::any::TypeId::of::<f64>()
                    {
                        let any_ref = val as &dyn std::any::Any;
                        if let Some(float_val) = any_ref.downcast_ref::<f64>() {
                            let new_val_any = float_val as &dyn std::any::Any;
                            if let Some(new_val) = new_val_any.downcast_ref::<NewType>() {
                                new_data.push(Some(new_val.clone()));
                            } else {
                                return Err(AxionError::CastError(CastError("Failed to downcast to target type after 'as' cast".to_string())));
                            }
                        } else {
                            return Err(AxionError::CastError(CastError("Failed to downcast original value".to_string())));
                        }
                    } else {
                        return Err(AxionError::CastError(CastError(format!(
                            "Unsupported cast from {:?} to {:?}",
                            self.dtype(),
                            target_dtype
                        ))));
                    }
                }
                None => new_data.push(None),
            }
        }

        Ok(Series {
            name: self.name.clone(),
            dtype: target_dtype,
            data: new_data,
            flags: SeriesFlags::default(),
        })
    }

    // === 数值计算方法 ===

    /// 计算所有有效值的和
    pub fn sum(&self) -> Option<T>
    where
        T: Sum<T> + Clone + Zero,
    {
        let iter = self.iter_valid_owned();
        let total: T = iter.sum();

        if self.data.iter().any(|opt| opt.is_some()) {
            Some(total)
        } else {
            None
        }
    }

    /// 计算所有有效值的最小值
    pub fn min(&self) -> Option<T>
    where
        T: PartialOrd + Clone,
    {
        self.data
            .iter()
            .filter_map(|opt| opt.as_ref())
            .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .cloned()
    }

    /// 计算所有有效值的最大值
    pub fn max(&self) -> Option<T>
    where
        T: PartialOrd + Clone,
    {
        self.data
            .iter()
            .filter_map(|opt| opt.as_ref())
            .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
            .cloned()
    }

    // === 浮点数特殊检测方法 ===

    /// 检测每个元素是否为 NaN（仅浮点数类型）
    pub fn is_nan(&self) -> Series<bool>
    where
        T: Float,
    {
        let new_data: Vec<Option<bool>> = self.data.iter().map(|opt_val| {
            opt_val.map(|v| v.is_nan())
                   .or(Some(false))
        }).collect();
        Series::new_from_options(format!("{}_is_nan", self.name), new_data)
    }

    /// 检测每个元素是否不为 NaN
    pub fn is_not_nan(&self) -> Series<bool>
    where
        T: Float,
    {
        let new_data: Vec<Option<bool>> = self.data.iter().map(|opt_val| {
            opt_val.map(|v| !v.is_nan())
                   .or(Some(true))
        }).collect();
        Series::new_from_options(format!("{}_is_not_nan", self.name), new_data)
    }

    /// 检测每个元素是否为无穷大
    pub fn is_infinite(&self) -> Series<bool>
    where
        T: Float,
    {
        let new_data: Vec<Option<bool>> = self.data.iter().map(|opt_val| {
            opt_val.map(|v| v.is_infinite())
                   .or(Some(false))
        }).collect();
        Series::new_from_options(format!("{}_is_infinite", self.name), new_data)
    }

    // === 函数式编程方法 ===

    /// 对每个元素应用函数，可改变元素类型
    pub fn map<U, F>(&self, f: F) -> Series<U>
    where
        T: Clone,
        U: DataTypeTrait + Clone + Debug,
        F: FnMut(Option<T>) -> Option<U>,
    {
        let new_data: Vec<Option<U>> = self.data.iter()
                                             .cloned()
                                             .map(f)
                                             .collect();
        Series::new_from_options(self.name.clone(), new_data)
    }

    /// 根据布尔掩码过滤 Series
    /// 
    /// # Panic
    /// 如果掩码长度与 Series 长度不匹配会 panic
    pub fn filter(&self, mask: &Series<bool>) -> Self where T: Clone {
        if self.len() != mask.len() {
            panic!(
                "Filter mask length ({}) must match Series length ({})",
                mask.len(),
                self.len()
            );
        }

        let new_data: Vec<Option<T>> = self.data
            .iter()
            .zip(mask.data.iter())
            .filter_map(|(data_opt, mask_opt)| {
                match mask_opt {
                    Some(true) => Some(data_opt.clone()),
                    _ => None,
                }
            })
            .collect();

        Series {
            name: self.name.clone(),
            dtype: self.dtype.clone(),
            data: new_data,
            flags: SeriesFlags::default(),
        }
    }

    /// 检查两个 Series 是否完全相等（包括名称）
    pub fn equals(&self, other: &Self) -> bool
    where
        T: PartialEq,
    {
        if self.name != other.name || self.dtype != other.dtype || self.len() != other.len() {
            return false;
        }
        self.data.iter().eq(other.data.iter())
    }

    /// 检查两个 Series 的数据是否相等（忽略名称）
    pub fn equals_missing(&self, other: &Self) -> bool
    where
        T: PartialEq,
    {
        if self.name != other.name || self.dtype != other.dtype || self.len() != other.len() {
            return false;
        }
        self.data.iter().eq(other.data.iter())
    }

    // === 内部操作方法 ===

    /// Series 间的二元操作
    fn binary_op<F, U>(&self, other: &Series<T>, op: F) -> Series<U>
    where
        T: Clone,
        U: DataTypeTrait + Clone + Debug,
        F: Fn(T, T) -> U,
    {
        if self.len() != other.len() {
            panic!("Cannot perform operation on Series of different lengths");
        }

        let new_data = self.data.iter().zip(other.data.iter()).map(|(opt_a, opt_b)| {
            match (opt_a, opt_b) {
                (Some(a), Some(b)) => Some(op(a.clone(), b.clone())),
                _ => None,
            }
        }).collect();

        Series::new_from_options(self.name.clone(), new_data)
    }

    /// Series 与标量的运算
    fn scalar_op<F, U>(&self, scalar: T, op: F) -> Series<U>
    where
        T: Clone,
        U: DataTypeTrait + Clone + Debug,
        F: Fn(T, T) -> U,
    {
        let new_data = self.data.iter().map(|opt_a| {
            opt_a.as_ref().map(|a| op(a.clone(), scalar.clone()))
        }).collect();
        Series::new_from_options(self.name.clone(), new_data)
    }

    /// 计算所有有效值的平均值，结果为 f64
    pub fn mean(&self) -> Option<f64>
    where
        T: Clone + Zero + ToPrimitive,
    {
        let mut sum = 0.0f64;
        let mut count = 0usize;

        for val in self.data.iter().flatten() {
            if let Some(float_val) = val.to_f64() {
                sum += float_val;
                count += 1;
            }
        }

        if count > 0 {
            Some(sum / count as f64)
        } else {
            None
        }
    }

    /// 对每个元素应用函数（接收引用）
    /// 
    /// 与 `map` 的区别：此方法接收引用，不移动所有权
    pub fn apply<F, U>(&self, func: F) -> Series<U>
    where
        U: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static,
        F: Fn(Option<&T>) -> Option<U>,
    {
        let new_data: Vec<Option<U>> = self.data.iter()
            .map(|opt_ref_t| func(opt_ref_t.as_ref()))
            .collect();

        if self.is_empty() {
            let dtype = U::DTYPE;
            Series::new_empty(self.name.clone(), dtype)
        } else {
            Series::new_from_options(self.name.clone(), new_data)
        }
    }

    /// 并行对每个元素应用函数
    /// 
    /// 适用于计算密集型转换操作
    pub fn par_apply<F, U>(&self, func: F) -> Series<U>
    where
        U: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static,
        T: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static,
        F: Fn(Option<&T>) -> Option<U> + Send + Sync,
    {
        if self.is_empty() {
            return Series::new_empty(self.name.clone(), U::DTYPE);
        }

        let new_data: Vec<Option<U>> = self.data
            .par_iter()
            .map(|opt_val| func(opt_val.as_ref()))
            .collect();

        Series::new_from_options(self.name.clone(), new_data)
    }
}

// === 字符串 Series 特殊方法 ===

impl Series<String> {
    /// 返回字符串操作访问器
    pub fn str(&self) -> StringAccessor<'_> {
        StringAccessor::new(self)
    }
}

// === 运算符重载实现 ===

impl<T> Add<&Series<T>> for &Series<T>
where
    T: DataTypeTrait + Clone + Debug + Add<Output = T>,
{
    type Output = Series<T>;

    fn add(self, rhs: &Series<T>) -> Self::Output {
        self.binary_op(rhs, |a, b| a + b)
    }
}

impl<T> Add<T> for &Series<T>
where
    T: DataTypeTrait + Clone + Debug + Add<Output = T>,
{
    type Output = Series<T>;

    fn add(self, rhs: T) -> Self::Output {
        self.scalar_op(rhs, |a, b| a + b)
    }
}

impl<T> Sub<&Series<T>> for &Series<T>
where
    T: DataTypeTrait + Clone + Debug + Sub<Output = T>,
{
    type Output = Series<T>;

    fn sub(self, rhs: &Series<T>) -> Self::Output {
        self.binary_op(rhs, |a, b| a - b)
    }
}

impl<T> Sub<T> for &Series<T>
where
    T: DataTypeTrait + Clone + Debug + Sub<Output = T>,
{
    type Output = Series<T>;

    fn sub(self, rhs: T) -> Self::Output {
        self.scalar_op(rhs, |a, b| a - b)
    }
}

impl<T> Mul<&Series<T>> for &Series<T>
where
    T: DataTypeTrait + Clone + Debug + Mul<Output = T>,
{
    type Output = Series<T>;

    fn mul(self, rhs: &Series<T>) -> Self::Output {
        self.binary_op(rhs, |a, b| a * b)
    }
}

impl<T> Mul<T> for &Series<T>
where
    T: DataTypeTrait + Clone + Debug + Mul<Output = T>,
{
    type Output = Series<T>;

    fn mul(self, rhs: T) -> Self::Output {
        self.scalar_op(rhs, |a, b| a * b)
    }
}

impl<T> Div<&Series<T>> for &Series<T>
where
    T: DataTypeTrait + Clone + Debug + Div<Output = T> + Zero,
{
    type Output = Series<T>;

    fn div(self, rhs: &Series<T>) -> Self::Output {
        if self.len() != rhs.len() {
            panic!("Cannot perform operation on Series of different lengths");
        }
        let new_data = self.data.iter().zip(rhs.data.iter()).map(|(opt_a, opt_b)| {
            match (opt_a, opt_b) {
                (Some(a), Some(b)) => {
                    if b.is_zero() { None }
                    else { Some(a.clone() / b.clone()) }
                },
                _ => None,
            }
        }).collect();
        Series::new_from_options(self.name.clone(), new_data)
    }
}

impl<T> Div<T> for &Series<T>
where
    T: DataTypeTrait + Clone + Debug + Div<Output = T> + Zero,
{
    type Output = Series<T>;

    fn div(self, rhs: T) -> Self::Output {
        if rhs.is_zero() {
            let new_data = vec![None; self.len()];
            Series::new_from_options(self.name.clone(), new_data)
        } else {
            self.scalar_op(rhs, |a, b| a / b)
        }
    }
}

impl<T> Rem<&Series<T>> for &Series<T>
where
    T: DataTypeTrait + Clone + Debug + Rem<Output = T> + Zero,
{
    type Output = Series<T>;

    fn rem(self, rhs: &Series<T>) -> Self::Output {
        if self.len() != rhs.len() {
            panic!("Cannot perform operation on Series of different lengths");
        }
        let new_data = self.data.iter().zip(rhs.data.iter()).map(|(opt_a, opt_b)| {
            match (opt_a, opt_b) {
                (Some(a), Some(b)) => {
                    if b.is_zero() { None }
                    else { Some(a.clone() % b.clone()) }
                },
                _ => None,
            }
        }).collect();
        Series::new_from_options(self.name.clone(), new_data)
    }
}

impl<T> Rem<T> for &Series<T>
where
    T: DataTypeTrait + Clone + Debug + Rem<Output = T> + Zero,
{
    type Output = Series<T>;

    fn rem(self, rhs: T) -> Self::Output {
        if rhs.is_zero() {
            let new_data = vec![None; self.len()];
            Series::new_from_options(self.name.clone(), new_data)
        } else {
            self.scalar_op(rhs, |a, b| a % b)
        }
    }
}

// === FromIterator 实现 ===

impl<T> FromIterator<T> for Series<T>
where
    T: DataTypeTrait + Clone + Debug,
{
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let data: Vec<T> = iter.into_iter().collect();
        let (data_vec, dtype) = data.into_series_data();
        let data_opts = data_vec.into_iter().map(Some).collect();
        Series {
            name: String::new(),
            dtype,
            data: data_opts,
            flags: SeriesFlags::default(),
        }
    }
}

impl<T> FromIterator<Option<T>> for Series<T>
where
    T: DataTypeTrait + Clone + Debug,
{
    fn from_iter<I: IntoIterator<Item = Option<T>>>(iter: I) -> Self {
        let data: Vec<Option<T>> = iter.into_iter().collect();
        Series::new_from_options(String::new(), data)
    }
}

// === From 实现 ===

impl<T> From<(String, Vec<T>)> for Series<T>
where
    T: DataTypeTrait + Clone + Debug,
{
    fn from(tuple: (String, Vec<T>)) -> Self {
        let (name, data) = tuple;
        let dtype = data.first().map_or(DataType::Null, |v| v.as_dtype());
        let data_opts = data.into_iter().map(Some).collect();
        Series {
            name,
            dtype,
            data: data_opts,
            flags: SeriesFlags::default(),
        }
    }
}

impl<T> From<(String, Vec<Option<T>>)> for Series<T>
where
    T: DataTypeTrait + Clone + Debug,
{
    fn from(tuple: (String, Vec<Option<T>>)) -> Self {
        let (name, data) = tuple;
        let dtype = data
            .iter()
            .find_map(|opt_val| opt_val.as_ref().map(|v| v.as_dtype()))
            .unwrap_or(DataType::Null);

        Series {
            name,
            dtype,
            data,
            flags: SeriesFlags::default(),
        }
    }
}

impl<'a, T> From<(String, &'a [T])> for Series<T>
where
    T: DataTypeTrait + Clone + Debug,
{
    fn from(tuple: (String, &'a [T])) -> Self {
        let (name, data_slice) = tuple;
        let dtype = data_slice.first().map_or(DataType::Null, |v| v.as_dtype());
        let data_opts = data_slice.iter().cloned().map(Some).collect();
        Series {
            name,
            dtype,
            data: data_opts,
            flags: SeriesFlags::default(),
        }
    }
}

impl<T, const N: usize> From<(String, [T; N])> for Series<T>
where
    T: DataTypeTrait + Clone + Debug,
{
    fn from(tuple: (String, [T; N])) -> Self {
        let (name, data_array) = tuple;
        let dtype = data_array.first().map_or(DataType::Null, |v| v.as_dtype());
        let data_opts = Vec::from(data_array).into_iter().map(Some).collect();
        Series {
            name,
            dtype,
            data: data_opts,
            flags: SeriesFlags::default(),
        }
    }
}

// === IntoIterator 实现 ===

impl<'a, T> IntoIterator for &'a Series<T>
where
    T: DataTypeTrait + Clone + Debug,
{
    type Item = Option<&'a T>;
    type IntoIter = std::iter::Map<std::slice::Iter<'a, Option<T>>, fn(&'a Option<T>) -> Option<&'a T>>;

    fn into_iter(self) -> Self::IntoIter {
        self.data.iter().map(|opt_val| opt_val.as_ref())
    }
}

// === 布尔 Series 特殊方法 ===

impl Series<bool> {
    /// 检查是否所有值都为 true
    pub fn all(&self) -> bool {
        self.data
            .iter()
            .all(|opt_val| matches!(opt_val, Some(true)))
    }

    /// 检查是否存在任何 true 值
    pub fn any(&self) -> bool {
        self.data
            .iter()
            .any(|opt_val| matches!(opt_val, Some(true)))
    }
}

// === SeriesTrait 实现 ===

impl<T> SeriesTrait for Series<T>
where
    T: DataTypeTrait + Display + Debug + Send + Sync + Clone + PartialEq + PartialOrd + 'static,
{
    fn name(&self) -> &str {
        self.name()
    }

    fn dtype(&self) -> DataType {
        self.dtype()
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn get_str(&self, index: usize) -> Option<String> {
        self.data.get(index).map(|opt_val| {
            match opt_val {
                Some(value) => value.to_string(),
                None => "null".to_string(),
            }
        })
    }

    fn is_null_at(&self, index: usize) -> bool {
        self.data.get(index).map_or(true, |opt_val| opt_val.is_none())
    }

    fn clone_box(&self) -> Box<dyn SeriesTrait> {
        Box::new(self.clone())
    }

    fn slice(&self, offset: usize, length: usize) -> Box<dyn SeriesTrait> {
        let start = std::cmp::min(offset, self.len());
        let end = std::cmp::min(start + length, self.len());
        let sliced_data = self.data[start..end].to_vec();
        let new_series = Series::new_from_options(self.name.clone(), sliced_data);
        Box::new(new_series)
    }

    fn filter(&self, mask: &Series<bool>) -> AxionResult<Box<dyn SeriesTrait>> {
        let mut new_data = Vec::with_capacity(self.len());

        for (opt_val, opt_mask_val) in self.data.iter().zip(mask.data_internal().iter()) {
            if let Some(true) = opt_mask_val {
                new_data.push(opt_val.clone());
            }
        }

        let new_series = Series::new_from_options(self.name.clone(), new_data);
        Ok(Box::new(new_series))
    }

    fn take_indices(&self, indices: &[usize]) -> AxionResult<Box<dyn SeriesTrait>> {
        let mut new_data = Vec::with_capacity(indices.len());

        for &idx in indices {
            new_data.push(self.data.get(idx).cloned().ok_or_else(|| AxionError::IndexOutOfBounds(idx, self.len()))?);
        }

        let new_series = Series::new_from_options(self.name.clone(), new_data);
        Ok(Box::new(new_series))
    }

    fn take_indices_option(&self, indices: &[Option<usize>]) -> AxionResult<Box<dyn SeriesTrait>> {
        let mut new_data = Vec::with_capacity(indices.len());
        for opt_idx in indices {
            match opt_idx {
                Some(idx) => {
                    let opt_val = self.data.get(*idx)
                        .ok_or_else(|| AxionError::IndexOutOfBounds(*idx, self.len()))?
                        .clone();
                    new_data.push(opt_val);
                }
                None => {
                    new_data.push(None);
                }
            }
        }
        Ok(Box::new(Series::new_from_options(self.name.clone(), new_data)))
    }

    fn rename(&mut self, new_name: &str){
        self.name = new_name.to_string();
    }

    fn series_equal(&self, other: &dyn SeriesTrait) -> bool {
        if self.dtype() != other.dtype() {
            return false;
        }
        if let Some(other_series) = other.as_any().downcast_ref::<Series<T>>() {
            if T::DTYPE == DataType::Float32 || T::DTYPE == DataType::Float64 {
                self.data.iter().zip(other_series.data.iter()).all(|(a, b)| {
                    match (a, b) {
                        (Some(val_a), Some(val_b)) => {
                            let f_a = unsafe { *(val_a as *const T as *const f64) };
                            let f_b = unsafe { *(val_b as *const T as *const f64) };
                            (f_a.is_nan() && f_b.is_nan()) || (f_a == f_b)
                        }
                        (None, None) => true,
                        _ => false,
                    }
                })
            } else {
                self.data == other_series.data
            }
        } else {
            false
        }
    }

    fn compare_row(&self, a_idx: usize, b_idx: usize) -> Ordering {
        let a_opt = self.data.get(a_idx);
        let b_opt = self.data.get(b_idx);

        match (a_opt, b_opt) {
            (Some(Some(a_val)), Some(Some(b_val))) => {
                match a_val.partial_cmp(b_val) {
                    Some(order) => order,
                    None => Ordering::Equal,
                }
            }
            (Some(Some(_)), Some(None)) => Ordering::Less,
            (Some(None), Some(Some(_))) => Ordering::Greater,
            (Some(None), Some(None)) => Ordering::Equal,
            (None, None) => Ordering::Equal,
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
        }
    }

    fn get_as_f64(&self, index: usize) -> AxionResult<Option<f64>> {
        match self.data.get(index) {
            Some(Some(val)) => {
                let any_val = val as &dyn Any;

                if let Some(v) = any_val.downcast_ref::<i8>() { Ok(v.to_f64()) }
                else if let Some(v) = any_val.downcast_ref::<i16>() { Ok(v.to_f64()) }
                else if let Some(v) = any_val.downcast_ref::<i32>() { Ok(v.to_f64()) }
                else if let Some(v) = any_val.downcast_ref::<i64>() { Ok(v.to_f64()) }
                else if let Some(v) = any_val.downcast_ref::<u8>() { Ok(v.to_f64()) }
                else if let Some(v) = any_val.downcast_ref::<u16>() { Ok(v.to_f64()) }
                else if let Some(v) = any_val.downcast_ref::<u32>() { Ok(v.to_f64()) }
                else if let Some(v) = any_val.downcast_ref::<u64>() { Ok(v.to_f64()) }
                else if let Some(v) = any_val.downcast_ref::<f32>() { Ok(v.to_f64()) }
                else if let Some(v) = any_val.downcast_ref::<f64>() { Ok(Some(*v)) }
                else {
                    Ok(None)
                }
            }
            Some(None) => Ok(None),
            None => Err(AxionError::IndexOutOfBounds(index, self.len())),
        }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Clone for Box<dyn SeriesTrait> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

// === 比较操作 trait 实现 ===

// 宏：简化比较操作实现
macro_rules! impl_compare_scalar {
    ($method_name:ident, $op:tt) => {
        fn $method_name(&self, rhs: Rhs) -> AxionResult<Series<bool>> {
            let new_data: Vec<Option<bool>> = self.data.iter().map(|opt_val| {
                opt_val.as_ref().map(|val| val $op &rhs)
            }).collect();
            Ok(Series::new_from_options(format!("{}_{}", self.name, stringify!($method_name)), new_data))
        }
    };
}

impl<T, Rhs> SeriesCompareScalar<Rhs> for Series<T>
where
    T: DataTypeTrait + Clone + PartialOrd<Rhs> + PartialEq<Rhs>,
    Rhs: Clone + PartialOrd + PartialEq,
{
    impl_compare_scalar!(gt_scalar, >);
    impl_compare_scalar!(lt_scalar, <);
    impl_compare_scalar!(eq_scalar, ==);
    impl_compare_scalar!(neq_scalar, !=);
    impl_compare_scalar!(gte_scalar, >=);
    impl_compare_scalar!(lte_scalar, <=);
}

// 宏：简化算术操作实现
macro_rules! impl_arith_scalar {
    ($method_name:ident, $op_trait:ident, $op_method:ident, $op_symbol:tt, $output_assoc_type:ident) => {
        fn $method_name(&self, rhs: Rhs) -> AxionResult<Series<Self::$output_assoc_type>> {
            let new_data: Vec<Option<Self::$output_assoc_type>> = self.data.iter().map(|opt_val| {
                opt_val.as_ref().map(|val| val.clone().$op_method(rhs.clone()))
            }).collect();
            Ok(Series::new_from_options(format!("{}_{}", self.name, stringify!($method_name)), new_data))
        }
    };
    ($method_name:ident, $op_trait:ident, $op_method:ident, $op_symbol:tt, $output_assoc_type:ident, ZeroCheck) => {
        fn $method_name(&self, rhs: Rhs) -> AxionResult<Series<Self::$output_assoc_type>> {
            if rhs.is_zero() {
                let new_data = vec![None; self.len()];
                Ok(Series::new_from_options(format!("{}_{}_by_zero", self.name, stringify!($method_name)), new_data))
            } else {
                let new_data: Vec<Option<Self::$output_assoc_type>> = self.data.iter().map(|opt_val| {
                    opt_val.as_ref().map(|val| val.clone().$op_method(rhs.clone()))
                }).collect();
                Ok(Series::new_from_options(format!("{}_{}", self.name, stringify!($method_name)), new_data))
            }
        }
    };
}

impl<T, Rhs> SeriesArithScalar<Rhs> for Series<T>
where
    T: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static,
    Rhs: Clone + Zero,
    T: Add<Rhs> + Sub<Rhs> + Mul<Rhs> + Div<Rhs> + Rem<Rhs>,
    <T as Add<Rhs>>::Output: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static,
    <T as Sub<Rhs>>::Output: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static,
    <T as Mul<Rhs>>::Output: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static,
    <T as Div<Rhs>>::Output: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static,
    <T as Rem<Rhs>>::Output: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static,
{
    type AddOutput = <T as Add<Rhs>>::Output;
    type SubOutput = <T as Sub<Rhs>>::Output;
    type MulOutput = <T as Mul<Rhs>>::Output;
    type DivOutput = <T as Div<Rhs>>::Output;
    type RemOutput = <T as Rem<Rhs>>::Output;

    impl_arith_scalar!(add_scalar, Add, add, +, AddOutput);
    impl_arith_scalar!(sub_scalar, Sub, sub, -, SubOutput);
    impl_arith_scalar!(mul_scalar, Mul, mul, *, MulOutput);
    impl_arith_scalar!(div_scalar, Div, div, /, DivOutput, ZeroCheck);
    impl_arith_scalar!(rem_scalar, Rem, rem, %, RemOutput, ZeroCheck);
}

// 宏：简化 Series 间比较操作
macro_rules! impl_compare_series {
    ($method_name:ident, $op:tt) => {
        fn $method_name(&self, rhs: &Series<T>) -> AxionResult<Series<bool>> {
            if self.len() != rhs.len() {
                return Err(AxionError::MismatchedLengths {
                    expected: self.len(),
                    found: rhs.len(),
                    name: rhs.name().to_string(),
                });
            }

            let new_data: Vec<Option<bool>> = self.data.iter()
                .zip(rhs.data.iter())
                .map(|(opt_left, opt_right)| {
                    match (opt_left.as_ref(), opt_right.as_ref()) {
                        (Some(left_val), Some(right_val)) => Some(left_val $op right_val),
                        _ => None,
                    }
                })
                .collect();

            Ok(Series::new_from_options(format!("{}_{}", self.name, stringify!($method_name)), new_data))
        }
    };
}

impl<T> SeriesCompareSeries<&Series<T>> for Series<T>
where
    T: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static,
    T: PartialOrd + PartialEq,
{
    impl_compare_series!(gt_series, >);
    impl_compare_series!(lt_series, <);
    impl_compare_series!(eq_series, ==);
    impl_compare_series!(neq_series, !=);
    impl_compare_series!(gte_series, >=);
    impl_compare_series!(lte_series, <=);
}

// === IntoSeriesBox trait 实现 ===

pub trait IntoSeriesBox<T>
where
    T: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static,
    Self: Sized,
{
    fn into_series_box(self, name: String) -> AxionResult<Box<dyn SeriesTrait>>;
}

impl<T> IntoSeriesBox<T> for Vec<T>
where
    T: DataTypeTrait + Clone + Debug + Display + Send + Sync + PartialEq + PartialOrd + 'static,
{
    fn into_series_box(self, name: String) -> AxionResult<Box<dyn SeriesTrait>> {
        Ok(Box::new(Series::new(name, self)))
    }
}

impl<T> IntoSeriesBox<T> for Vec<Option<T>>
where
    T: DataTypeTrait + Clone + Debug + Display + Send + Sync + PartialEq + PartialOrd + 'static,
{
    fn into_series_box(self, name: String) -> AxionResult<Box<dyn SeriesTrait>> {
        Ok(Box::new(Series::new_from_options(name, self)))
    }
}

impl<T> IntoSeriesBox<T> for &[T]
where
    T: DataTypeTrait + Clone + Debug + Display + Send + Sync + PartialEq + PartialOrd + 'static,
{
    fn into_series_box(self, name: String) -> AxionResult<Box<dyn SeriesTrait>> {
        Ok(Box::new(Series::new(name, self.to_vec())))
    }
}

impl<T> IntoSeriesBox<T> for &[Option<T>]
where
    T: DataTypeTrait + Clone + Debug + Display + Send + Sync + PartialEq + PartialOrd + 'static,
{
    fn into_series_box(self, name: String) -> AxionResult<Box<dyn SeriesTrait>> {
        Ok(Box::new(Series::new_from_options(name, self.to_vec())))
    }
}

impl<'a> IntoSeriesBox<String> for &'a [&'a str]
{
     fn into_series_box(self, name: String) -> AxionResult<Box<dyn SeriesTrait>> {
         let string_vec: Vec<String> = self.iter().map(|&s| s.to_string()).collect();
         string_vec.into_series_box(name)
     }
}

impl<'a, const N: usize> IntoSeriesBox<String> for &'a [&'a str; N]
{
     fn into_series_box(self, name: String) -> AxionResult<Box<dyn SeriesTrait>> {
         let string_vec: Vec<String> = self.iter().map(|&s| s.to_string()).collect();
         string_vec.into_series_box(name)
     }
}

impl<T, const N: usize> IntoSeriesBox<T> for &[T; N]
where
    T: DataTypeTrait + Clone + Debug + Display + Send + Sync + PartialEq + PartialOrd + 'static,
{
    fn into_series_box(self, name: String) -> AxionResult<Box<dyn SeriesTrait>> {
        Ok(Box::new(Series::new(name, self.to_vec())))
    }
}

impl IntoSeriesBox<String> for Vec<&str>
{
    fn into_series_box(self, name: String) -> AxionResult<Box<dyn SeriesTrait>> {
        let string_vec: Vec<String> = self.into_iter().map(|s| s.to_string()).collect();
        string_vec.into_series_box(name)
    }
}

impl IntoSeriesBox<String> for Vec<Option<&str>>
{
    fn into_series_box(self, name: String) -> AxionResult<Box<dyn SeriesTrait>> {
        let string_opt_vec: Vec<Option<String>> = self
            .into_iter()
            .map(|opt_s| opt_s.map(|s| s.to_string()))
            .collect();
        string_opt_vec.into_series_box(name)
    }
}

impl<T, const N: usize> IntoSeriesBox<T> for [T; N]
where
    T: DataTypeTrait + Clone + Debug + Display + Send + Sync + PartialEq + PartialOrd + 'static,
{
    fn into_series_box(self, name: String) -> AxionResult<Box<dyn SeriesTrait>> {
        Ok(Box::new(Series::new(name, self.to_vec())))
    }
}

impl<const N: usize> IntoSeriesBox<String> for [&str; N]
{
     fn into_series_box(self, name: String) -> AxionResult<Box<dyn SeriesTrait>> {
         let string_vec: Vec<String> = self.iter().map(|&s| s.to_string()).collect();
         string_vec.into_series_box(name)
     }
}

// === 高级比较和算术操作实现 ===

impl<T, RhsScalar> SeriesCompare<RhsScalar> for Series<T>
where
    Self: SeriesCompareScalar<RhsScalar>,
    RhsScalar: Clone + PartialOrd + PartialEq,
    T: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static,
{
    #[inline]
    fn gt(&self, rhs: RhsScalar) -> AxionResult<Series<bool>> {
        SeriesCompareScalar::gt_scalar(self, rhs)
    }
    #[inline]
    fn lt(&self, rhs: RhsScalar) -> AxionResult<Series<bool>> {
        SeriesCompareScalar::lt_scalar(self, rhs)
    }
    #[inline]
    fn eq(&self, rhs: RhsScalar) -> AxionResult<Series<bool>> {
        SeriesCompareScalar::eq_scalar(self, rhs)
    }
    #[inline]
    fn neq(&self, rhs: RhsScalar) -> AxionResult<Series<bool>> {
        SeriesCompareScalar::neq_scalar(self, rhs)
    }
    #[inline]
    fn gte(&self, rhs: RhsScalar) -> AxionResult<Series<bool>> {
        SeriesCompareScalar::gte_scalar(self, rhs)
    }
    #[inline]
    fn lte(&self, rhs: RhsScalar) -> AxionResult<Series<bool>> {
        SeriesCompareScalar::lte_scalar(self, rhs)
    }
}

impl<T> SeriesCompare<&Series<T>> for Series<T>
where
    T: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static,
    for<'a> Self: SeriesCompareSeries<&'a Series<T>>,
{
     #[inline]
    fn gt(&self, rhs: &Series<T>) -> AxionResult<Series<bool>> {
        SeriesCompareSeries::gt_series(self, rhs)
    }
     #[inline]
    fn lt(&self, rhs: &Series<T>) -> AxionResult<Series<bool>> {
        SeriesCompareSeries::lt_series(self, rhs)
    }
    #[inline]
    fn eq(&self, rhs: &Series<T>) -> AxionResult<Series<bool>> {
        SeriesCompareSeries::eq_series(self, rhs)
    }
    #[inline]
    fn neq(&self, rhs: &Series<T>) -> AxionResult<Series<bool>> {
        SeriesCompareSeries::neq_series(self, rhs)
    }
    #[inline]
    fn gte(&self, rhs: &Series<T>) -> AxionResult<Series<bool>> {
        SeriesCompareSeries::gte_series(self, rhs)
    }
    #[inline]
    fn lte(&self, rhs: &Series<T>) -> AxionResult<Series<bool>> {
        SeriesCompareSeries::lte_series(self, rhs)
    }
}

// 实现 Series-Series 算术运算 
macro_rules! impl_arith_series {
    // 通用模式 (Add, Sub, Mul, Div, Rem) - 移除零检查
    ($method_name:ident, $op_trait:ident, $op_method:ident, $op_symbol:tt, $output_assoc_type:ident) => {
        fn $method_name(&self, rhs: &Series<T>) -> AxionResult<Series<Self::$output_assoc_type>> {
            // --- 检查长度是否匹配 ---
            if self.len() != rhs.len() {
                return Err(AxionError::MismatchedLengths {
                    expected: self.len(),
                    found: rhs.len(),
                    name: rhs.name().to_string(), // 使用 rhs 的名称报告错误
                });
            }

            // --- 使用 zip 迭代，处理 None ---
            let new_data: Vec<Option<Self::$output_assoc_type>> = self.data.iter()
                .zip(rhs.data.iter())
                .map(|(opt_left, opt_right)| {
                    // 如果任一操作数为 None，结果为 None
                    match (opt_left.as_ref(), opt_right.as_ref()) {
                        (Some(left), Some(right)) => {
                            // --- 克隆左右操作数 ---
                            // 对于 Div/Rem，如果 right 是整数 0，这里会 panic
                            Some(left.clone().$op_method(right.clone()))
                        }
                        _ => None, // 至少有一个是 None
                    }
                })
                .collect();

            // 创建新的 Series
            Ok(Series::new_from_options(format!("{}_{}_{}", self.name, stringify!($method_name), rhs.name), new_data))
        }
    };
}


// --- 为 Series<T> 实现 SeriesArithSeries<&Series<T>> -
impl<T> SeriesArithSeries<&Series<T>> for Series<T>
where
    // T 的基本约束
    T: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static,
    // T 必须能与自身进行相应的运算
    T: Add<T> + Sub<T> + Mul<T> + Div<T> + Rem<T>,
    // 运算结果类型也必须满足 Series<T> 的基本约束
    <T as Add<T>>::Output: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static,
    <T as Sub<T>>::Output: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static,
    <T as Mul<T>>::Output: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static,
    <T as Div<T>>::Output: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static,
    <T as Rem<T>>::Output: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static,
    // --- 移除 Zero 约束，因为我们不再检查零 ---
{
    // --- 定义关联类型 ---
    type AddOutput = <T as Add<T>>::Output;
    type SubOutput = <T as Sub<T>>::Output;
    type MulOutput = <T as Mul<T>>::Output;
    type DivOutput = <T as Div<T>>::Output;
    type RemOutput = <T as Rem<T>>::Output;

    // --- 使用宏实现各个方法 ---
    impl_arith_series!(add_series, Add, add, +, AddOutput);
    impl_arith_series!(sub_series, Sub, sub, -, SubOutput);
    impl_arith_series!(mul_series, Mul, mul, *, MulOutput);
    impl_arith_series!(div_series, Div, div, /, DivOutput);
    impl_arith_series!(rem_series, Rem, rem, %, RemOutput);
}

impl<T: DataTypeTrait + Clone + 'static> Series<T> {
    /// 返回一个布尔 Series，表示每个元素是否为 null
    pub fn is_null(&self) -> Series<bool> {
        let data: Vec<Option<bool>> = self.data.iter().map(|v| Some(v.is_none())).collect();
        Series::new_from_options(format!("{}_is_null", self.name), data)
    }

    /// 返回一个布尔 Series，表示每个元素是否非 null
    pub fn not_null(&self) -> Series<bool> {
        let data: Vec<Option<bool>> = self.data.iter().map(|v| Some(v.is_some())).collect();
        Series::new_from_options(format!("{}_not_null", self.name), data)
    }

    /// 用指定值填充 null，返回新 Series
    pub fn fill_null(&self, value: T) -> Series<T> {
        let data: Vec<Option<T>> = self.data.iter().map(|v| v.clone().or(Some(value.clone()))).collect();
        Series::new_from_options(self.name.clone(), data)
    }
}