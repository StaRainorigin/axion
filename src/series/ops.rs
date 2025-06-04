use std::fmt::{Display, Debug};

use crate::error::AxionResult;
use crate::series::core::Series;
use crate::dtype::DataTypeTrait;

/// Series 与标量值比较操作的 trait
/// 
/// 提供了 Series 与单个值进行逐元素比较的功能。
pub trait SeriesCompareScalar<Rhs>
where
    Rhs: Clone + PartialOrd + PartialEq,
{
    /// 大于比较
    fn gt_scalar(&self, rhs: Rhs) -> AxionResult<Series<bool>>;
    
    /// 小于比较
    fn lt_scalar(&self, rhs: Rhs) -> AxionResult<Series<bool>>;
    
    /// 等于比较
    fn eq_scalar(&self, rhs: Rhs) -> AxionResult<Series<bool>>;
    
    /// 不等于比较
    fn neq_scalar(&self, rhs: Rhs) -> AxionResult<Series<bool>>;
    
    /// 大于等于比较
    fn gte_scalar(&self, rhs: Rhs) -> AxionResult<Series<bool>>;
    
    /// 小于等于比较
    fn lte_scalar(&self, rhs: Rhs) -> AxionResult<Series<bool>>;
}

/// Series 与 Series 逐元素比较操作的 trait
/// 
/// 提供了两个 Series 之间进行逐元素比较的功能。
pub trait SeriesCompareSeries<Rhs> {
    /// 大于比较
    fn gt_series(&self, rhs: Rhs) -> AxionResult<Series<bool>>;
    
    /// 小于比较
    fn lt_series(&self, rhs: Rhs) -> AxionResult<Series<bool>>;
    
    /// 等于比较
    fn eq_series(&self, rhs: Rhs) -> AxionResult<Series<bool>>;
    
    /// 不等于比较
    fn neq_series(&self, rhs: Rhs) -> AxionResult<Series<bool>>;
    
    /// 大于等于比较
    fn gte_series(&self, rhs: Rhs) -> AxionResult<Series<bool>>;
    
    /// 小于等于比较
    fn lte_series(&self, rhs: Rhs) -> AxionResult<Series<bool>>;
}

/// 统一的 Series 比较操作 trait
/// 
/// 提供了统一的比较接口，支持与标量或其他 Series 进行比较。
pub trait SeriesCompare<Rhs> {
    /// 大于比较
    fn gt(&self, rhs: Rhs) -> AxionResult<Series<bool>>;
    
    /// 小于比较
    fn lt(&self, rhs: Rhs) -> AxionResult<Series<bool>>;
    
    /// 等于比较
    fn eq(&self, rhs: Rhs) -> AxionResult<Series<bool>>;
    
    /// 不等于比较
    fn neq(&self, rhs: Rhs) -> AxionResult<Series<bool>>;
    
    /// 大于等于比较
    fn gte(&self, rhs: Rhs) -> AxionResult<Series<bool>>;
    
    /// 小于等于比较
    fn lte(&self, rhs: Rhs) -> AxionResult<Series<bool>>;
}

/// Series 与标量算术运算操作的 trait
/// 
/// 提供了 Series 与标量值进行逐元素算术运算的功能。
pub trait SeriesArithScalar<Rhs>
where
    Rhs: Clone,
{
    /// 加法运算的输出类型
    type AddOutput: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static;
    
    /// 减法运算的输出类型
    type SubOutput: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static;
    
    /// 乘法运算的输出类型
    type MulOutput: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static;
    
    /// 除法运算的输出类型
    type DivOutput: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static;
    
    /// 取余运算的输出类型
    type RemOutput: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static;

    /// 逐元素加法运算
    fn add_scalar(&self, rhs: Rhs) -> AxionResult<Series<Self::AddOutput>>;

    /// 逐元素减法运算
    fn sub_scalar(&self, rhs: Rhs) -> AxionResult<Series<Self::SubOutput>>;

    /// 逐元素乘法运算
    fn mul_scalar(&self, rhs: Rhs) -> AxionResult<Series<Self::MulOutput>>;

    /// 逐元素除法运算
    fn div_scalar(&self, rhs: Rhs) -> AxionResult<Series<Self::DivOutput>>;

    /// 逐元素取余运算
    fn rem_scalar(&self, rhs: Rhs) -> AxionResult<Series<Self::RemOutput>>;
}

/// Series 与 Series 算术运算操作的 trait
/// 
/// 提供了两个 Series 之间进行逐元素算术运算的功能。
pub trait SeriesArithSeries<Rhs>
where
    Rhs: ?Sized,
{
    /// 加法运算的输出类型
    type AddOutput: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static;
    
    /// 减法运算的输出类型
    type SubOutput: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static;
    
    /// 乘法运算的输出类型
    type MulOutput: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static;
    
    /// 除法运算的输出类型
    type DivOutput: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static;
    
    /// 取余运算的输出类型
    type RemOutput: DataTypeTrait + Clone + Debug + Display + Send + Sync + 'static;

    /// 逐元素加法运算
    fn add_series(&self, rhs: Rhs) -> AxionResult<Series<Self::AddOutput>>;

    /// 逐元素减法运算
    fn sub_series(&self, rhs: Rhs) -> AxionResult<Series<Self::SubOutput>>;

    /// 逐元素乘法运算
    fn mul_series(&self, rhs: Rhs) -> AxionResult<Series<Self::MulOutput>>;

    /// 逐元素除法运算
    fn div_series(&self, rhs: Rhs) -> AxionResult<Series<Self::DivOutput>>;

    /// 逐元素取余运算
    fn rem_series(&self, rhs: Rhs) -> AxionResult<Series<Self::RemOutput>>;
}