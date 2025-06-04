use crate::dtype::DataType;
use crate::series::core::Series;
use crate::error::AxionResult;
use std::any::Any;
use std::fmt::{Debug, Display};
use std::cmp::Ordering;

/// Series 的统一接口 trait
/// 
/// 定义了所有 Series 类型必须实现的核心功能，包括数据访问、
/// 类型转换、过滤、排序等操作。
/// 
/// # 设计原则
/// 
/// - 支持泛型数据类型
/// - 提供高效的数据访问
/// - 支持 null 值处理
/// - 兼容 DataFrame 操作
pub trait SeriesTrait: Display + Debug + Send + Sync + Any {
    /// 返回 Series 的名称
    fn name(&self) -> &str;

    /// 返回 Series 的数据类型
    fn dtype(&self) -> DataType;

    /// 返回 Series 的长度
    fn len(&self) -> usize;

    /// 检查 Series 是否为空
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// 将 Series 转换为 Any trait 对象，用于向下转型
    fn as_any(&self) -> &dyn Any;

    /// 将 Series 转换为可变的 Any trait 对象
    fn as_any_mut(&mut self) -> &mut dyn Any;

    /// 克隆 Series 并返回 trait 对象
    /// 
    /// 用于在 DataFrame 中克隆包含 trait 对象的 Series
    fn clone_box(&self) -> Box<dyn SeriesTrait>;

    /// 获取指定索引处的值的字符串表示
    /// 
    /// # 参数
    /// 
    /// * `index` - 元素索引
    /// 
    /// # 返回值
    /// 
    /// 如果索引有效且值不为 null，返回 `Some(String)`，否则返回 `None`
    fn get_str(&self, index: usize) -> Option<String>;

    /// 检查指定索引处的值是否为 null
    /// 
    /// # 参数
    /// 
    /// * `index` - 要检查的索引
    /// 
    /// # 返回值
    /// 
    /// 如果值为 null 返回 `true`，否则返回 `false`
    /// 
    /// # Panics
    /// 
    /// 如果索引超出范围可能会 panic
    fn is_null_at(&self, index: usize) -> bool;

    /// 创建 Series 的切片
    /// 
    /// # 参数
    /// 
    /// * `start` - 起始索引（包含）
    /// * `end` - 结束索引（不包含）
    fn slice(&self, start: usize, end: usize) -> Box<dyn SeriesTrait>;

    /// 根据布尔掩码过滤 Series
    /// 
    /// 返回一个新的 Series，只包含掩码为 true 的元素。
    /// 
    /// # 参数
    /// 
    /// * `mask` - 布尔类型的 Series，作为过滤条件
    /// 
    /// # 错误
    /// 
    /// 如果掩码长度与 Series 长度不匹配
    fn filter(&self, mask: &Series<bool>) -> AxionResult<Box<dyn SeriesTrait>>;

    /// 根据索引列表选取元素
    /// 
    /// 创建一个新的 Series，包含指定索引处的元素。
    /// 索引不需要有序或唯一，结果按 `indices` 的顺序排列。
    /// 
    /// # 参数
    /// 
    /// * `indices` - 要选取的索引列表
    /// 
    /// # 错误
    /// 
    /// 如果任何索引超出范围
    fn take_indices(&self, indices: &[usize]) -> AxionResult<Box<dyn SeriesTrait>>;

    /// 根据可选索引列表选取元素
    /// 
    /// 对于 `None` 索引，在结果中插入 null 值。
    /// 
    /// # 参数
    /// 
    /// * `indices` - 可选索引列表，`None` 表示插入 null
    /// 
    /// # 错误
    /// 
    /// 如果任何非 `None` 索引超出范围
    fn take_indices_option(&self, indices: &[Option<usize>]) -> AxionResult<Box<dyn SeriesTrait>>;

    /// 重命名 Series
    /// 
    /// # 参数
    /// 
    /// * `new_name` - 新的 Series 名称
    fn rename(&mut self, new_name: &str);

    /// 检查当前 Series 是否与另一个 Series 相等
    /// 
    /// # 参数
    /// 
    /// * `other` - 要比较的 Series
    fn series_equal(&self, other: &dyn SeriesTrait) -> bool;

    /// 比较 Series 中两个索引处的元素
    /// 
    /// 根据预定义策略处理 null 值（例如，null 值排在最后）。
    /// 
    /// # 参数
    /// 
    /// * `a_idx` - 第一个元素的索引
    /// * `b_idx` - 第二个元素的索引
    /// 
    /// # 返回值
    /// 
    /// 返回 `Ordering::Less`、`Ordering::Equal` 或 `Ordering::Greater`
    /// 
    /// # Panics
    /// 
    /// 如果索引超出范围可能会 panic
    fn compare_row(&self, a_idx: usize, b_idx: usize) -> Ordering;

    /// 尝试将指定索引处的值转换为 f64
    /// 
    /// # 参数
    /// 
    /// * `index` - 元素索引
    /// 
    /// # 返回值
    /// 
    /// - 如果索引越界，返回 `Err`
    /// - 如果值为 null，返回 `Ok(None)`
    /// - 如果值无法转换为 f64（如字符串），返回 `Ok(None)`
    /// - 成功转换时返回 `Ok(Some(f64))`
    fn get_as_f64(&self, index: usize) -> AxionResult<Option<f64>>;
}
