use crate::dtype::DataType;
use crate::error::{AxionError, AxionResult};
use super::core::Series;
use super::interface::SeriesTrait;
use std::any::Any;
use std::cmp::Ordering;
use std::fmt::{self, Debug, Display};

/// 表示包含其他 Series 的列表类型 Series
/// 
/// ListSeries 允许在 DataFrame 的单个列中存储复杂的嵌套数据结构。
/// 每个元素本身就是一个 Series，支持不同长度的子 Series。
/// 
/// # 特性
/// 
/// - 支持嵌套的数据结构
/// - 内部 Series 可以有不同的长度
/// - 统一的内部元素类型约束
/// - 完整的 SeriesTrait 实现
#[derive(Clone)]
pub struct ListSeries {
    name: String,
    /// 数据是 Box<dyn SeriesTrait> 的 Option Vec
    data: Vec<Option<Box<dyn SeriesTrait>>>,
    /// 存储列表内部元素的统一数据类型
    inner_dtype: DataType,
}

impl ListSeries {
    /// 创建一个新的 ListSeries
    /// 
    /// # 参数
    /// 
    /// * `name` - Series 名称
    /// * `data` - Series 数据向量
    /// * `inner_dtype` - 内部元素的统一数据类型
    pub fn new(name: String, data: Vec<Option<Box<dyn SeriesTrait>>>, inner_dtype: DataType) -> Self {
        ListSeries { name, data, inner_dtype }
    }

    /// 获取指定索引处的内部 Series
    /// 
    /// # 参数
    /// 
    /// * `index` - 元素索引
    /// 
    /// # 返回值
    /// 
    /// 如果索引有效且值不为 null，返回内部 Series 的引用
    pub fn get_inner_series(&self, index: usize) -> Option<&dyn SeriesTrait> {
        self.data.get(index).and_then(|opt_box| opt_box.as_deref())
    }
}

// 为 ListSeries 实现 SeriesTrait
impl SeriesTrait for ListSeries {
    fn name(&self) -> &str {
        &self.name
    }

    fn dtype(&self) -> DataType {
        // 返回 List 类型，包含内部类型信息
        DataType::List(Box::new(self.inner_dtype.clone()))
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn clone_box(&self) -> Box<dyn SeriesTrait> {
        // 需要 ListSeries 实现 Clone 
        Box::new(self.clone())
    }

    // get_str 的实现比较复杂，需要决定如何显示内部的 Series 
    fn get_str(&self, index: usize) -> Option<String> {
        self.data.get(index).and_then(|opt_box| {
            opt_box.as_ref().map(|inner_series| {
                // --- 修改这里 --- 
                // 1. 检查内部 Series 是否为空
                if inner_series.is_empty() {
                    return "[]".to_string();
                }

                // 2. 获取内部 Series 的前几个元素的字符串表示
                let max_elements_to_show = 5; // 最多显示多少个元素
                let mut elements_str = Vec::with_capacity(max_elements_to_show);
                for i in 0..std::cmp::min(inner_series.len(), max_elements_to_show) {
                    // 调用内部 Series 的 get_str 获取每个元素的字符串
                    elements_str.push(inner_series.get_str(i).unwrap_or_else(|| "null".to_string()));
                }

                // 3. 组合成列表字符串
                let mut result = format!("[{}", elements_str.join(", "));

                // 4. 如果内部 Series 元素过多，添加省略号
                if inner_series.len() > max_elements_to_show {
                    result.push_str(", ...");
                }
                result.push(']');
                result
            })
        })
    }

    // slice 实现
    fn slice(&self, start: usize, length: usize) -> Box<dyn SeriesTrait> {
        let end = std::cmp::min(start + length, self.len());
        let start = std::cmp::min(start, end);
        let sliced_data = self.data[start..end].to_vec(); // Vec<Option<Box<dyn SeriesTrait>>> 支持 Clone
        Box::new(ListSeries::new(self.name.clone(), sliced_data, self.inner_dtype.clone()))
    }

    // filter 实现 (需要 Series<bool>)
    fn filter(&self, mask: &Series<bool>) -> AxionResult<Box<dyn SeriesTrait>> {
        if mask.len() != self.len() {
             return Err(AxionError::MismatchedLengths {
                 expected: self.len(),
                 found: mask.len(),
                 name: format!("filter mask for list series '{}'", self.name),
             });
        }
        let mut new_data = Vec::with_capacity(self.len());
        for (opt_val, opt_mask) in self.data.iter().zip(mask.data_internal().iter()) {
            if let Some(true) = opt_mask {
                new_data.push(opt_val.clone());
            }
        }
        Ok(Box::new(ListSeries::new(self.name.clone(), new_data, self.inner_dtype.clone())))
    }

    // take_indices 实现
    fn take_indices(&self, indices: &[usize]) -> AxionResult<Box<dyn SeriesTrait>> {
        let mut new_data = Vec::with_capacity(indices.len());
        for &idx in indices {
            let opt_val = self.data.get(idx)
                .ok_or_else(|| AxionError::IndexOutOfBounds(idx, self.len()))?
                .clone();
            new_data.push(opt_val);
        }
        Ok(Box::new(ListSeries::new(self.name.clone(), new_data, self.inner_dtype.clone())))
    }

    fn take_indices_option(&self, indices: &[Option<usize>]) -> AxionResult<Box<dyn SeriesTrait>> {
        let mut new_data = Vec::with_capacity(indices.len());
        for opt_idx in indices {
            match opt_idx {
                Some(idx) => {
                    // 获取 Option<Box<dyn SeriesTrait>> 并克隆
                    let opt_val = self.data.get(*idx)
                        .ok_or_else(|| AxionError::IndexOutOfBounds(*idx, self.len()))?
                        .clone();
                    new_data.push(opt_val);
                }
                None => {
                    // 索引为 None 时，插入 None 值
                    new_data.push(None);
                }
            }
        }
        Ok(Box::new(ListSeries::new(self.name.clone(), new_data, self.inner_dtype.clone())))
    }

    fn rename(&mut self, new_name: &str) {
        self.name = new_name.to_string();
    }

    /// Compares this ListSeries with another SeriesTrait object for equality.
    fn series_equal(&self, other: &dyn SeriesTrait) -> bool {
        // 1. 检查对方是否也是 ListSeries
        if let Some(other_list) = other.as_any().downcast_ref::<ListSeries>() {
            // 2. 检查内部类型是否一致
            if self.inner_dtype != other_list.inner_dtype {
                return false;
            }
            // 3. 检查长度是否一致
            if self.len() != other_list.len() {
                return false;
            }
            // 4. 逐一比较内部的 Option<Box<dyn SeriesTrait>>
            self.data.iter().zip(other_list.data.iter()).all(|(self_opt_box, other_opt_box)| {
                match (self_opt_box, other_opt_box) {
                    // 两个都是 None，相等
                    (None, None) => true,
                    // 两个都是 Some，递归比较内部的 Series
                    (Some(self_inner_series), Some(other_inner_series)) => {
                        // 调用内部 Series 的 series_equal 方法
                        self_inner_series.series_equal(&**other_inner_series)
                    }
                    // 一个 Some 一个 None，不相等
                    _ => false,
                }
            })
        } else {
            // 类型不匹配，肯定不相等
            false
        }
    }

    fn compare_row(&self, _a_idx: usize, _b_idx: usize) -> Ordering {
        // List 类型通常不支持直接比较行来进行排序
        // 返回 Equal 表示此列不影响排序顺序
        Ordering::Equal
        // 或者，如果你想禁止按 List 列排序，可以在 DataFrame::sort 中检查并返回错误
    }

    fn get_as_f64(&self, index: usize) -> AxionResult<Option<f64>> {
        let _index = index; // 这里的 index 是 ListSeries 的索引
        Ok(None)
    }    

    fn is_null_at(&self, index: usize) -> bool {
        // self.data is Vec<Option<T>>
        // If index is out of bounds, get returns None, map_or returns true (treat as null)
        // If index is valid, opt_val_ref is &Option<T>, is_none() checks if this Option<T> is None
        self.data.get(index).map_or(true, |opt_val_ref| opt_val_ref.is_none())
    }
}

// --- (可选) 实现 Debug 和 Display for ListSeries --- 
impl Debug for ListSeries {
     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
         f.debug_struct("ListSeries")
          .field("name", &self.name)
          .field("dtype", &self.dtype()) // 调用 dtype() 方法
          .field("len", &self.len())
          .field("data_head", &self.data.iter().take(5).collect::<Vec<_>>()) // 只显示前几个内部 Series 的 Debug
          .finish()
     }
}

impl Display for ListSeries {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "ListSeries: '{}' [{:?}]", self.name, self.dtype())?;
        for (i, opt_val) in self.data.iter().take(10).enumerate() {
            write!(f, "{}: ", i)?;
            match opt_val {
                Some(val) => writeln!(f, "{:?}", val)?, // 使用 Debug 打印内部 Series
                None => writeln!(f, "null")?,
            }
        }
        if self.len() > 10 {
            writeln!(f, "... ({} more)", self.len() - 10)?;
        }
        Ok(())
    }
}

/// 创建一个新的 ListSeries，并验证类型一致性
/// 
/// 该函数会检查所有输入 Series 是否具有相同的数据类型，
/// 如果类型不一致会返回错误。
/// 
/// # 参数
/// 
/// * `name` - Series 名称
/// * `data` - Series 数据向量
/// 
/// # 返回值
/// 
/// 成功时返回新创建的 ListSeries，失败时返回错误
/// 
/// # 错误
/// 
/// 当内部 Series 类型不一致时返回错误
pub fn new_list_series(name: String, data: Vec<Box<dyn SeriesTrait>>) -> AxionResult<ListSeries> {
    let inner_dtype = data.first()
        .map(|s| s.dtype())
        .unwrap_or(DataType::Null);

    for series in data.iter().skip(1) {
        if series.dtype() != inner_dtype {
            return Err(AxionError::Other(format!(
                "List series '{}' expects inner type {:?} but found {:?}",
                name, inner_dtype, series.dtype()
            )));
        }
    }

    let data_opts = data.into_iter().map(Some).collect();

    Ok(ListSeries::new(name, data_opts, inner_dtype))
}