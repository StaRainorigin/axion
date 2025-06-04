use super::core::Series;
use crate::error::AxionResult;
use crate::dtype::DataType;

/// 字符串 Series 的专用操作访问器
/// 
/// 提供了专门针对 `Series<String>` 的字符串处理方法，
/// 包括模式匹配、大小写转换、空白字符处理等功能。
/// 
/// # 使用方式
/// 
/// 通过 `Series<String>` 的 `.str()` 方法获取该访问器实例。
/// 
/// # 示例
/// 
/// ```rust
/// let s = Series::new("names".to_string(), vec!["Alice", "Bob", "Charlie"]);
/// let contains_result = s.str().contains("a")?;
/// let uppercase_result = s.str().to_uppercase()?;
/// ```
pub struct StringAccessor<'a> {
    series: &'a Series<String>,
}

impl<'a> StringAccessor<'a> {
    /// 创建新的 StringAccessor 实例
    /// 
    /// # 参数
    /// 
    /// * `series` - 字符串类型的 Series 引用
    /// 
    /// # Panics
    /// 
    /// 如果 Series 的数据类型不是 String 则会 panic
    pub(super) fn new(series: &'a Series<String>) -> Self {
        assert_eq!(series.dtype(), DataType::String, "StringAccessor can only be used on Series<String>");
        Self { series }
    }

    /// 检查元素是否包含指定模式
    /// 
    /// # 参数
    /// 
    /// * `pattern` - 要搜索的字符串模式
    /// 
    /// # 返回值
    /// 
    /// 返回新的布尔类型 Series，null 值保持为 null
    pub fn contains(&self, pattern: &str) -> AxionResult<Series<bool>> {
        let new_name = format!("{}_contains_{}", self.series.name(), pattern);
        if self.series.is_empty() {
            return Ok(Series::new_empty(new_name, DataType::Bool));
        }
        let new_data: Vec<Option<bool>> = self.series.data.iter()
            .map(|opt_s| opt_s.as_ref().map(|s| s.contains(pattern)))
            .collect();
        Ok(Series::new_from_options(new_name, new_data))
    }

    /// 检查元素是否以指定模式开头
    /// 
    /// # 参数
    /// 
    /// * `pattern` - 要检查的前缀模式
    /// 
    /// # 返回值
    /// 
    /// 返回新的布尔类型 Series，null 值保持为 null
    pub fn startswith(&self, pattern: &str) -> AxionResult<Series<bool>> {
        let new_name = format!("{}_startswith_{}", self.series.name(), pattern);
        if self.series.is_empty() {
            return Ok(Series::new_empty(new_name, DataType::Bool));
        }
        let new_data: Vec<Option<bool>> = self.series.data.iter()
            .map(|opt_s| opt_s.as_ref().map(|s| s.starts_with(pattern)))
            .collect();
        Ok(Series::new_from_options(new_name, new_data))
    }

    /// 检查元素是否以指定模式结尾
    /// 
    /// # 参数
    /// 
    /// * `pattern` - 要检查的后缀模式
    /// 
    /// # 返回值
    /// 
    /// 返回新的布尔类型 Series，null 值保持为 null
    pub fn endswith(&self, pattern: &str) -> AxionResult<Series<bool>> {
        let new_name = format!("{}_endswith_{}", self.series.name(), pattern);
        if self.series.is_empty() {
            return Ok(Series::new_empty(new_name, DataType::Bool));
        }
        let new_data: Vec<Option<bool>> = self.series.data.iter()
            .map(|opt_s| opt_s.as_ref().map(|s| s.ends_with(pattern)))
            .collect();
        Ok(Series::new_from_options(new_name, new_data))
    }

    /// 获取每个字符串元素的长度（字节数）
    /// 
    /// # 返回值
    /// 
    /// 返回新的 u32 类型 Series，null 值保持为 null
    pub fn str_len(&self) -> AxionResult<Series<u32>> {
        let new_name = format!("{}_len", self.series.name());
        if self.series.is_empty() {
            return Ok(Series::new_empty(new_name, DataType::UInt32));
        }
        let new_data: Vec<Option<u32>> = self.series.data.iter()
            .map(|opt_s| opt_s.as_ref().map(|s| s.len() as u32))
            .collect();
        Ok(Series::new_from_options(new_name, new_data))
    }

    /// 替换所有匹配的模式
    /// 
    /// # 参数
    /// 
    /// * `from` - 要替换的模式
    /// * `to` - 替换后的字符串
    /// 
    /// # 返回值
    /// 
    /// 返回新的字符串 Series，null 值保持为 null
    pub fn replace(&self, from: &str, to: &str) -> AxionResult<Series<String>> {
        let new_name = format!("{}_replace", self.series.name());
        if self.series.is_empty() {
            return Ok(Series::new_empty(new_name, DataType::String));
        }
        let new_data: Vec<Option<String>> = self.series.data.iter()
            .map(|opt_s| opt_s.as_ref().map(|s| s.replace(from, to)))
            .collect();
        Ok(Series::new_from_options(new_name, new_data))
    }

    /// 转换为小写
    /// 
    /// # 返回值
    /// 
    /// 返回新的字符串 Series，所有字符转换为小写，null 值保持为 null
    pub fn to_lowercase(&self) -> AxionResult<Series<String>> {
        let new_name = format!("{}_lower", self.series.name());
        if self.series.is_empty() {
            return Ok(Series::new_empty(new_name, DataType::String));
        }
        let new_data: Vec<Option<String>> = self.series.data.iter()
            .map(|opt_s| opt_s.as_ref().map(|s| s.to_lowercase()))
            .collect();
        Ok(Series::new_from_options(new_name, new_data))
    }

    /// 转换为大写
    /// 
    /// # 返回值
    /// 
    /// 返回新的字符串 Series，所有字符转换为大写，null 值保持为 null
    pub fn to_uppercase(&self) -> AxionResult<Series<String>> {
        let new_name = format!("{}_upper", self.series.name());
        if self.series.is_empty() {
            return Ok(Series::new_empty(new_name, DataType::String));
        }
        let new_data: Vec<Option<String>> = self.series.data.iter()
            .map(|opt_s| opt_s.as_ref().map(|s| s.to_uppercase()))
            .collect();
        Ok(Series::new_from_options(new_name, new_data))
    }

    /// 去除首尾空白字符
    /// 
    /// # 返回值
    /// 
    /// 返回新的字符串 Series，去除每个字符串的首尾空白字符，null 值保持为 null
    pub fn strip(&self) -> AxionResult<Series<String>> {
        let new_name = format!("{}_strip", self.series.name());
        if self.series.is_empty() {
            return Ok(Series::new_empty(new_name, DataType::String));
        }
        let new_data: Vec<Option<String>> = self.series.data.iter()
            .map(|opt_s| opt_s.as_ref().map(|s| s.trim().to_string()))
            .collect();
        Ok(Series::new_from_options(new_name, new_data))
    }

    /// 去除开头的空白字符
    /// 
    /// # 返回值
    /// 
    /// 返回新的字符串 Series，去除每个字符串开头的空白字符，null 值保持为 null
    pub fn lstrip(&self) -> AxionResult<Series<String>> {
        let new_name = format!("{}_lstrip", self.series.name());
        if self.series.is_empty() {
            return Ok(Series::new_empty(new_name, DataType::String));
        }
        let new_data: Vec<Option<String>> = self.series.data.iter()
            .map(|opt_s| opt_s.as_ref().map(|s| s.trim_start().to_string()))
            .collect();
        Ok(Series::new_from_options(new_name, new_data))
    }

    /// 去除末尾的空白字符
    /// 
    /// # 返回值
    /// 
    /// 返回新的字符串 Series，去除每个字符串末尾的空白字符，null 值保持为 null
    pub fn rstrip(&self) -> AxionResult<Series<String>> {
        let new_name = format!("{}_rstrip", self.series.name());
        if self.series.is_empty() {
            return Ok(Series::new_empty(new_name, DataType::String));
        }
        let new_data: Vec<Option<String>> = self.series.data.iter()
            .map(|opt_s| opt_s.as_ref().map(|s| s.trim_end().to_string()))
            .collect();
        Ok(Series::new_from_options(new_name, new_data))
    }
}