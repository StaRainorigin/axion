use crate::dtype::DataType;
use std::fmt::{self, Display};
use std::error::Error;

/// Axion 操作中可能发生的错误类型
/// 
/// 涵盖了数据处理、类型转换、IO 操作等各种场景下可能出现的错误情况。
/// 提供了详细的错误信息以便于调试和错误处理。
#[derive(Debug)]
pub enum AxionError {
    /// DataFrame 列长度不一致错误
    /// 
    /// 当尝试创建 DataFrame 时，如果不同列的长度不匹配会触发此错误
    MismatchedLengths { 
        /// 期望的长度
        expected: usize, 
        /// 实际发现的长度
        found: usize, 
        /// 相关的列名或操作名
        name: String 
    },
    
    /// 重复列名错误
    /// 
    /// 当尝试创建包含重复列名的 DataFrame 时触发
    DuplicateColumnName(String),
    
    /// 列不存在错误
    /// 
    /// 当按名称或索引选择列时，指定的列不存在
    ColumnNotFound(String),
    
    /// 类型错误
    /// 
    /// 当尝试对列执行操作但类型不匹配时触发
    TypeError { 
        /// 期望的类型描述
        expected: String, 
        /// 实际发现的类型
        found: DataType, 
        /// 相关的列名
        name: String 
    },
    
    /// 类型不匹配错误
    /// 
    /// 当尝试向下转型列时，类型不匹配
    TypeMismatch { 
        /// 期望的数据类型
        expected: DataType, 
        /// 实际发现的数据类型
        found: DataType, 
        /// 相关的列名
        name: String 
    },
    
    /// 未提供列错误
    /// 
    /// 当尝试创建 DataFrame 时没有提供任何列
    NoColumnsProvided,
    
    /// 类型转换错误
    CastError(CastError),
    
    /// Join 操作键列类型错误
    JoinKeyTypeError {
        /// 哪一侧的表（"left" 或 "right"）
        side: String,
        /// 键列名称
        name: String,
        /// 期望的数据类型
        expected: DataType,
        /// 实际发现的数据类型
        found: DataType,
    },
    
    /// 索引越界错误
    /// 
    /// 当访问的索引超出有效范围时触发
    IndexOutOfBounds(usize, usize), // (访问的索引, 集合长度)
    
    /// 索引超出范围错误（别名，用于向后兼容）
    IndexOutOfRange(usize, usize), // (访问的索引, 集合长度)
    
    /// 计算错误
    /// 
    /// 在数学计算过程中发生的错误（如除零）
    ComputeError(String),
    
    /// 不支持的操作错误
    /// 
    /// 当在不支持的类型上执行特定操作时触发
    UnsupportedOperation(String),
    
    /// 无效参数错误
    /// 
    /// 当提供的参数不符合要求时触发
    InvalidArgument(String),
    
    /// 内部错误
    /// 
    /// 表示意外的内部状态或逻辑错误，通常表示代码中存在 bug
    InternalError(String),
    
    /// CSV 处理错误
    /// 
    /// 在读取或写入 CSV 文件时发生的错误
    CsvError(String),
    
    /// IO 错误
    /// 
    /// 文件输入输出相关的错误
    IoError(String),
    
    /// 其他未分类错误
    Other(String),
}

/// Series 类型转换错误
/// 
/// 当 Series 之间进行类型转换失败时产生的错误。
#[derive(Debug)]
pub struct CastError(pub String);

impl Display for CastError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Cast error: {}", self.0)
    }
}

impl Error for CastError {}

impl Display for AxionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AxionError::MismatchedLengths { expected, found, name } => write!(
                f,
                "列长度不匹配: 列 '{}' 期望长度 {}，但发现长度 {}",
                name, expected, found
            ),
            AxionError::DuplicateColumnName(name) => {
                write!(f, "发现重复的列名: '{}'", name)
            }
            AxionError::ColumnNotFound(name) => write!(f, "未找到列: '{}'", name),
            AxionError::TypeError { expected, found, name } => write!(
                f,
                "列 '{}' 类型错误: 期望 {}，发现 {:?}",
                name, expected, found
            ),
            AxionError::TypeMismatch { expected, found, name } => write!(
                f,
                "列 '{}' 类型不匹配: 期望 {:?}，发现 {:?}",
                name, expected, found
            ),
            AxionError::NoColumnsProvided => write!(f, "创建 DataFrame 时未提供任何列"),
            AxionError::CastError(err) => write!(f, "{}", err),
            AxionError::JoinKeyTypeError { side, name, expected, found } => write!(
                f,
                "{} 表的连接键列 '{}' 类型无效: 期望 {:?}，发现 {:?}",
                side, name, expected, found
            ),
            AxionError::IndexOutOfBounds(index, len) => write!(
                f,
                "索引越界: 集合长度为 {}，但访问索引为 {}",
                len, index
            ),
            AxionError::IndexOutOfRange(index, len) => write!(
                f,
                "索引超出范围: 集合长度为 {}，但访问索引为 {}",
                len, index
            ),
            AxionError::ComputeError(msg) => write!(f, "计算错误: {}", msg),
            AxionError::UnsupportedOperation(msg) => write!(f, "不支持的操作: {}", msg),
            AxionError::InvalidArgument(msg) => write!(f, "无效参数: {}", msg),
            AxionError::InternalError(msg) => write!(f, "内部错误: {}，请报告此问题", msg),
            AxionError::CsvError(msg) => write!(f, "CSV 错误: {}", msg),
            AxionError::IoError(msg) => write!(f, "IO 错误: {}", msg),
            AxionError::Other(msg) => write!(f, "Axion 错误: {}", msg),
        }
    }
}

impl Error for AxionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            AxionError::CastError(err) => Some(err),
            _ => None,
        }
    }
}

/// Axion 库中常用的 Result 类型别名
/// 
/// 简化错误处理代码，统一使用 `AxionResult<T>` 而不是完整的 `Result<T, AxionError>`。
/// 
/// # 示例
/// 
/// ```rust
/// fn process_data() -> AxionResult<DataFrame> {
///     // 处理逻辑
///     Ok(dataframe)
/// }
/// ```
pub type AxionResult<T> = std::result::Result<T, AxionError>;

impl From<csv::Error> for AxionError {
    fn from(err: csv::Error) -> Self {
        AxionError::CsvError(format!("CSV 处理错误: {}", err))
    }
}

impl From<std::string::FromUtf8Error> for AxionError {
    fn from(err: std::string::FromUtf8Error) -> Self {
        AxionError::IoError(format!("UTF-8 转换错误: {}", err))
    }
}

impl From<std::io::Error> for AxionError {
    fn from(err: std::io::Error) -> Self {
        AxionError::IoError(err.to_string())
    }
}
