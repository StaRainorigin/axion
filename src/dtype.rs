use std::cmp::Ordering;
use std::fmt::Debug;
use serde::{Deserialize, Serialize};

/// 数据类型枚举
/// 
/// 定义了 Axion 库支持的所有数据类型，包括基础类型和复合类型。
/// 支持序列化、反序列化以及类型比较和排序。
/// 
/// # 类型层次
/// 
/// - **Null** - 空值类型
/// - **Bool** - 布尔类型
/// - **整数类型** - Int8, Int16, Int32, Int64, Int128
/// - **无符号整数类型** - UInt8, UInt16, UInt32, UInt64, UInt128
/// - **浮点类型** - Float32, Float64
/// - **字符串类型** - String
/// - **复合类型** - List(内部类型)
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    /// 空值类型
    Null,
    /// 布尔类型
    Bool,
    /// 8位有符号整数
    Int8,
    /// 16位有符号整数
    Int16,
    /// 32位有符号整数
    Int32,
    /// 64位有符号整数
    Int64,
    /// 128位有符号整数
    Int128,
    /// 8位无符号整数
    UInt8,
    /// 16位无符号整数
    UInt16,
    /// 32位无符号整数
    UInt32,
    /// 64位无符号整数
    UInt64,
    /// 128位无符号整数
    UInt128,
    /// 32位浮点数
    Float32,
    /// 64位浮点数
    Float64,
    /// 字符串类型
    String,
    /// 列表类型，包含内部元素的数据类型
    List(Box<DataType>),
}

impl DataType {
    /// 检查数据类型是否为浮点类型
    pub fn is_float(&self) -> bool {
        matches!(self, DataType::Float32 | DataType::Float64)
    }

    /// 检查数据类型是否为整数类型
    pub fn is_integer(&self) -> bool {
        matches!(
            self,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 | DataType::Int128 |
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 | DataType::UInt128
        )
    }

    /// 检查数据类型是否为数值类型（整数或浮点）
    pub fn is_numeric(&self) -> bool {
        matches!(
            self,
            DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 |
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 |
            DataType::Float32 | DataType::Float64
        )
    }
}

impl Ord for DataType {
    fn cmp(&self, other: &Self) -> Ordering {
        fn order_index(dt: &DataType) -> i32 {
            match dt {
                DataType::Null => 0,
                DataType::Bool => 1,
                DataType::Int8 => 10,
                DataType::Int16 => 11,
                DataType::Int32 => 12,
                DataType::Int64 => 13,
                DataType::Int128 => 14,
                DataType::UInt8 => 20,
                DataType::UInt16 => 21,
                DataType::UInt32 => 22,
                DataType::UInt64 => 23,
                DataType::UInt128 => 24,
                DataType::Float32 => 30,
                DataType::Float64 => 31,
                DataType::String => 40,
                DataType::List(_) => 100,
            }
        }

        match order_index(self).cmp(&order_index(other)) {
            Ordering::Equal => {
                match (self, other) {
                    (DataType::List(a), DataType::List(b)) => a.cmp(b),
                    _ => Ordering::Equal,
                }
            }
            order => order,
        }
    }
}

impl PartialOrd for DataType {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// 数据类型特征
/// 
/// 为 Rust 原生类型与 Axion DataType 枚举之间提供映射关系。
/// 所有可以存储在 Series 中的类型都必须实现此特征。
/// 
/// # 要求
/// 
/// 实现此特征的类型必须满足：
/// - `Debug` + `Clone` - 用于调试和克隆
/// - `Send` + `Sync` - 用于多线程安全
/// - `'static` - 确保类型生命周期足够长
pub trait DataTypeTrait: Debug + Clone + Send + Sync + 'static {
    /// 与此类型关联的 DataType 枚举变体
    const DTYPE: DataType;

    /// 获取实例的 DataType
    /// 
    /// 通常直接返回 `DTYPE` 常量
    fn as_dtype(&self) -> DataType;
}

/// 为基础类型实现 DataTypeTrait 的宏
macro_rules! impl_datatype_trait {
    ($prim_type: ty, $dtype_variant: ident) => {
        impl DataTypeTrait for $prim_type where $prim_type: Debug + Clone {
            const DTYPE: DataType = DataType::$dtype_variant;

            fn as_dtype(&self) -> DataType {
                Self::DTYPE
            }
        }
    };
}

// 为整数类型实现 DataTypeTrait
impl_datatype_trait!(i8, Int8);
impl_datatype_trait!(i16, Int16);
impl_datatype_trait!(i32, Int32);
impl_datatype_trait!(i64, Int64);
impl_datatype_trait!(u8, UInt8);
impl_datatype_trait!(u16, UInt16);
impl_datatype_trait!(u32, UInt32);
impl_datatype_trait!(u64, UInt64);

// 为浮点类型实现 DataTypeTrait
impl_datatype_trait!(f32, Float32);
impl_datatype_trait!(f64, Float64);

// 为布尔类型实现 DataTypeTrait
impl_datatype_trait!(bool, Bool);

// 为字符串类型手动实现 DataTypeTrait
impl DataTypeTrait for String {
    const DTYPE: DataType = DataType::String;

    fn as_dtype(&self) -> DataType {
        Self::DTYPE
    }
}
