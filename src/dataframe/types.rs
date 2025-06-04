use std::hash::{Hash, Hasher};
use std::fmt::Debug;

/// 表示分组键中的单个值
/// 
/// 在 GroupBy 操作中用于构建分组键，支持常用的可哈希和可比较的数据类型。
/// 
/// # 支持的类型
/// 
/// - `Int` - 32位整数
/// - `Str` - 字符串
/// - `Bool` - 布尔值
/// 
/// # 示例
/// 
/// ```rust
/// use axion::dataframe::types::GroupKeyValue;
/// 
/// let key1 = GroupKeyValue::Int(42);
/// let key2 = GroupKeyValue::Str("category".to_string());
/// let key3 = GroupKeyValue::Bool(true);
/// ```
#[derive(PartialEq, Eq, Clone, Debug)]
pub enum GroupKeyValue {
    Int(i32),
    Str(String),
    Bool(bool),
}

impl Hash for GroupKeyValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            GroupKeyValue::Int(i) => i.hash(state),
            GroupKeyValue::Str(s) => s.hash(state),
            GroupKeyValue::Bool(b) => b.hash(state),
        }
    }
}

/// 复合分组键类型
/// 
/// 当按多列分组时，使用此类型表示组合键。
/// 每个 `GroupKey` 是一个 `GroupKeyValue` 的向量，
/// 按分组列的顺序排列。
/// 
/// # 示例
/// 
/// ```rust
/// use axion::dataframe::types::{GroupKey, GroupKeyValue};
/// 
/// // 按 "类别" 和 "状态" 两列分组的键
/// let group_key: GroupKey = vec![
///     GroupKeyValue::Str("A类".to_string()),
///     GroupKeyValue::Bool(true),
/// ];
/// ```
pub type GroupKey = Vec<GroupKeyValue>;
