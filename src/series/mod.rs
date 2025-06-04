//! Series 模块
//! 
//! 提供了 Axion 数据处理库的核心 Series 数据结构和相关功能。
//! 
//! # 模块组织
//! 
//! - `core` - Series 核心实现
//! - `interface` - Series trait 定义
//! - `list` - 列表类型 Series 实现
//! - `ops` - Series 操作 trait 定义
//! - `string` - 字符串操作扩展

pub mod core;
pub mod interface;
pub mod list;
pub mod ops;
pub mod string;

// 重新导出核心类型和 trait
pub use self::core::{
    Series,
    SeriesFlags,
    IntoSeriesData,
    IntoSeriesBox,
};

pub use self::interface::SeriesTrait;

pub use self::list::{
    ListSeries,
    new_list_series,
};

pub use self::ops::{
    SeriesCompareScalar,
    SeriesCompareSeries, 
    SeriesCompare,
    SeriesArithScalar,
    SeriesArithSeries,
};
