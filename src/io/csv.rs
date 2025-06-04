use rayon::iter::{IntoParallelIterator, ParallelIterator};

use crate::dataframe::DataFrame;
use crate::series::{Series, SeriesTrait};
use crate::AxionResult;
use crate::AxionError;
use crate::dtype::DataType;
use std::fs::File;
use std::path::Path;
use std::collections::{HashMap, HashSet};
use std::io::{BufReader, BufRead};

/// CSV 读取配置选项
/// 
/// 提供了丰富的 CSV 文件读取配置，支持自定义分隔符、数据类型推断、
/// 列选择等功能。
/// 
/// # 示例
/// 
/// ```rust
/// use axion::io::csv::{ReadCsvOptions, read_csv};
/// use axion::dtype::DataType;
/// use std::collections::HashMap;
/// 
/// // 使用默认配置
/// let df1 = read_csv("data.csv", None)?;
/// 
/// // 使用自定义配置
/// let options = ReadCsvOptions::builder()
///     .with_delimiter(b';')
///     .with_header(true)
///     .infer_schema(true)
///     .build();
/// let df2 = read_csv("data.csv", Some(options))?;
/// ```
#[derive(Debug, Clone)]
pub struct ReadCsvOptions {
    /// 字段分隔符，默认为 `,`
    pub delimiter: u8,
    /// CSV 文件是否包含表头行，默认为 `true`
    /// 如果为 `false`，列名将自动生成为 "column_0", "column_1", ...
    pub has_header: bool,
    /// 尝试推断列的数据类型，默认为 `true`
    /// 如果为 `false`，所有列将被读取为字符串
    pub infer_schema: bool,
    /// 用于类型推断的最大行数，默认为 `100`
    /// 如果为 `None`，则使用所有行进行推断
    pub infer_schema_length: Option<usize>,
    /// 可选的 HashMap，用于手动指定某些列的数据类型
    /// 手动指定的类型将覆盖类型推断的结果
    pub dtypes: Option<HashMap<String, DataType>>,
    /// 跳过文件开头的 N 行，默认为 `0`
    pub skip_rows: usize,
    /// 将以此字符开头的行视作注释并忽略，默认为 `None`
    pub comment_char: Option<u8>,
    /// 可选的列选择器，指定要读取的列名子集
    /// 如果为 `None`，则读取所有列
    pub use_columns: Option<Vec<String>>,
    /// 一组应被视为空值的字符串，默认为 `None`
    pub na_values: Option<HashSet<String>>,
}

impl Default for ReadCsvOptions {
    fn default() -> Self {
        ReadCsvOptions {
            delimiter: b',',
            has_header: true,
            infer_schema: true,
            infer_schema_length: Some(100),
            dtypes: None,
            skip_rows: 0,
            comment_char: None,
            use_columns: None,
            na_values: None,
        }
    }
}

impl ReadCsvOptions {
    /// 创建一个新的 ReadCsvOptions 构建器，使用默认值
    pub fn builder() -> ReadCsvOptionsBuilder {
        ReadCsvOptionsBuilder::new()
    }
}

/// ReadCsvOptions 的构建器
/// 
/// 提供了一种链式调用的方式来配置 CSV 读取选项。
/// 
/// # 示例
/// 
/// ```rust
/// let options = ReadCsvOptions::builder()
///     .with_delimiter(b';')
///     .with_header(true)
///     .skip_rows(2)
///     .build();
/// ```
#[derive(Debug, Clone, Default)]
pub struct ReadCsvOptionsBuilder {
    delimiter: Option<u8>,
    has_header: Option<bool>,
    infer_schema: Option<bool>,
    infer_schema_length: Option<Option<usize>>,
    dtypes: Option<HashMap<String, DataType>>,
    skip_rows: Option<usize>,
    comment_char: Option<Option<u8>>,
    use_columns: Option<Vec<String>>,
    na_values: Option<HashSet<String>>,
}

impl ReadCsvOptionsBuilder {
    /// 创建一个新的构建器实例
    pub fn new() -> Self {
        Default::default()
    }

    /// 设置字段分隔符
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = Some(delimiter);
        self
    }

    /// 设置是否包含表头行
    pub fn with_header(mut self, has_header: bool) -> Self {
        self.has_header = Some(has_header);
        self
    }

    /// 设置是否启用类型推断
    pub fn infer_schema(mut self, infer: bool) -> Self {
        self.infer_schema = Some(infer);
        self
    }

    /// 设置类型推断使用的行数
    pub fn infer_schema_length(mut self, length: Option<usize>) -> Self {
        self.infer_schema_length = Some(length);
        self
    }

    /// 设置列数据类型映射
    pub fn with_dtypes(mut self, dtypes: HashMap<String, DataType>) -> Self {
        self.dtypes = Some(dtypes);
        self
    }
    
    /// 添加单个列的数据类型
    pub fn add_dtype(mut self, column_name: String, dtype: DataType) -> Self {
        self.dtypes.get_or_insert_with(HashMap::new).insert(column_name, dtype);
        self
    }

    /// 设置跳过的行数
    pub fn skip_rows(mut self, n: usize) -> Self {
        self.skip_rows = Some(n);
        self
    }

    /// 设置注释字符
    pub fn comment_char(mut self, char_opt: Option<u8>) -> Self {
        self.comment_char = Some(char_opt);
        self
    }

    /// 设置要读取的列
    pub fn use_columns(mut self, columns: Vec<String>) -> Self {
        self.use_columns = Some(columns);
        self
    }

    /// 添加要读取的列
    pub fn add_use_column(mut self, column_name: String) -> Self {
        self.use_columns.get_or_insert_with(Vec::new).push(column_name);
        self
    }
    
    /// 设置 null 值表示
    pub fn na_values(mut self, values: Option<HashSet<String>>) -> Self {
        self.na_values = values;
        self
    }

    /// 添加 null 值表示
    pub fn add_na_value(mut self, value: String) -> Self {
        self.na_values
            .get_or_insert_with(HashSet::new)
            .insert(value);
        self
    }

    /// 构建最终的 `ReadCsvOptions` 实例
    pub fn build(self) -> ReadCsvOptions {
        let defaults = ReadCsvOptions::default();
        ReadCsvOptions {
            delimiter: self.delimiter.unwrap_or(defaults.delimiter),
            has_header: self.has_header.unwrap_or(defaults.has_header),
            infer_schema: self.infer_schema.unwrap_or(defaults.infer_schema),
            infer_schema_length: self.infer_schema_length.unwrap_or(defaults.infer_schema_length),
            dtypes: self.dtypes.or(defaults.dtypes),
            skip_rows: self.skip_rows.unwrap_or(defaults.skip_rows),
            comment_char: self.comment_char.unwrap_or(defaults.comment_char),
            use_columns: self.use_columns.or(defaults.use_columns),
            na_values: self.na_values.or(defaults.na_values),
        }
    }
}

/// 尝试解析字符串为 i64
fn try_parse_i64(s: &str) -> Option<i64> {
    s.parse::<i64>().ok()
}

/// 尝试解析字符串为 f64
fn try_parse_f64(s: &str) -> Option<f64> {
    s.parse::<f64>().ok()
}

/// 尝试解析字符串为布尔值
fn try_parse_bool(s: &str) -> Option<bool> {
    match s.to_lowercase().as_str() {
        "true" | "t" | "yes" | "y" | "1" => Some(true),
        "false" | "f" | "no" | "n" | "0" => Some(false),
        _ => None,
    }
}

/// 推断单列的数据类型
fn infer_column_type(
    column_values: &[Option<String>],
    infer_length: Option<usize>,
) -> DataType {
    let non_empty_values = column_values
        .iter()
        .filter_map(|opt_s| opt_s.as_ref().map(|s| s.as_str()))
        .filter(|s| !s.is_empty());

    let sample: Vec<&str> = if let Some(len) = infer_length {
        non_empty_values.take(len).collect()
    } else {
        non_empty_values.collect()
    };

    if sample.is_empty() {
        return DataType::String;
    }

    if sample.iter().all(|s| try_parse_i64(s).is_some()) {
        return DataType::Int64;
    }
    if sample.iter().all(|s| try_parse_f64(s).is_some()) {
        return DataType::Float64;
    }
    if sample.iter().all(|s| try_parse_bool(s).is_some()) {
        return DataType::Bool;
    }
    DataType::String
}

/// 将字符串列解析为指定类型的 Series
fn parse_column_as_type(
    column_name: String,
    string_data: Vec<Option<String>>,
    target_type: &DataType,
) -> AxionResult<Box<dyn SeriesTrait>> {
    match target_type {
        DataType::Int64 => {
            let parsed_data: Vec<Option<i64>> = string_data
                .into_iter()
                .map(|opt_s| opt_s.and_then(|s| try_parse_i64(&s)))
                .collect();
            Ok(Box::new(Series::<i64>::new_from_options(column_name, parsed_data)))
        }
        DataType::Float64 => {
            let parsed_data: Vec<Option<f64>> = string_data
                .into_iter()
                .map(|opt_s| opt_s.and_then(|s| try_parse_f64(&s)))
                .collect();
            Ok(Box::new(Series::<f64>::new_from_options(column_name, parsed_data)))
        }
        DataType::Bool => {
            let parsed_data: Vec<Option<bool>> = string_data
                .into_iter()
                .map(|opt_s| opt_s.and_then(|s| try_parse_bool(&s)))
                .collect();
            Ok(Box::new(Series::<bool>::new_from_options(column_name, parsed_data)))
        }
        DataType::String => {
            Ok(Box::new(Series::<String>::new_from_options(column_name, string_data)))
        }
        dt => Err(AxionError::UnsupportedOperation(format!(
            "无法将 CSV 列 '{}' 解析为类型 {:?}。CSV 解析仅支持 Int64、Float64、Bool、String 类型。",
            column_name, dt
        ))),
    }
}

/// 从 CSV 文件读取数据到 DataFrame
/// 
/// 支持自动类型推断、列选择、注释行处理等高级功能。
/// 
/// # 参数
/// 
/// * `filepath` - CSV 文件路径
/// * `options` - 可选的读取配置，如果为 None 则使用默认配置
/// 
/// # 返回值
/// 
/// 成功时返回包含 CSV 数据的 DataFrame
/// 
/// # 错误
/// 
/// * `AxionError::IoError` - 文件读取失败
/// * `AxionError::CsvError` - CSV 格式错误或解析失败
/// 
/// # 示例
/// 
/// ```rust
/// // 使用默认配置读取
/// let df = read_csv("data.csv", None)?;
/// 
/// // 使用自定义配置读取
/// let options = ReadCsvOptions::builder()
///     .with_delimiter(b';')
///     .infer_schema(true)
///     .build();
/// let df = read_csv("data.csv", Some(options))?;
/// ```
pub fn read_csv(filepath: impl AsRef<Path>, options: Option<ReadCsvOptions>) -> AxionResult<DataFrame> {
    let opts = options.unwrap_or_default();

    let file = File::open(filepath.as_ref())
        .map_err(|e| AxionError::IoError(format!("无法打开文件 {:?}: {}", filepath.as_ref(), e)))?;
    
    let mut buf_reader = BufReader::new(file);

    // 跳过指定行数
    if opts.skip_rows > 0 {
        let mut line_buffer = String::new();
        for i in 0..opts.skip_rows {
            match buf_reader.read_line(&mut line_buffer) {
                Ok(0) => {
                    return Err(AxionError::CsvError(format!(
                        "CSV 文件行数少于需要跳过的行数 {}，在第 {} 行到达文件末尾。",
                        opts.skip_rows, i
                    )));
                }
                Ok(_) => {
                    line_buffer.clear();
                }
                Err(e) => {
                    return Err(AxionError::IoError(format!("跳过行时出错: {}", e)));
                }
            }
        }
    }

    let mut rdr_builder = csv::ReaderBuilder::new();
    rdr_builder.delimiter(opts.delimiter);
    rdr_builder.has_headers(false);
    if let Some(comment) = opts.comment_char {
        rdr_builder.comment(Some(comment));
    }

    let rdr = rdr_builder.from_reader(buf_reader); 
    let mut records_iter = rdr.into_records();

    // 确定文件表头和第一行数据
    let original_file_headers: Vec<String>;
    let mut first_data_row_buffer: Option<csv::StringRecord> = None;

    if opts.has_header {
        if let Some(header_result) = records_iter.next() {
            original_file_headers = header_result
                .map_err(|e| AxionError::CsvError(format!("读取 CSV 表头失败: {}", e)))?
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>();
            if original_file_headers.is_empty() && !Path::new(filepath.as_ref()).metadata().map_or(true, |m| m.len() == 0) {
                 return Err(AxionError::CsvError("CSV 表头行存在但为空。".to_string()));
            }
        } else {
            return Ok(DataFrame::new_empty());
        }
    } else if let Some(first_record_result) = records_iter.next() {
        let record = first_record_result.map_err(|e| AxionError::CsvError(format!("读取第一条数据记录失败: {}", e)))?;
        if record.iter().all(|field| field.is_empty()) && !record.is_empty() { 
             original_file_headers = (0..record.len()).map(|i| format!("column_{}", i)).collect();
        } else if record.is_empty() { 
             return Ok(DataFrame::new_empty());
        } else {
            original_file_headers = (0..record.len()).map(|i| format!("column_{}", i)).collect();
        }
        first_data_row_buffer = Some(record); 
    } else {
        return Ok(DataFrame::new_empty());
    }
    if original_file_headers.is_empty() {
        return Ok(DataFrame::new_empty());
    }

    let (final_headers_to_use, column_indices_to_read): (Vec<String>, Vec<usize>) =
        if let Some(ref wanted_columns) = opts.use_columns {
            if wanted_columns.is_empty() { 
                (Vec::new(), Vec::new())
            } else {
                let mut final_h = Vec::new();
                let mut indices = Vec::new();
                let original_header_map: HashMap<&String, usize> = original_file_headers.iter().enumerate().map(|(i, h_name)| (h_name, i)).collect();

                for col_name_to_use in wanted_columns {
                    if let Some(&original_index) = original_header_map.get(col_name_to_use) {
                        final_h.push(col_name_to_use.clone());
                        indices.push(original_index);
                    } else {
                        return Err(AxionError::CsvError(format!(
                            "use_columns 中指定的列 '{}' 在 CSV 表头中未找到: {:?}",
                            col_name_to_use, original_file_headers
                        )));
                    }
                }
                (final_h, indices)
            }
        } else {
            (original_file_headers.clone(), (0..original_file_headers.len()).collect())
        };

    if final_headers_to_use.is_empty() {
        return Ok(DataFrame::new_empty());
    }

    let num_selected_columns = final_headers_to_use.len();
    let mut column_data_str: Vec<Vec<Option<String>>> = vec![Vec::new(); num_selected_columns];

    let process_record_logic = |record: &csv::StringRecord,
                                 col_data_storage: &mut Vec<Vec<Option<String>>>| -> AxionResult<()> {
        
        if opts.comment_char.is_some() && record.iter().all(|field| field.is_empty()) {
            return Ok(()); 
        }

        if record.len() != original_file_headers.len() {
            return Err(AxionError::CsvError(format!(
                "CSV 记录有 {} 个字段，但表头有 {} 个字段。记录: {:?}",
                record.len(),
                original_file_headers.len(),
                record
            )));
        }

        for (target_idx, &original_field_idx) in column_indices_to_read.iter().enumerate() {
            if let Some(field_str_val) = record.get(original_field_idx) {
                let is_user_defined_na = opts.na_values
                    .as_ref()
                    .is_some_and(|na_set| na_set.contains(field_str_val));

                if is_user_defined_na || field_str_val.is_empty() {
                    col_data_storage[target_idx].push(None);
                } else {
                    col_data_storage[target_idx].push(Some(field_str_val.to_string()));
                }
            } else {
                return Err(AxionError::CsvError(format!(
                    "内部错误或记录长度不一致: 尝试访问索引 {} 的字段，但记录只有 {} 个字段。",
                    original_field_idx, record.len()
                )));
            }
        }
        Ok(())
    };

    if let Some(ref record) = first_data_row_buffer {
        process_record_logic(record, &mut column_data_str)?
    }

    for result in records_iter { 
        match result {
            Ok(record) => {
                process_record_logic(&record, &mut column_data_str)?
            }
            Err(e) => {
                return Err(AxionError::CsvError(format!("读取 CSV 记录失败: {}", e)));
            }
        }
    }

    let mut data_to_process: Vec<(String, Vec<Option<String>>, DataType)> = Vec::with_capacity(num_selected_columns);

    for i in 0..num_selected_columns {
        let column_name = final_headers_to_use[i].clone();
        let current_column_str_data = std::mem::take(&mut column_data_str[i]); 

        let final_dtype = if let Some(ref manual_dtypes) = opts.dtypes {
            manual_dtypes.get(&column_name).cloned().unwrap_or_else(|| {
                if opts.infer_schema {
                    infer_column_type(&current_column_str_data, opts.infer_schema_length)
                } else {
                    DataType::String
                }
            })
        } else if opts.infer_schema {
            infer_column_type(&current_column_str_data, opts.infer_schema_length)
        } else {
            DataType::String
        };
        data_to_process.push((column_name, current_column_str_data, final_dtype));
    }

    let series_results: Vec<AxionResult<Box<dyn SeriesTrait>>> = data_to_process
        .into_par_iter() 
        .map(|(col_name, str_data, dtype)| {
            parse_column_as_type(col_name, str_data, &dtype)
        })
        .collect(); 

    let mut series_vec: Vec<Box<dyn SeriesTrait>> = Vec::with_capacity(num_selected_columns);
    for result in series_results {
        match result {
            Ok(series) => series_vec.push(series),
            Err(e) => return Err(e), 
        }
    }

    DataFrame::new(series_vec)
}

/// CSV 引用样式
/// 
/// 控制 CSV 写入时字段的引号使用策略。
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub enum QuoteStyle {
    /// 总是为所有字段加上引号
    Always,
    /// 仅在字段包含分隔符、引号或换行符时加上引号（默认）
    #[default]
    Necessary,
    /// 从不为字段加上引号（如果字段包含特殊字符，可能导致 CSV 格式无效）
    Never,
    /// 仅为非数字字段加上引号
    NonNumeric,
}

/// CSV 写入配置选项
/// 
/// 控制 DataFrame 导出为 CSV 文件时的格式设置。
/// 
/// # 示例
/// 
/// ```rust
/// use axion::io::csv::{WriteCsvOptions, QuoteStyle};
/// 
/// let options = WriteCsvOptions::builder()
///     .with_header(true)
///     .with_delimiter(b';')
///     .quote_style(QuoteStyle::Always)
///     .build();
/// ```
#[derive(Debug, Clone)]
pub struct WriteCsvOptions {
    /// 是否写入表头行，默认为 `true`
    pub has_header: bool,
    /// 字段分隔符，默认为 `,`
    pub delimiter: u8,
    /// 用于表示 null 值的字符串，默认为空字符串 `""`
    pub na_rep: String,
    /// 字段的引用样式，默认为 `QuoteStyle::Necessary`
    pub quote_style: QuoteStyle,
    /// 行终止符，默认为 `\n`
    pub line_terminator: String,
}

impl Default for WriteCsvOptions {
    fn default() -> Self {
        WriteCsvOptions {
            has_header: true,
            delimiter: b',',
            na_rep: "".to_string(),
            quote_style: QuoteStyle::default(),
            line_terminator: "\n".to_string(),
        }
    }
}

impl WriteCsvOptions {
    /// 创建一个新的 WriteCsvOptions 构建器，使用默认值
    pub fn builder() -> WriteCsvOptionsBuilder {
        WriteCsvOptionsBuilder::new()
    }
}

/// WriteCsvOptions 的构建器
/// 
/// 提供了一种链式调用的方式来配置 CSV 写入选项。
#[derive(Debug, Clone, Default)]
pub struct WriteCsvOptionsBuilder {
    has_header: Option<bool>,
    delimiter: Option<u8>,
    na_rep: Option<String>,
    quote_style: Option<QuoteStyle>,
    line_terminator: Option<String>,
}

impl WriteCsvOptionsBuilder {
    /// 创建一个新的构建器实例
    pub fn new() -> Self {
        Default::default()
    }

    /// 设置是否写入表头行
    pub fn with_header(mut self, has_header: bool) -> Self {
        self.has_header = Some(has_header);
        self
    }

    /// 设置字段分隔符
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = Some(delimiter);
        self
    }

    /// 设置用于表示 null 值的字符串
    pub fn na_representation(mut self, na_rep: String) -> Self {
        self.na_rep = Some(na_rep);
        self
    }

    /// 设置字段的引用样式
    pub fn quote_style(mut self, style: QuoteStyle) -> Self {
        self.quote_style = Some(style);
        self
    }

    /// 设置行终止符
    /// 
    /// 例如：`"\n"` (LF), `"\r\n"` (CRLF)
    pub fn line_terminator(mut self, terminator: String) -> Self {
        self.line_terminator = Some(terminator);
        self
    }

    /// 构建最终的 WriteCsvOptions 实例
    /// 
    /// 未在构建器中设置的字段将使用默认值
    pub fn build(self) -> WriteCsvOptions {
        let defaults = WriteCsvOptions::default();
        WriteCsvOptions {
            has_header: self.has_header.unwrap_or(defaults.has_header),
            delimiter: self.delimiter.unwrap_or(defaults.delimiter),
            na_rep: self.na_rep.unwrap_or(defaults.na_rep),
            quote_style: self.quote_style.unwrap_or(defaults.quote_style),
            line_terminator: self.line_terminator.unwrap_or(defaults.line_terminator),
        }
    }
}
