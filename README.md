# Axion

一个高性能的 Rust 数据处理库，提供类似 Pandas 的数据操作体验，专为大规模数据分析和并行计算而设计。

## 🖥️ 开发环境

### 硬件环境
- **处理器**:` AMD R9 5900HX (x86_64)`
- **系统**: `Ubuntu 22.04 LTS (WSL2)`

### 软件环境
- **Rust版本**: `rustc 1.88.0-nightly`
- **包管理**: `cargo 1.88.0-nightly`
- **代码检查**: `clippy 0.1.87`
- **版本控制**: `Git`

### 开发工具
- **编辑器**: `VSCode 1.99.1`
- **主要插件**: 
  - `rust-analyzer v0.3.2370`
  - `CodeLLDB v1.11.4`

## 🚀 系统部署

### 1. Rust 环境安装

#### 在 Linux/macOS 上安装
```bash
# 安装 Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# 重新加载环境变量
source ~/.bashrc
# 或者
source ~/.zshrc

# 验证安装
rustc --version
cargo --version
```

#### 在 Windows 上安装
1. 访问 [https://rustup.rs/](https://rustup.rs/)
2. 下载并运行 `rustup-init.exe`
3. 按照安装向导完成安装
4. 重启命令提示符或PowerShell

#### 安装 nightly 版本（推荐）
```bash
# 安装 nightly 工具链
rustup install nightly

# 设置为默认版本
rustup default nightly

# 验证版本
rustc --version
```

### 2. VSCode 开发环境配置

#### 安装 VSCode
- **Linux**: `sudo snap install code --classic`
- **macOS**: 从 [VSCode官网](https://code.visualstudio.com/) 下载
- **Windows**: 从 [VSCode官网](https://code.visualstudio.com/) 下载

#### 安装必需插件
在 VSCode 中安装以下插件：

1. **rust-analyzer** (推荐)
   ```
   扩展ID: rust-lang.rust-analyzer
   功能: 代码补全、语法检查、重构等
   ```

2. **CodeLLDB** (调试支持)
   ```
   扩展ID: vadimcn.vscode-lldb
   功能: Rust 程序调试
   ```

3. **Even Better TOML** (可选)
   ```
   扩展ID: tamasfe.even-better-toml
   功能: Cargo.toml 文件支持
   ```

#### VSCode 配置
在项目根目录创建 `.vscode/settings.json`:
```json
{
    "rust-analyzer.cargo.features": "all",
    "rust-analyzer.checkOnSave.command": "clippy",
    "rust-analyzer.checkOnSave.extraArgs": ["--all-targets"],
    "rust-analyzer.cargo.loadOutDirsFromCheck": true
}
```

### 3. 开发工具安装

```bash
# 安装代码格式化工具
rustup component add rustfmt

# 安装代码检查工具
rustup component add clippy

# 验证工具安装
cargo fmt --version
cargo clippy --version
```

## 📦 使用说明

### 在项目中集成 Axion

#### 1. 创建新项目
```bash
# 创建新的 Rust 项目
cargo new my_data_project
cd my_data_project
```

#### 2. 添加依赖
在 `Cargo.toml` 文件中添加 Axion 依赖：

```toml
[dependencies]
axion-data = "0.1.0"
```

或使用命令行添加：
```bash
cargo add axion-data
```

#### 3. 导入和使用
在 `src/main.rs` 中：

```rust
use axion_data::*;

fn main() -> AxionResult<()> {
    // 创建 DataFrame
    let df = df![
        "name" => &["Alice", "Bob", "Charlie"],
        "age" => &[25, 30, 35],
        "salary" => &[50000.0, 60000.0, 70000.0]
    ]?;
    
    println!("DataFrame 形状: {:?}", df.shape());
    println!("DataFrame 内容:\n{}", df);
    
    // 数据过滤
    let age_col: &Series<i32> = df.downcast_column("age")?;
    let mask = age_col.gt(28)?;
    let filtered = df.filter(&mask)?;
    
    println!("年龄 > 28 的员工:\n{}", filtered);
    
    Ok(())
}
```

#### 4. 运行项目
```bash
# 编译并运行
cargo run

# 运行测试
cargo test

# 运行优化版本
cargo run --release
```

### 示例：完整的数据分析流程

```rust
use axion_data::*;

fn analyze_sales_data() -> AxionResult<()> {
    // 1. 读取数据
    let df = read_csv("data/sales.csv", None)?;
    
    // 2. 数据概览
    println!("数据形状: {:?}", df.shape());
    println!("前5行:\n{}", df.head(5));
    
    // 3. 数据清洗
    let sales_col: &Series<f64> = df.downcast_column("sales")?;
    let cleaned_df = df.filter(&sales_col.not_null())?;
    
    // 4. 分组分析
    let grouped = cleaned_df.groupby(&["region"])?;
    let summary = grouped.sum()?;
    
    println!("按地区汇总:\n{}", summary);
    
    // 5. 保存结果
    summary.to_csv("output/sales_summary.csv", None)?;
    
    Ok(())
}
```

## 🛠️ 快速开始

### 创建 Series

```rust
use axion_data::*;

// 从向量创建 Series
let s1 = Series::new("numbers".to_string(), vec![1, 2, 3, 4, 5]);

// 从数组创建 Series
let s2 = Series::new("names".to_string(), vec!["Alice", "Bob", "Charlie"]);

// 包含空值的 Series
let s3 = Series::new_from_options("values".to_string(), vec![
    Some(10), None, Some(20), Some(30)
]);
```

### 基本操作

```rust
// 获取长度
println!("长度: {}", s1.len());

// 访问元素
if let Some(value) = s1.get(0) {
    println!("第一个元素: {}", value);
}

// 迭代器
for value in s1.iter_valid() {
    println!("值: {}", value);
}
```

### 数学运算

```rust
// Series 间运算
let s1 = Series::new("a".to_string(), vec![1, 2, 3]);
let s2 = Series::new("b".to_string(), vec![4, 5, 6]);
let result = &s1 + &s2;  // [5, 7, 9]

// 与标量运算
let scaled = &s1 * 2;    // [2, 4, 6]
```

### 比较操作

```rust
let s = Series::new("data".to_string(), vec![1, 2, 3, 4, 5]);

// 与标量比较
let mask = s.gt(3).unwrap();  // [false, false, false, true, true]

// 过滤数据
let filtered = s.filter(&mask);
```

### 聚合函数

```rust
let s = Series::new("values".to_string(), vec![1.0, 2.0, 3.0, 4.0, 5.0]);

println!("总和: {:?}", s.sum());      // Some(15.0)
println!("平均值: {:?}", s.mean());    // Some(3.0)
println!("最小值: {:?}", s.min());      // Some(1.0)
println!("最大值: {:?}", s.max());      // Some(5.0)
```

### 空值处理

```rust
let s = Series::new_from_options("data".to_string(), vec![
    Some(1), None, Some(3), None, Some(5)
]);

// 检查空值
let null_mask = s.is_null();

// 填充空值
let filled = s.fill_null(0);

// 只处理有效值
for value in s.iter_valid() {
    println!("有效值: {}", value);
}
```

### 字符串操作

```rust
let s = Series::new("text".to_string(), vec![
    "hello".to_string(),
    "world".to_string(),
    "rust".to_string()
]);

// 字符串访问器
let lengths = s.str().str_len()?;        // 字符串长度
let upper = s.str().to_uppercase()?;     // 转大写
let contains = s.str().contains("o")?;   // 包含检查
```

### 函数式编程

```rust
let s = Series::new("numbers".to_string(), vec![1, 2, 3, 4, 5]);

// 映射操作
let doubled = s.apply(|opt_val| {
    opt_val.map(|x| x * 2)
});

// 并行处理
let processed = s.par_apply(|opt_val| {
    opt_val.map(|x| x.pow(2))
});
```

## 📊 DataFrame 支持

```rust
use axion_data::*;

// 使用宏创建 DataFrame
let df = df![
    "name" => &["Alice", "Bob", "Charlie"],
    "age" => &[25, 30, 35],
    "salary" => &[50000.0, 60000.0, 70000.0]
]?;

// 选择列
let selected = df.select(&["name", "age"])?;

// 过滤行
let age_col: &Series<i32> = df.downcast_column("age")?;
let mask = age_col.gt(28)?;
let filtered = df.filter(&mask)?;

// 分组操作
let grouped = df.groupby(&["department"])?;
let summary = grouped.mean()?;

// 连接操作
let joined = df1.inner_join(&df2, "id", "id")?;

// 显示 DataFrame
println!("DataFrame:\n{}", df);
```

### CSV 文件操作

```rust
// 读取 CSV
let df = read_csv("data/sample.csv", None)?;

// 带选项读取
let options = ReadCsvOptions::builder()
    .with_header(true)
    .skip_rows(1)
    .use_columns(vec!["name".to_string(), "age".to_string()])
    .build();
let df = read_csv("data/sample.csv", Some(options))?;

// 写入 CSV
df.to_csv("output/result.csv", None)?;
```

### 排序

```rust
let mut s = Series::new("data".to_string(), vec![3, 1, 4, 1, 5]);
s.sort(false);  // 升序排序
println!("{:?}", s.data);  // [Some(1), Some(1), Some(3), Some(4), Some(5)]

// DataFrame 排序
let sorted_df = df.sort(&["age", "salary"], &[false, true])?; // 年龄升序，工资降序
```

## 🧪 测试

运行测试套件：

```bash
# 运行特定模块测试
cargo test series::
```

运行基准测试：

```bash
# 运行基准测试
cargo bench
```

## 🤝 贡献

欢迎贡献代码！请查看我们的贡献指南：

1. Fork 项目
2. 创建特性分支 (`git checkout -b feature/amazing-feature`)
3. 提交更改 (`git commit -m 'Add amazing feature'`)
4. 推送到分支 (`git push origin feature/amazing-feature`)
5. 开启 Pull Request

### 开发建议
- 遵循 Rust 代码规范
- 为新功能添加测试用例
- 更新相关文档
- 运行 `cargo clippy` 检查代码质量

## 📄 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

## 📧 联系方式

- **项目地址**: [GitHub Repository](https://github.com/StaRainorigin/axion)
- **问题反馈**: [GitHub Issues](https://github.com/StaRainorigin/axion/issues)
- **文档**: [项目文档](https://docs.rs/axion-data)

---

<div align="center">
  <b>⭐ 如果这个项目对你有帮助，请给个 Star！⭐</b>
  <br>
  <sub>为数据科学和分析提供高性能的 Rust 解决方案</sub>
</div>