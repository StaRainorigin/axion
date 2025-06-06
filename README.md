# Axion

一个 Rust 数据处理库，提供类似 Pandas 的数据操作体验。

## 📦 安装

将以下内容添加到你的 `Cargo.toml` 文件中：

```toml
[dependencies]
axion = "0.1.0"
```

## 🛠️ 快速开始

### 创建 Series

```rust
use axion::series::Series;

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
println!("Length: {}", s1.len());

// 访问元素
if let Some(value) = s1.get(0) {
    println!("First element: {}", value);
}

// 迭代器
for value in s1.iter_valid() {
    println!("Value: {}", value);
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

println!("Sum: {:?}", s.sum());      // Some(15.0)
println!("Mean: {:?}", s.mean());    // Some(3.0)
println!("Min: {:?}", s.min());      // Some(1.0)
println!("Max: {:?}", s.max());      // Some(5.0)
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
    println!("Valid value: {}", value);
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
let lengths = s.str().len();      // 字符串长度
let upper = s.str().to_uppercase(); // 转大写
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
use axion::dataframe::DataFrame;

// 创建 DataFrame
let mut df = DataFrame::new();
df.add_column("name", vec!["Alice", "Bob", "Charlie"]).unwrap();
df.add_column("age", vec![25, 30, 35]).unwrap();
df.add_column("salary", vec![50000.0, 60000.0, 70000.0]).unwrap();

// 选择列
let age_column = df.column("age").unwrap();

// 过滤行
let high_earners = df.filter(&df.column("salary").unwrap().gt(55000.0).unwrap()).unwrap();

// 显示 DataFrame
println!("{}", df);
```

## 🔧 高级特性

### 类型转换

```rust
let s_f64 = Series::new("floats".to_string(), vec![1.0, 2.0, 3.0]);
let s_f32 = s_f64.cast::<f32>().unwrap();
```

### 排序

```rust
let mut s = Series::new("data".to_string(), vec![3, 1, 4, 1, 5]);
s.sort(false);  // 升序排序
println!("{}", s);  // [1, 1, 3, 4, 5]
```

### 性能优化提示

```rust
// 检查是否已排序（用于优化某些操作）
if s.is_sorted() {
    println!("Series is already sorted!");
}

// 预分配容量
let mut s = Series::new_empty("data".to_string(), DataType::Int32);
// 添加数据...
```

## 🧪 测试

运行测试套件：

```bash
cargo test
```

运行基准测试：

```bash
cargo bench
```

## 🛣️ 路线图

- [ ] 更多聚合函数 (std, var, quantile 等)
- [ ] 窗口函数支持
- [ ] 分组操作 (groupby)
- [ ] 连接操作 (join)
- [ ] 时间序列支持
- [ ] 文件 I/O (CSV, JSON, Parquet)
- [ ] 更多字符串操作
- [ ] 缺失值插值方法

## 🤝 贡献

欢迎贡献代码！

1. Fork 项目
2. 创建特性分支
3. 提交更改
4. 推送到分支
5. 开启 Pull Request

## 📄 许可证

本项目采用 MIT 许可证 - 查看 [LICENSE](LICENSE) 文件了解详情。

---

<div align="center">
  <b>⭐ 如果这个项目对你有帮助，请给个 Star！⭐</b>
</div>