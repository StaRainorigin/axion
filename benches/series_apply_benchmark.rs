use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use axion_data::series::Series;
use rand::Rng;
use std::hint::black_box;
use std::time::Duration;

// 辅助函数：生成一个包含随机 i32 数据的 Series
fn generate_series_i32(size: usize, name_prefix: &str) -> Series<i32> {
    let mut rng = rand::rng();
    let data: Vec<i32> = (0..size).map(|_| rng.random_range(0..1000)).collect();
    Series::new(format!("{}_{}", name_prefix, size), data)
}

// 辅助函数：生成一个包含随机 f64 数据的 Series
fn generate_series_f64(size: usize, name_prefix: &str) -> Series<f64> {
    let mut rng = rand::rng();
    let data: Vec<f64> = (0..size).map(|_| rng.random_range(0.0..1000.0)).collect();
    Series::new(format!("{}_{}", name_prefix, size), data)
}

// 复杂操作
fn complex_op_i32(opt_x: Option<&i32>) -> Option<i32> {
    opt_x.map(|x| (x * 3) - (x / 2) + (x % 5) * 10)
}

fn complex_op_f64(opt_x: Option<&f64>) -> Option<f64> {
    opt_x.map(|x| (x * 3.5).sin() - (x / 2.1).cos() + x.sqrt().abs() * 10.0)
}

// 自定义 Criterion 配置
fn custom_criterion() -> Criterion {
    Criterion::default()
        .sample_size(30)                     // 适中的样本数量
        .measurement_time(Duration::from_secs(10))  // 测量时间
        .warm_up_time(Duration::from_secs(3))       // 预热时间
        .with_plots()                        // 生成性能图表
}

fn benchmark_series_apply(c: &mut Criterion) {
    // === 完整数据生成：1K, 10K, 100K, 1M, 10M, 100M ===
    println!("开始生成完整测试数据集...");
    
    // 整型数据集
    println!("生成整型 (i32) 数据集...");
    let series_i32_1k = generate_series_i32(1_000, "i32_1k");           // 1K
    let series_i32_10k = generate_series_i32(10_000, "i32_10k");        // 10K
    let series_i32_100k = generate_series_i32(100_000, "i32_100k");     // 100K
    let series_i32_1m = generate_series_i32(1_000_000, "i32_1m");       // 1M
    let series_i32_10m = generate_series_i32(10_000_000, "i32_10m");    // 10M
    let series_i32_100m = generate_series_i32(100_000_000, "i32_100m"); // 100M
    
    // 浮点型数据集
    println!("生成浮点型 (f64) 数据集...");
    let series_f64_1k = generate_series_f64(1_000, "f64_1k");           // 1K
    let series_f64_10k = generate_series_f64(10_000, "f64_10k");        // 10K
    let series_f64_100k = generate_series_f64(100_000, "f64_100k");     // 100K
    let series_f64_1m = generate_series_f64(1_000_000, "f64_1m");       // 1M
    let series_f64_10m = generate_series_f64(10_000_000, "f64_10m");    // 10M
    let series_f64_100m = generate_series_f64(100_000_000, "f64_100m"); // 100M
    
    println!("所有测试数据生成完成！\n");

    // =====================================
    // 整型复杂操作：串行 vs 并行完整对比
    // =====================================
    println!("开始整型 (i32) 复杂操作性能测试 - 完整规模对比...");
    println!("复杂操作：(x * 3) - (x / 2) + (x % 5) * 10");
    
    let mut group_i32_complex = c.benchmark_group("i32 Complex Operations - Sequential vs Parallel - All Scales");
    
    // 1K 数据集对比
    println!("测试 1K 数据集...");
    group_i32_complex.throughput(Throughput::Elements(series_i32_1k.len() as u64));
    group_i32_complex.bench_function("i32_complex_sequential_1K", |b| {
        b.iter(|| series_i32_1k.apply(black_box(complex_op_i32)))
    });
    group_i32_complex.bench_function("i32_complex_parallel_1K", |b| {
        b.iter(|| series_i32_1k.par_apply(black_box(complex_op_i32)))
    });
    
    // 10K 数据集对比
    println!("测试 10K 数据集...");
    group_i32_complex.throughput(Throughput::Elements(series_i32_10k.len() as u64));
    group_i32_complex.bench_function("i32_complex_sequential_10K", |b| {
        b.iter(|| series_i32_10k.apply(black_box(complex_op_i32)))
    });
    group_i32_complex.bench_function("i32_complex_parallel_10K", |b| {
        b.iter(|| series_i32_10k.par_apply(black_box(complex_op_i32)))
    });
    
    // 100K 数据集对比
    println!("测试 100K 数据集...");
    group_i32_complex.throughput(Throughput::Elements(series_i32_100k.len() as u64));
    group_i32_complex.bench_function("i32_complex_sequential_100K", |b| {
        b.iter(|| series_i32_100k.apply(black_box(complex_op_i32)))
    });
    group_i32_complex.bench_function("i32_complex_parallel_100K", |b| {
        b.iter(|| series_i32_100k.par_apply(black_box(complex_op_i32)))
    });
    
    // 1M 数据集对比
    println!("测试 1M 数据集...");
    group_i32_complex.throughput(Throughput::Elements(series_i32_1m.len() as u64));
    group_i32_complex.bench_function("i32_complex_sequential_1M", |b| {
        b.iter(|| series_i32_1m.apply(black_box(complex_op_i32)))
    });
    group_i32_complex.bench_function("i32_complex_parallel_1M", |b| {
        b.iter(|| series_i32_1m.par_apply(black_box(complex_op_i32)))
    });
    
    // 10M 数据集对比
    println!("测试 10M 数据集...");
    group_i32_complex.throughput(Throughput::Elements(series_i32_10m.len() as u64));
    group_i32_complex.bench_function("i32_complex_sequential_10M", |b| {
        b.iter(|| series_i32_10m.apply(black_box(complex_op_i32)))
    });
    group_i32_complex.bench_function("i32_complex_parallel_10M", |b| {
        b.iter(|| series_i32_10m.par_apply(black_box(complex_op_i32)))
    });
    
    // 100M 数据集对比 (调整配置以适应大数据集)
    println!("测试 100M 数据集 (大规模测试)...");
    group_i32_complex.sample_size(10);  // 减少样本数量
    group_i32_complex.measurement_time(Duration::from_secs(15));  // 增加测量时间
    group_i32_complex.throughput(Throughput::Elements(series_i32_100m.len() as u64));
    
    group_i32_complex.bench_function("i32_complex_sequential_100M", |b| {
        b.iter(|| series_i32_100m.apply(black_box(complex_op_i32)))
    });
    group_i32_complex.bench_function("i32_complex_parallel_100M", |b| {
        b.iter(|| series_i32_100m.par_apply(black_box(complex_op_i32)))
    });
    
    group_i32_complex.finish();
    println!("整型 (i32) 复杂操作性能测试完成！\n");

    // =====================================
    // 浮点型复杂操作：串行 vs 并行完整对比
    // =====================================
    println!("开始浮点型 (f64) 复杂操作性能测试 - 完整规模对比...");
    println!("复杂操作：(x * 3.5).sin() - (x / 2.1).cos() + x.sqrt().abs() * 10.0");
    
    let mut group_f64_complex = c.benchmark_group("f64 Complex Operations - Sequential vs Parallel - All Scales");
    
    // 1K 数据集对比
    println!("测试 1K 数据集...");
    group_f64_complex.throughput(Throughput::Elements(series_f64_1k.len() as u64));
    group_f64_complex.bench_function("f64_complex_sequential_1K", |b| {
        b.iter(|| series_f64_1k.apply(black_box(complex_op_f64)))
    });
    group_f64_complex.bench_function("f64_complex_parallel_1K", |b| {
        b.iter(|| series_f64_1k.par_apply(black_box(complex_op_f64)))
    });
    
    // 10K 数据集对比
    println!("测试 10K 数据集...");
    group_f64_complex.throughput(Throughput::Elements(series_f64_10k.len() as u64));
    group_f64_complex.bench_function("f64_complex_sequential_10K", |b| {
        b.iter(|| series_f64_10k.apply(black_box(complex_op_f64)))
    });
    group_f64_complex.bench_function("f64_complex_parallel_10K", |b| {
        b.iter(|| series_f64_10k.par_apply(black_box(complex_op_f64)))
    });
    
    // 100K 数据集对比
    println!("测试 100K 数据集...");
    group_f64_complex.throughput(Throughput::Elements(series_f64_100k.len() as u64));
    group_f64_complex.bench_function("f64_complex_sequential_100K", |b| {
        b.iter(|| series_f64_100k.apply(black_box(complex_op_f64)))
    });
    group_f64_complex.bench_function("f64_complex_parallel_100K", |b| {
        b.iter(|| series_f64_100k.par_apply(black_box(complex_op_f64)))
    });
    
    // 1M 数据集对比
    println!("测试 1M 数据集...");
    group_f64_complex.throughput(Throughput::Elements(series_f64_1m.len() as u64));
    group_f64_complex.bench_function("f64_complex_sequential_1M", |b| {
        b.iter(|| series_f64_1m.apply(black_box(complex_op_f64)))
    });
    group_f64_complex.bench_function("f64_complex_parallel_1M", |b| {
        b.iter(|| series_f64_1m.par_apply(black_box(complex_op_f64)))
    });
    
    // 10M 数据集对比
    println!("测试 10M 数据集...");
    group_f64_complex.throughput(Throughput::Elements(series_f64_10m.len() as u64));
    group_f64_complex.bench_function("f64_complex_sequential_10M", |b| {
        b.iter(|| series_f64_10m.apply(black_box(complex_op_f64)))
    });
    group_f64_complex.bench_function("f64_complex_parallel_10M", |b| {
        b.iter(|| series_f64_10m.par_apply(black_box(complex_op_f64)))
    });
    
    // 100M 数据集对比 (调整配置以适应大数据集)
    println!("测试 100M 数据集 (大规模测试)...");
    group_f64_complex.sample_size(10);  // 减少样本数量
    group_f64_complex.measurement_time(Duration::from_secs(15));  // 增加测量时间
    group_f64_complex.throughput(Throughput::Elements(series_f64_100m.len() as u64));
    
    group_f64_complex.bench_function("f64_complex_sequential_100M", |b| {
        b.iter(|| series_f64_100m.apply(black_box(complex_op_f64)))
    });
    group_f64_complex.bench_function("f64_complex_parallel_100M", |b| {
        b.iter(|| series_f64_100m.par_apply(black_box(complex_op_f64)))
    });
    
    group_f64_complex.finish();
    println!("浮点型 (f64) 复杂操作性能测试完成！\n");

    println!("\n所有基准测试全部完成！");
    println!("查看详细结果：target/criterion/report/index.html");

}

criterion_group!(
    name = benches;
    config = custom_criterion();
    targets = benchmark_series_apply
);
criterion_main!(benches);