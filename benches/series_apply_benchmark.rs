use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use axion::series::Series;
use rand::Rng;
use std::time::Duration;

// 辅助函数：生成一个包含随机 i32 数据的 Series
fn generate_series_i32(size: usize, name_prefix: &str) -> Series<i32> {
    let mut rng = rand::rng();
    let data: Vec<i32> = (0..size).map(|_| rng.random_range(0..1000)).collect();
    Series::new(format!("{}_{}", name_prefix, size), data)
}

// 辅助函数：生成一个包含随机 f64 数据的 Series
#[allow(dead_code)]
fn generate_series_f64(size: usize, name_prefix: &str) -> Series<f64> {
    let mut rng = rand::rng();
    let data: Vec<f64> = (0..size).map(|_| rng.random_range(0.0..1000.0)).collect();
    Series::new(format!("{}_{}", name_prefix, size), data)
}

// --- 原始复杂操作 ---
fn complex_op_i32(opt_x: Option<&i32>) -> Option<i32> {
    opt_x.map(|x| (x * 3) - (x / 2) + (x % 5) * 10)
}

#[allow(dead_code)]
fn complex_op_f64(opt_x: Option<&f64>) -> Option<f64> {
    opt_x.map(|x| (x * 3.5).sin() - (x / 2.1).cos() + x.sqrt().abs() * 10.0)
}

// --- 新增：简单操作 ---
fn simple_op_i32(opt_x: Option<&i32>) -> Option<i32> {
    opt_x.map(|x| x.wrapping_add(10))
}

#[allow(dead_code)]
fn simple_op_f64(opt_x: Option<&f64>) -> Option<f64> {
    opt_x.map(|x| x + 10.0)
}

// --- 新增：非常复杂的操作 ---
fn very_complex_op_i32(opt_x: Option<&i32>) -> Option<i32> {
    opt_x.map(|x| {
        let mut val = *x;
        for _ in 0..5 {
            val = (val.wrapping_mul(3)).wrapping_sub(val / 2).wrapping_add((val % 5).wrapping_mul(10));
        }
        val
    })
}

#[allow(dead_code)]
fn very_complex_op_f64(opt_x: Option<&f64>) -> Option<f64> {
    opt_x.map(|x| {
        let mut val = *x;
        for _ in 0..5 {
            val = (val * 3.5).sin() - (val / 2.1).cos() + val.sqrt().abs() * 10.0;
            if !val.is_finite() { return f64::NAN; }
        }
        val
    })
}

fn benchmark_series_apply(c: &mut Criterion) {
    // --- 数据生成 ---
    let series_i32_small = generate_series_i32(1_000, "s_i32");
    let series_i32_medium = generate_series_i32(100_000, "m_i32");
    let series_i32_large = generate_series_i32(1_000_000, "l_i32");
    let series_i32_xlarge = generate_series_i32(5_000_000, "xl_i32");
    let series_i32_xxlarge = generate_series_i32(10_000_000, "xxl_i32");
    
    println!("正在生成 500M i32 series...");
    let series_i32_huge = generate_series_i32(500_000_000, "h_i32_500M");
    println!("已完成生成 500M i32 series。");

    let series_f64_medium = generate_series_f64(100_000, "m_f64");
    let series_f64_large = generate_series_f64(1_000_000, "l_f64");
    let series_f64_xlarge = generate_series_f64(5_000_000, "xl_f64");
    println!("正在生成 100M f64 series...");
    let series_f64_huge = generate_series_f64(100_000_000, "h_f64_100M");
    println!("已完成生成 100M f64 series。");

    // --- i32 基准测试组 (原始复杂操作) ---
    let mut group_i32_complex = c.benchmark_group("Series<i32> ComplexOp Apply vs ParApply");
    group_i32_complex.bench_function("apply_i32_small_complex (1k)", |b| b.iter(|| series_i32_small.apply(black_box(complex_op_i32))));
    group_i32_complex.bench_function("par_apply_i32_small_complex (1k)", |b| b.iter(|| series_i32_small.par_apply(black_box(complex_op_i32))));
    group_i32_complex.bench_function("apply_i32_medium_complex (100k)", |b| b.iter(|| series_i32_medium.apply(black_box(complex_op_i32))));
    group_i32_complex.bench_function("par_apply_i32_medium_complex (100k)", |b| b.iter(|| series_i32_medium.par_apply(black_box(complex_op_i32))));
    group_i32_complex.bench_function("apply_i32_large_complex (1M)", |b| b.iter(|| series_i32_large.apply(black_box(complex_op_i32))));
    group_i32_complex.bench_function("par_apply_i32_large_complex (1M)", |b| b.iter(|| series_i32_large.par_apply(black_box(complex_op_i32))));
    group_i32_complex.bench_function("apply_i32_xlarge_complex (5M)", |b| b.iter(|| series_i32_xlarge.apply(black_box(complex_op_i32))));
    group_i32_complex.bench_function("par_apply_i32_xlarge_complex (5M)", |b| b.iter(|| series_i32_xlarge.par_apply(black_box(complex_op_i32))));
    group_i32_complex.bench_function("apply_i32_xxlarge_complex (10M)", |b| b.iter(|| series_i32_xxlarge.apply(black_box(complex_op_i32))));
    group_i32_complex.bench_function("par_apply_i32_xxlarge_complex (10M)", |b| b.iter(|| series_i32_xxlarge.par_apply(black_box(complex_op_i32))));

    group_i32_complex.sample_size(10);
    group_i32_complex.measurement_time(Duration::from_secs(60));
    group_i32_complex.throughput(Throughput::Elements(series_i32_huge.len() as u64));
    println!("开始 500M i32 complex_op apply 基准测试...");
    group_i32_complex.bench_function("apply_i32_huge_complex (500M)", |b| b.iter(|| series_i32_huge.apply(black_box(complex_op_i32))));
    println!("开始 500M i32 complex_op par_apply 基准测试...");
    group_i32_complex.bench_function("par_apply_i32_huge_complex (500M)", |b| b.iter(|| series_i32_huge.par_apply(black_box(complex_op_i32))));
    group_i32_complex.finish();

    // --- i32 基准测试组 (简单操作) ---
    let mut group_i32_simple = c.benchmark_group("Series<i32> SimpleOp Apply vs ParApply");
    group_i32_simple.bench_function("apply_i32_large_simple (1M)", |b| b.iter(|| series_i32_large.apply(black_box(simple_op_i32))));
    group_i32_simple.bench_function("par_apply_i32_large_simple (1M)", |b| b.iter(|| series_i32_large.par_apply(black_box(simple_op_i32))));
    
    group_i32_simple.sample_size(10);
    group_i32_simple.measurement_time(Duration::from_secs(30));
    group_i32_simple.throughput(Throughput::Elements(series_i32_huge.len() as u64));
    println!("开始 500M i32 simple_op apply 基准测试...");
    group_i32_simple.bench_function("apply_i32_huge_simple (500M)", |b| b.iter(|| series_i32_huge.apply(black_box(simple_op_i32))));
    println!("开始 500M i32 simple_op par_apply 基准测试...");
    group_i32_simple.bench_function("par_apply_i32_huge_simple (500M)", |b| b.iter(|| series_i32_huge.par_apply(black_box(simple_op_i32))));
    group_i32_simple.finish();

    // --- i32 基准测试组 (非常复杂的操作) ---
    let mut group_i32_very_complex = c.benchmark_group("Series<i32> VeryComplexOp Apply vs ParApply");
    group_i32_very_complex.bench_function("apply_i32_medium_very_complex (100k)", |b| b.iter(|| series_i32_medium.apply(black_box(very_complex_op_i32))));
    group_i32_very_complex.bench_function("par_apply_i32_medium_very_complex (100k)", |b| b.iter(|| series_i32_medium.par_apply(black_box(very_complex_op_i32))));
    group_i32_very_complex.bench_function("apply_i32_large_very_complex (1M)", |b| b.iter(|| series_i32_large.apply(black_box(very_complex_op_i32))));
    group_i32_very_complex.bench_function("par_apply_i32_large_very_complex (1M)", |b| b.iter(|| series_i32_large.par_apply(black_box(very_complex_op_i32))));
    
    group_i32_very_complex.sample_size(10);
    group_i32_very_complex.measurement_time(Duration::from_secs(90));
    group_i32_very_complex.throughput(Throughput::Elements(series_i32_huge.len() as u64));
    println!("开始 500M i32 very_complex_op apply 基准测试...");
    group_i32_very_complex.bench_function("apply_i32_huge_very_complex (500M)", |b| b.iter(|| series_i32_huge.apply(black_box(very_complex_op_i32))));
    println!("开始 500M i32 very_complex_op par_apply 基准测试...");
    group_i32_very_complex.bench_function("par_apply_i32_huge_very_complex (500M)", |b| b.iter(|| series_i32_huge.par_apply(black_box(very_complex_op_i32))));
    group_i32_very_complex.finish();

    // --- f64 基准测试组 (原始复杂操作) ---
    let mut group_f64_complex = c.benchmark_group("Series<f64> ComplexOp Apply vs ParApply");
    group_f64_complex.bench_function("apply_f64_medium_complex (100k)", |b| b.iter(|| series_f64_medium.apply(black_box(complex_op_f64))));
    group_f64_complex.bench_function("par_apply_f64_medium_complex (100k)", |b| b.iter(|| series_f64_medium.par_apply(black_box(complex_op_f64))));
    group_f64_complex.bench_function("apply_f64_large_complex (1M)", |b| b.iter(|| series_f64_large.apply(black_box(complex_op_f64))));
    group_f64_complex.bench_function("par_apply_f64_large_complex (1M)", |b| b.iter(|| series_f64_large.par_apply(black_box(complex_op_f64))));
    group_f64_complex.bench_function("apply_f64_xlarge_complex (5M)", |b| b.iter(|| series_f64_xlarge.apply(black_box(complex_op_f64))));
    group_f64_complex.bench_function("par_apply_f64_xlarge_complex (5M)", |b| b.iter(|| series_f64_xlarge.par_apply(black_box(complex_op_f64))));

    group_f64_complex.sample_size(10);
    group_f64_complex.measurement_time(Duration::from_secs(120));
    group_f64_complex.throughput(Throughput::Elements(series_f64_huge.len() as u64));
    println!("开始 100M f64 complex_op apply 基准测试...");
    group_f64_complex.bench_function("apply_f64_huge_complex (100M)", |b| b.iter(|| series_f64_huge.apply(black_box(complex_op_f64))));
    println!("开始 100M f64 complex_op par_apply 基准测试...");
    group_f64_complex.bench_function("par_apply_f64_huge_complex (100M)", |b| b.iter(|| series_f64_huge.par_apply(black_box(complex_op_f64))));
    group_f64_complex.finish();

    // --- f64 基准测试组 (简单操作) ---
    let mut group_f64_simple = c.benchmark_group("Series<f64> SimpleOp Apply vs ParApply");
    group_f64_simple.bench_function("apply_f64_large_simple (1M)", |b| b.iter(|| series_f64_large.apply(black_box(simple_op_f64))));
    group_f64_simple.bench_function("par_apply_f64_large_simple (1M)", |b| b.iter(|| series_f64_large.par_apply(black_box(simple_op_f64))));

    group_f64_simple.sample_size(10);
    group_f64_simple.measurement_time(Duration::from_secs(60));
    group_f64_simple.throughput(Throughput::Elements(series_f64_huge.len() as u64));
    println!("开始 100M f64 simple_op apply 基准测试...");
    group_f64_simple.bench_function("apply_f64_huge_simple (100M)", |b| b.iter(|| series_f64_huge.apply(black_box(simple_op_f64))));
    println!("开始 100M f64 simple_op par_apply 基准测试...");
    group_f64_simple.bench_function("par_apply_f64_huge_simple (100M)", |b| b.iter(|| series_f64_huge.par_apply(black_box(simple_op_f64))));
    group_f64_simple.finish();

    // --- f64 基准测试组 (非常复杂的操作) ---
    let mut group_f64_very_complex = c.benchmark_group("Series<f64> VeryComplexOp Apply vs ParApply");
    group_f64_very_complex.bench_function("apply_f64_medium_very_complex (100k)", |b| b.iter(|| series_f64_medium.apply(black_box(very_complex_op_f64))));
    group_f64_very_complex.bench_function("par_apply_f64_medium_very_complex (100k)", |b| b.iter(|| series_f64_medium.par_apply(black_box(very_complex_op_f64))));
    group_f64_very_complex.bench_function("apply_f64_large_very_complex (1M)", |b| b.iter(|| series_f64_large.apply(black_box(very_complex_op_f64))));
    group_f64_very_complex.bench_function("par_apply_f64_large_very_complex (1M)", |b| b.iter(|| series_f64_large.par_apply(black_box(very_complex_op_f64))));
    
    group_f64_very_complex.sample_size(10);
    group_f64_very_complex.measurement_time(Duration::from_secs(180)); // Might be very long
    group_f64_very_complex.throughput(Throughput::Elements(series_f64_huge.len() as u64));
    println!("开始 100M f64 very_complex_op apply 基准测试...");
    group_f64_very_complex.bench_function("apply_f64_huge_very_complex (100M)", |b| b.iter(|| series_f64_huge.apply(black_box(very_complex_op_f64))));
    println!("开始 100M f64 very_complex_op par_apply 基准测试...");
    group_f64_very_complex.bench_function("par_apply_f64_huge_very_complex (100M)", |b| b.iter(|| series_f64_huge.par_apply(black_box(very_complex_op_f64))));
    group_f64_very_complex.finish();
}

criterion_group!(benches, benchmark_series_apply);
criterion_main!(benches);