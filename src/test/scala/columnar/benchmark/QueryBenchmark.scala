package columnar.benchmark

import columnar.query.*
import columnar.schema.{Schema, Column, DataType}
import columnar.writer.FileWriter
import columnar.reader.FileReader
import java.nio.file.Files
import java.io.File
import scala.util.Random

/**
 * Comprehensive benchmark suite for Arquet query engine.
 * Tests read/write performance, query operations, and scalability.
 */
object QueryBenchmark:

  case class BenchmarkResult(
    name: String,
    rowCount: Int,
    timeMs: Long,
    rowsPerSec: Double,
    mbPerSec: Option[Double] = None,
    fileSize: Option[Long] = None
  ):
    def throughput: String =
      f"${rowsPerSec / 1000}%.2f K rows/sec"

    def fileSizeStr: String =
      fileSize.map(s => f"${s / 1024.0 / 1024.0}%.2f MB").getOrElse("N/A")

  /**
   * Timing utility
   */
  def time[T](operation: => T): (T, Long) =
    val start = System.currentTimeMillis()
    val result = operation
    val elapsed = System.currentTimeMillis() - start
    (result, elapsed)

  /**
   * Generate synthetic data for benchmarking
   */
  def generateData(rowCount: Int, seed: Int = 42): Seq[Map[String, Option[Any]]] =
    val rand = new Random(seed)
    val names = Array("Alice", "Bob", "Carol", "Dave", "Eve", "Frank", "Grace", "Heidi")

    (1 to rowCount).map { i =>
      Map(
        "id" -> Some(i),
        "name" -> (if rand.nextDouble() > 0.1 then Some(names(rand.nextInt(names.length))) else None),
        "age" -> (if rand.nextDouble() > 0.05 then Some(20 + rand.nextInt(60)) else None),
        "score" -> Some(rand.nextInt(100)),
        "active" -> Some(rand.nextBoolean())
      )
    }

  /**
   * Schema for benchmark data
   */
  val benchmarkSchema = Schema(List(
    Column("id", DataType.IntegerType, false),
    Column("name", DataType.StringType, true),
    Column("age", DataType.IntegerType, true),
    Column("score", DataType.IntegerType, false),
    Column("active", DataType.BooleanType, false)
  ))

  /**
   * Benchmark: Write throughput
   */
  def benchmarkWrite(rowCount: Int): BenchmarkResult =
    val tmpFile = Files.createTempFile("bench_write", ".colf").toFile
    val data = generateData(rowCount)
    val writer = new FileWriter(benchmarkSchema)

    try
      val (_, timeMs) = time {
        writer.write(data, tmpFile.getAbsolutePath)
      }

      val fileSize = tmpFile.length()
      val rowsPerSec = (rowCount * 1000.0) / timeMs
      val mbPerSec = (fileSize / 1024.0 / 1024.0) / (timeMs / 1000.0)

      BenchmarkResult(
        s"Write ($rowCount rows)",
        rowCount,
        timeMs,
        rowsPerSec,
        Some(mbPerSec),
        Some(fileSize)
      )
    finally
      tmpFile.delete()

  /**
   * Benchmark: Read throughput
   */
  def benchmarkRead(rowCount: Int): BenchmarkResult =
    val tmpFile = Files.createTempFile("bench_read", ".colf").toFile
    val data = generateData(rowCount)
    val writer = new FileWriter(benchmarkSchema)
    writer.write(data, tmpFile.getAbsolutePath)

    try
      val reader = new FileReader(tmpFile.getAbsolutePath)
      val (_, timeMs) = time {
        reader.readAllColumns()
      }

      val fileSize = tmpFile.length()
      val rowsPerSec = (rowCount * 1000.0) / timeMs
      val mbPerSec = (fileSize / 1024.0 / 1024.0) / (timeMs / 1000.0)

      BenchmarkResult(
        s"Read ($rowCount rows)",
        rowCount,
        timeMs,
        rowsPerSec,
        Some(mbPerSec),
        Some(fileSize)
      )
    finally
      tmpFile.delete()

  /**
   * Benchmark: Filter with varying selectivity
   */
  def benchmarkFilter(rowCount: Int, selectivity: Double): BenchmarkResult =
    val tmpFile = Files.createTempFile("bench_filter", ".colf").toFile
    val data = generateData(rowCount)
    val writer = new FileWriter(benchmarkSchema)
    writer.write(data, tmpFile.getAbsolutePath)

    try
      val reader = new FileReader(tmpFile.getAbsolutePath)
      val engine = new QueryEngine(reader)

      // Filter that should match approximately selectivity% of rows
      val threshold = (100 * selectivity).toInt
      val predicate = LessThan("score", threshold)

      val (results, timeMs) = time {
        engine.filter(predicate).collect()
      }

      val rowsPerSec = (rowCount * 1000.0) / timeMs
      val actualSelectivity = results.size.toDouble / rowCount

      BenchmarkResult(
        f"Filter ${selectivity * 100}%.0f%% selectivity ($rowCount rows, actual: ${actualSelectivity * 100}%.1f%%)",
        rowCount,
        timeMs,
        rowsPerSec
      )
    finally
      tmpFile.delete()

  /**
   * Benchmark: Column projection
   */
  def benchmarkProjection(rowCount: Int, numColumns: Int): BenchmarkResult =
    val tmpFile = Files.createTempFile("bench_projection", ".colf").toFile
    val data = generateData(rowCount)
    val writer = new FileWriter(benchmarkSchema)
    writer.write(data, tmpFile.getAbsolutePath)

    try
      val reader = new FileReader(tmpFile.getAbsolutePath)
      val engine = new QueryEngine(reader)

      val columns = benchmarkSchema.columns.take(numColumns).map(_.name)

      val (_, timeMs) = time {
        engine.select(columns*).collect()
      }

      val rowsPerSec = (rowCount * 1000.0) / timeMs

      BenchmarkResult(
        s"Project $numColumns/${benchmarkSchema.columnCount} columns ($rowCount rows)",
        rowCount,
        timeMs,
        rowsPerSec
      )
    finally
      tmpFile.delete()

  /**
   * Benchmark: Aggregation operations
   */
  def benchmarkAggregation(rowCount: Int, operation: String): BenchmarkResult =
    val tmpFile = Files.createTempFile("bench_agg", ".colf").toFile
    val data = generateData(rowCount)
    val writer = new FileWriter(benchmarkSchema)
    writer.write(data, tmpFile.getAbsolutePath)

    try
      val reader = new FileReader(tmpFile.getAbsolutePath)
      val engine = new QueryEngine(reader)

      val (_, timeMs) = time {
        operation match
          case "count" => engine.count()
          case "sum" => engine.sum("score")
          case "avg" => engine.avg("score")
          case "min" => engine.min("score")
          case "max" => engine.max("score")
          case "groupby" => engine.groupByCount("name")
          case _ => throw new IllegalArgumentException(s"Unknown operation: $operation")
      }

      val rowsPerSec = (rowCount * 1000.0) / timeMs

      BenchmarkResult(
        s"Aggregation: $operation ($rowCount rows)",
        rowCount,
        timeMs,
        rowsPerSec
      )
    finally
      tmpFile.delete()

  /**
   * Benchmark: Complex query chain
   */
  def benchmarkComplexQuery(rowCount: Int): BenchmarkResult =
    val tmpFile = Files.createTempFile("bench_complex", ".colf").toFile
    val data = generateData(rowCount)
    val writer = new FileWriter(benchmarkSchema)
    writer.write(data, tmpFile.getAbsolutePath)

    try
      val reader = new FileReader(tmpFile.getAbsolutePath)
      val engine = new QueryEngine(reader)

      val (_, timeMs) = time {
        engine
          .filter(And(GreaterThan("age", 30), Equals("active", true)))
          .select("name", "age", "score")
          .limit(100)
          .collect()
      }

      val rowsPerSec = (rowCount * 1000.0) / timeMs

      BenchmarkResult(
        s"Complex query chain ($rowCount rows)",
        rowCount,
        timeMs,
        rowsPerSec
      )
    finally
      tmpFile.delete()

  /**
   * Print benchmark results in a nice table
   */
  def printResults(results: Seq[BenchmarkResult]): Unit =
    println("\n" + "=" * 100)
    println("ARQUET BENCHMARK RESULTS")
    println("=" * 100)

    val nameWidth = results.map(_.name.length).max + 2
    val benchmarkTitle = "Benchmark".padTo(nameWidth, ' ')
    println(s"$benchmarkTitle | ${"Rows".padTo(10, ' ')} | ${"Time (ms)".padTo(10, ' ')} | ${"Throughput".padTo(15, ' ')} | ${"File Size".padTo(12, ' ')}")
    println("-" * 100)

    results.foreach { r =>
      val name = r.name.padTo(nameWidth, ' ')
      println(f"$name | ${r.rowCount}%10d | ${r.timeMs}%10d | ${r.throughput}%15s | ${r.fileSizeStr}%12s")
    }

    println("=" * 100 + "\n")

  /**
   * Run all benchmarks
   */
  def runAll(): Unit =
    println("Starting Arquet Benchmark Suite...")
    println("Generating synthetic data and running tests...\n")

    val results = scala.collection.mutable.ArrayBuffer[BenchmarkResult]()

    // Scalability tests
    println(">>> Scalability Tests (Write/Read)")
    for rowCount <- Seq(1000, 10000, 100000, 1000000) do
      results += benchmarkWrite(rowCount)
      results += benchmarkRead(rowCount)

    // Filter selectivity tests
    println("\n>>> Filter Selectivity Tests (100K rows)")
    for selectivity <- Seq(0.01, 0.1, 0.5, 0.9) do
      results += benchmarkFilter(100000, selectivity)

    // Projection tests
    println("\n>>> Column Projection Tests (100K rows)")
    for numCols <- Seq(1, 2, 5) do
      results += benchmarkProjection(100000, numCols)

    // Aggregation tests
    println("\n>>> Aggregation Tests (100K rows)")
    for op <- Seq("count", "sum", "avg", "min", "max", "groupby") do
      results += benchmarkAggregation(100000, op)

    // Complex query test
    println("\n>>> Complex Query Test (100K rows)")
    results += benchmarkComplexQuery(100000)

    // Print final results
    printResults(results.toSeq)

  /**
   * Quick benchmark for testing
   */
  def runQuick(): Unit =
    println("Running quick benchmark...\n")

    val results = Seq(
      benchmarkWrite(10000),
      benchmarkRead(10000),
      benchmarkFilter(10000, 0.1),
      benchmarkProjection(10000, 2),
      benchmarkAggregation(10000, "sum")
    )

    printResults(results)

end QueryBenchmark
