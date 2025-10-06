package examples

import columnar.query.*
import columnar.schema.{Schema, Column, DataType}
import columnar.writer.FileWriter
import columnar.reader.FileReader
import java.nio.file.Files
import scala.util.Random

/**
 * Simple benchmark runner (inline version for main sources).
 * For full benchmarks, run: sbt "test:runMain columnar.benchmark.QueryBenchmark"
 */
@main
def benchmarkRunner(): Unit =
  println("Running simple read/write benchmark...\n")

  // Generate test data
//  val schema = Schema(List(
//    Column("id", DataType.IntegerType, false),
//    Column("name", DataType.StringType, true),
//    Column("value", DataType.IntegerType, true)
//  ))

  val schema = Schema(List(
    Column("id", DataType.IntegerType, false),
    Column("value1", DataType.IntegerType, true),
    Column("value2", DataType.IntegerType, true)
  ))

  val rowCounts = Seq(1000, 10000, 100000, 1000000, 10000000)

  println("=" * 80)
  println(f"${"Operation"}%-40s | ${"Rows"}%10s | ${"Time (ms)"}%12s | ${"Rows/sec"}%12s")
  println("=" * 80)

  for (rowCount <- rowCounts) {
    val rand = new Random(42)
    val data = (1 to rowCount).map { i =>
      Map(
        "id" -> Some(i),
        "name" -> Some(s"User_$i"),
        "value" -> Some(rand.nextInt(100))
      )
    }

    val tmpFile = Files.createTempFile("bench", ".colf").toFile

    try {
      // Write benchmark
      val writer = new FileWriter(schema)
      val writeStart = System.currentTimeMillis()
      writer.write(data, tmpFile.getAbsolutePath)
      val writeTime = System.currentTimeMillis() - writeStart
      val writeRate = (rowCount * 1000.0) / writeTime

      val writeName = s"Write $rowCount rows".padTo(40, ' ')
      println(f"$writeName | ${rowCount}%10d | ${writeTime}%12d | ${writeRate / 1000}%12.2f K")

      // Read benchmark
      val reader = new FileReader(tmpFile.getAbsolutePath)
      val readStart = System.currentTimeMillis()
      val readData = reader.readAllColumns()
      val readTime = System.currentTimeMillis() - readStart
      val readRate = (rowCount * 1000.0) / readTime

      val readName = s"Read $rowCount rows".padTo(40, ' ')
      println(f"$readName | ${rowCount}%10d | ${readTime}%12d | ${readRate / 1000}%12.2f K")

      // Query benchmark
      val engine = new QueryEngine(reader)
      val queryStart = System.currentTimeMillis()
      val filtered = engine.filter(GreaterThan("value", 50)).collect()
      val queryTime = System.currentTimeMillis() - queryStart
      val queryRate = (rowCount * 1000.0) / queryTime

      val filterName = s"Filter $rowCount rows".padTo(40, ' ')
      println(f"$filterName | ${rowCount}%10d | ${queryTime}%12d | ${queryRate / 1000}%12.2f K")

      val fileSize = tmpFile.length() / 1024.0
      println(f"File size: ${fileSize}%.2f KB\n")

    } finally {
      tmpFile.delete()
    }
  }

  println("=" * 80)
  println("\nFor comprehensive benchmarks, see: src/test/scala/columnar/benchmark/")
