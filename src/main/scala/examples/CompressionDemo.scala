package examples

import columnar.schema.{Schema, Column, DataType}
import columnar.writer.FileWriter
import columnar.reader.FileReader
import columnar.format.CompressionCodec
import java.nio.file.Files
import scala.util.Random

/**
 * Demonstrates compression with Snappy codec.
 */
@main
def compressionDemo(): Unit =
  println("=== Snappy Compression Demo ===\n")

  val schema = Schema(
    columns = List(
      Column("id", DataType.IntegerType, false),
      Column("value1", DataType.IntegerType, true),
      Column("value2", DataType.IntegerType, true)
    )
  )

  // Generate test data
  val rand = new Random(42)
  val rowCount = 10000000
  val data = (1 to rowCount).map { i =>
    Map(
      "id" -> Some(i),
      "value1" -> Some(rand.nextInt(100)),
      "value2" -> Some(rand.nextInt(100))
    )
  }

  println(s"Testing with $rowCount rows, 3 integer columns\n")

  // Test without compression
  println(">>> WITHOUT Compression")
  val uncompressedFile = Files.createTempFile("bench_uncompressed", ".colf").toFile
  val uncompressedSchema = schema.copy(compression = CompressionCodec.NoCompression)
  val uncompressedWriter = new FileWriter(uncompressedSchema)

  val uncompressedWriteStart = System.currentTimeMillis()
  uncompressedWriter.write(data, uncompressedFile.getAbsolutePath)
  val uncompressedWriteTime = System.currentTimeMillis() - uncompressedWriteStart

  val uncompressedSize = uncompressedFile.length()
  val uncompressedWriteRate = (rowCount * 1000.0) / uncompressedWriteTime / 1000.0
  println(f"File size: ${uncompressedSize / 1024.0}%.2f KB")
  println(f"Write time: ${uncompressedWriteTime}%d ms (${uncompressedWriteRate}%.2f K rows/sec)")

  val uncompressedReader = new FileReader(uncompressedFile.getAbsolutePath)
  val uncompressedReadStart = System.currentTimeMillis()
  val uncompressedReadData = uncompressedReader.readAllColumns()
  val uncompressedReadTime = System.currentTimeMillis() - uncompressedReadStart
  val uncompressedReadRate = (rowCount * 1000.0) / uncompressedReadTime / 1000.0
  println(f"Read time: ${uncompressedReadTime}%d ms (${uncompressedReadRate}%.2f K rows/sec)\n")

  // Test with Snappy compression
  println(">>> WITH Snappy Compression")
  val compressedFile = Files.createTempFile("bench_compressed", ".colf").toFile
  val compressedSchema = schema.copy(compression = CompressionCodec.SnappyCompression)
  val compressedWriter = new FileWriter(compressedSchema)

  val compressedWriteStart = System.currentTimeMillis()
  compressedWriter.write(data, compressedFile.getAbsolutePath)
  val compressedWriteTime = System.currentTimeMillis() - compressedWriteStart

  val compressedSize = compressedFile.length()
  val compressedWriteRate = (rowCount * 1000.0) / compressedWriteTime / 1000.0
  println(f"File size: ${compressedSize / 1024.0}%.2f KB")
  println(f"Write time: ${compressedWriteTime}%d ms (${compressedWriteRate}%.2f K rows/sec)")

  val compressedReader = new FileReader(compressedFile.getAbsolutePath)
  val compressedReadStart = System.currentTimeMillis()
  val compressedReadData = compressedReader.readAllColumns()
  val compressedReadTime = System.currentTimeMillis() - compressedReadStart
  val compressedReadRate = (rowCount * 1000.0) / compressedReadTime / 1000.0
  println(f"Read time: ${compressedReadTime}%d ms (${compressedReadRate}%.2f K rows/sec)\n")

  // Verify data integrity
  assert(uncompressedReadData == compressedReadData, "Data mismatch after compression!")
  println("✓ Data integrity verified: compressed == uncompressed\n")

  // Print comparison
  println("=== Compression Results ===")
  val compressionRatio = (1.0 - compressedSize.toDouble / uncompressedSize) * 100
  println(f"Compression ratio: ${compressionRatio}%.1f%%")
  println(f"Space saved: ${(uncompressedSize - compressedSize) / 1024.0}%.2f KB")

  val writeSpeedDiff = ((compressedWriteTime.toDouble / uncompressedWriteTime - 1) * 100)
  println(f"Write speed impact: ${if (writeSpeedDiff > 0) "+" else ""}${writeSpeedDiff}%.1f%%")

  val readSpeedDiff = ((compressedReadTime.toDouble / uncompressedReadTime - 1) * 100)
  println(f"Read speed impact: ${if (readSpeedDiff > 0) "+" else ""}${readSpeedDiff}%.1f%%")

  // Cleanup
  uncompressedFile.delete()
  compressedFile.delete()
