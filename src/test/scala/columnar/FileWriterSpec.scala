package columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import columnar.writer.{FileWriter, ColumnIndexEntry}
import columnar.schema.*
import java.io.{File, DataInputStream, FileInputStream, RandomAccessFile}
import java.nio.file.{Files, Paths}

class FileWriterSpec extends AnyFlatSpec with Matchers:

  // Helper to clean up test files
  def withTestFile(filename: String)(test: String => Unit): Unit =
    try
      test(filename)
    finally
      val file = new File(filename)
      if file.exists() then file.delete()

  "FileWriter" should "convert rows to columns correctly" in {
    val schema = TestData.simpleSchema
    val rows = TestData.sampleRows
    val writer = new FileWriter(schema)

    val chunks = writer.rowsToColumns(rows)

    // Should have one chunk per column
    chunks.size shouldBe 3

    // Each chunk should have the same row count
    chunks.foreach { chunk =>
      chunk.rowCount shouldBe 3
    }

    // Check column names match
    chunks.map(_.column.name) shouldBe Seq("id", "name", "active")
  }

  it should "create chunks with correct data sizes" in {
    val schema = TestData.simpleSchema
    val rows = TestData.sampleRows
    val writer = new FileWriter(schema)

    val chunks = writer.rowsToColumns(rows)

    // "id" column: 1 byte (null bitmap for 3 rows) + 3 integers (3 * 4) = 1 + 12 = 13 bytes
    chunks(0).sizeInBytes shouldBe 13

    // "name" column: 1 byte (bitmap) + "Alice" (4+5=9) + "Bob" (4+3=7) + "Carol" (4+5=9) = 1 + 25 = 26 bytes
    chunks(1).sizeInBytes shouldBe 26

    // "active" column: 1 byte (bitmap) + 3 booleans (3 * 1) = 1 + 3 = 4 bytes
    chunks(2).sizeInBytes shouldBe 4
  }

  it should "write a valid file with correct structure" in withTestFile("test_basic.col") { filename =>
    val schema = TestData.simpleSchema
    val rows = TestData.sampleRows
    val writer = new FileWriter(schema)

    writer.write(rows, filename)

    // Verify file was created
    val file = new File(filename)
    file.exists() shouldBe true
    file.length() should be > 0L

    // Verify file structure
    val in = new DataInputStream(new FileInputStream(filename))

    try {
      // Check header
      val magic = new Array[Byte](4)
      in.read(magic)
      new String(magic) shouldBe "COLF"

      val version = in.readByte()
      version shouldBe 1

      val compression = in.readByte()
      compression shouldBe 0  // NoCompression

      val columnCount = in.readInt()
      columnCount shouldBe 3

      val rowCount = in.readInt()
      rowCount shouldBe 3

      println("✓ Header verified")

    } finally {
      in.close()
    }
  }

  it should "write index entries correctly" in withTestFile("test_index.col") { filename =>
    val schema = TestData.simpleSchema
    val rows = TestData.sampleRows
    val writer = new FileWriter(schema)

    writer.write(rows, filename)

    val in = new DataInputStream(new FileInputStream(filename))

    try {
      // Skip header (14 bytes)
      in.skip(14)

      // Read first index entry
      val entry1 = ColumnIndexEntry.readFromStream(in)

      // Metadata offset should be after header (14) + index (72)
      entry1.metadataOffset should be >= 86L

      // Data offset should be after metadata section
      entry1.dataOffset should be > entry1.metadataOffset

      // Data size should be 13 bytes (1 byte null bitmap + 3 integers * 4 bytes)
      entry1.dataSize shouldBe 13

      println(s"✓ First index entry: ${entry1.summary}")

      // Read second index entry
      val entry2 = ColumnIndexEntry.readFromStream(in)
      entry2.metadataOffset should be > entry1.metadataOffset
      entry2.dataOffset should be > entry1.dataOffset

      println(s"✓ Second index entry: ${entry2.summary}")

    } finally {
      in.close()
    }
  }

  it should "write metadata correctly" in withTestFile("test_metadata.col") { filename =>
    val schema = TestData.simpleSchema
    val rows = TestData.sampleRows
    val writer = new FileWriter(schema)

    writer.write(rows, filename)

    val raf = new RandomAccessFile(filename, "r")

    try {
      // Skip to first metadata entry (header 14 + index 72 = 86)
      raf.seek(86)

      // Read first column metadata ("id")
      val nameLength = raf.readInt()
      nameLength shouldBe 2

      val nameBytes = new Array[Byte](nameLength)
      raf.read(nameBytes)
      new String(nameBytes) shouldBe "id"

      val typeCode = raf.readByte()
      typeCode shouldBe 1  // IntegerType

      val nullable = raf.readByte()
      nullable shouldBe 0  // false

      println("✓ Metadata verified")

    } finally {
      raf.close()
    }
  }

  it should "write column data correctly" in withTestFile("test_data.col") { filename =>
    val schema = Schema(Seq(
      Column("x", DataType.IntegerType, nullable = false)
    ))
    val rows = Seq(
      Map("x" -> Some(42))
    )
    val writer = new FileWriter(schema)

    writer.write(rows, filename)

    val raf = new RandomAccessFile(filename, "r")

    try {
      // Read index to find data offset
      raf.seek(14)  // Skip header

      val metadataOffset = raf.readLong()
      val dataOffset = raf.readLong()
      val dataSize = raf.readInt()

      // Seek to data
      raf.seek(dataOffset)

      // Read size prefix
      val size = raf.readInt()
      size shouldBe 5  // 1 byte (null bitmap) + 4 bytes (integer)

      // Read null bitmap (1 byte for 1 row)
      val bitmap = raf.readByte()
      bitmap shouldBe 0  // not null

      // Read actual data
      val value = raf.readInt()
      value shouldBe 42

      println("✓ Data verified: read value 42")

    } finally {
      raf.close()
    }
  }

  it should "handle empty strings" in withTestFile("test_empty_string.col") { filename =>
    val schema = Schema(Seq(
      Column("name", DataType.StringType, nullable = true)
    ))
    val rows = Seq(
      Map("name" -> Some(""))
    )
    val writer = new FileWriter(schema)

    writer.write(rows, filename)

    val file = new File(filename)
    file.exists() shouldBe true

    println("✓ Empty string handled")
  }

  it should "handle multiple rows" in withTestFile("test_multi_row.col") { filename =>
    val schema = Schema(Seq(
      Column("id", DataType.IntegerType, nullable = false)
    ))
    val rows = (1 to 100).map(i => Map("id" -> Some(i)))
    val writer = new FileWriter(schema)

    writer.write(rows, filename)

    val in = new DataInputStream(new FileInputStream(filename))

    try {
      in.skip(4)  // Skip magic
      in.readByte()  // Skip version
      in.readByte()  // Skip compression codec
      in.readInt()  // Skip column count
      val rowCount = in.readInt()

      rowCount shouldBe 100

      println("✓ 100 rows written")

    } finally {
      in.close()
    }
  }

  it should "calculate correct file size" in withTestFile("test_size.col") { filename =>
    val schema = TestData.simpleSchema
    val rows = TestData.sampleRows
    val writer = new FileWriter(schema)

    writer.write(rows, filename)

    val file = new File(filename)
    val actualSize = file.length()

    // Expected:
    // Header: 14
    // Index: 3 * 24 = 72
    // Metadata: ~30 (variable based on column names)
    // Data: 12 (id) + 25 (name) + 3 (active) = 40
    // Size prefixes: 3 * 4 = 12
    // Total: ~14 + 72 + 30 + 52 = ~168 bytes

    actualSize should be > 150L
    actualSize should be < 210L

    println(s"✓ File size: $actualSize bytes")
  }

  it should "handle special characters in strings" in withTestFile("test_special.col") { filename =>
    val schema = Schema(Seq(
      Column("text", DataType.StringType, nullable = true)
    ))
    val rows = Seq(
      Map("text" -> Some("Hello, 世界!")),
      Map("text" -> Some("café ☕"))
    )
    val writer = new FileWriter(schema)

    writer.write(rows, filename)

    val file = new File(filename)
    file.exists() shouldBe true

    println("✓ Special characters handled")
  }

end FileWriterSpec