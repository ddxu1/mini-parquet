package columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import columnar.writer.{FileWriter, ColumnIndexEntry}
import columnar.schema.*
import java.io.{File, RandomAccessFile}

class FileVerificationSpec extends AnyFlatSpec with Matchers:

  /**
   * Helper to read and verify entire file structure
   */
  def verifyFileStructure(filename: String): Unit =
    val raf = new RandomAccessFile(filename, "r")

    try {
      println(s"\n=== Verifying $filename ===")

      // 1. Verify Header
      val magic = new Array[Byte](4)
      raf.read(magic)
      assert(new String(magic) == "COLF", "Invalid magic bytes")

      val version = raf.readByte()
      assert(version == 1, "Invalid version")

      val columnCount = raf.readInt()
      val rowCount = raf.readInt()

      println(s"Header: $columnCount columns, $rowCount rows")

      // 2. Read Index
      val indexEntries = (0 until columnCount).map { i =>
        val entry = ColumnIndexEntry.readFromStream(raf)
        println(s"Index[$i]: ${entry.summary}")
        entry
      }

      // 3. Verify Metadata
      indexEntries.zipWithIndex.foreach { case (entry, i) =>
        raf.seek(entry.metadataOffset)

        val nameLength = raf.readInt()
        val nameBytes = new Array[Byte](nameLength)
        raf.read(nameBytes)
        val name = new String(nameBytes)

        val typeCode = raf.readByte()
        val nullable = raf.readByte()

        println(s"Metadata[$i]: name=$name, type=$typeCode, nullable=$nullable")
      }

      // 4. Verify Data
      indexEntries.zipWithIndex.foreach { case (entry, i) =>
        raf.seek(entry.dataOffset)

        val dataSize = raf.readInt()
        assert(dataSize == entry.dataSize, s"Data size mismatch for column $i")

        val dataBytes = new Array[Byte](dataSize)
        raf.read(dataBytes)

        println(s"Data[$i]: ${dataBytes.take(20).mkString(", ")}... (${dataBytes.length} bytes)")
      }

      println("✓ All verification passed!\n")

    } finally {
      raf.close()
    }

  "FileWriter" should "create verifiable files" in {
    val schema = TestData.simpleSchema
    val rows = TestData.sampleRows
    val writer = new FileWriter(schema)

    val filename = "test_verify.col"
    writer.write(rows, filename)

    verifyFileStructure(filename)

    // Clean up
    new File(filename).delete()
  }

  it should "maintain consistency across writes" in {
    val schema = TestData.simpleSchema
    val rows = TestData.sampleRows
    val writer = new FileWriter(schema)

    val filename1 = "test_consistent1.col"
    val filename2 = "test_consistent2.col"

    // Write same data twice
    writer.write(rows, filename1)
    writer.write(rows, filename2)

    // Files should be identical
    val bytes1 = java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(filename1))
    val bytes2 = java.nio.file.Files.readAllBytes(java.nio.file.Paths.get(filename2))

    bytes1 shouldBe bytes2

    println("✓ Files are identical")

    // Clean up
    new File(filename1).delete()
    new File(filename2).delete()
  }

end FileVerificationSpec