package columnar.reader

import org.scalatest.funsuite.AnyFunSuite
import columnar.schema.{Schema, Column, DataType}
import columnar.writer.ColumnIndexEntry
import java.io.{File, RandomAccessFile}
import java.nio.file.Files

class FileReaderSpec extends AnyFunSuite:

  private def writeFakeFile(path: String): Unit =
    val raf = new RandomAccessFile(new File(path), "rw")
    try
      // HEADER (magic, version, colCount=1, rowCount=2)
      raf.write("COLF".getBytes())
      raf.writeByte(1)
      raf.writeInt(1)
      raf.writeInt(2)

      // INDEX (24 bytes)
      val metadataOffset = 13 + 24
      val dataOffset = metadataOffset + 4 + 2 + 1 + 1
      val dataSize = 8 // two ints
      raf.writeLong(metadataOffset)
      raf.writeLong(dataOffset)
      raf.writeInt(dataSize)
      raf.writeInt(0) // padding

      // METADATA (id -> Int)
      raf.writeInt(2)           // name length
      raf.write("id".getBytes())
      raf.writeByte(1)          // type code = Int
      raf.writeByte(0)          // nullable = false

      // DATA (two integers)
      raf.writeInt(8)           // size prefix
      raf.writeInt(1)
      raf.writeInt(2)
    finally raf.close()

  test("FileReader should correctly read header, schema, and data") {
    val tmp = Files.createTempFile("coltest", ".colf").toFile
    writeFakeFile(tmp.getAbsolutePath)
    val reader = new FileReader(tmp.getAbsolutePath)

    val names = reader.readColumnNames()
    assert(names == Seq("id"))

    val data = reader.readColumn("id").flatten
    assert(data == Seq(1, 2))

    val rows = reader.readAllColumns()
    assert(rows == Seq(
      Map("id" -> Some(1)),
      Map("id" -> Some(2))
    ))

    tmp.delete()
  }
