package columnar.reader

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import columnar.schema.{Column, DataType, Schema}
import columnar.writer.ColumnIndexEntry
import java.nio.charset.StandardCharsets
import scala.collection.mutable


class FileReader(filePath: String):
  private val raf = new RandomAccessFile(new File(filePath), "r")
  /** Reads 13 byte header */
  private def readHeader(): FileHeader =
    raf.seek(0) // just in case, ensure we start at 0
    val magic = new Array[Byte](4)
    raf.readFully(magic)
    val magicStr = String(magic, StandardCharsets.UTF_8)
    require(magicStr == "COLF", s"Invalid file format: magic bytes $magicStr")
    val version = raf.readByte()
    val columnCount = raf.readInt()
    val rowCount = raf.readInt()
    FileHeader(version, columnCount, rowCount)

  /** Reads 24 byte column index entry */
  private def readIndexEntries(colCount: Int): Seq[ColumnIndexEntry] =
    (0 until colCount).map { _ =>
      val metadataOffset = raf.readLong()
      val dataOffset = raf.readLong()
      val dataSize = raf.readInt()
      raf.skipBytes(4) // ignore padding
      ColumnIndexEntry(metadataOffset, dataOffset, dataSize)
    }

  private def readSchema(entries: Seq[ColumnIndexEntry]): Schema =
    val columns = entries.map { entry =>
      raf.seek(entry.metadataOffset)
      val nameLen = raf.readInt()
      val nameBytes = new Array[Byte](nameLen)
      raf.readFully(nameBytes)
      val name = String(nameBytes, StandardCharsets.UTF_8)
      val typeCode = raf.readByte()
      val nullable = raf.readByte() == 1.toByte
      val dataType = typeCode match
        case 1 => DataType.IntegerType
        case 2 => DataType.StringType
        case 3 => DataType.BooleanType
        case _ => throw new IllegalArgumentException(s"Unknown type code $typeCode")
      Column(name, dataType, nullable)
    }
    Schema(columns.toList)

  def readColumn(columnName: String): Seq[Option[Any]] =
    val header = readHeader()
    val index = readIndexEntries(header.columnCount)
    val schema = readSchema(index)
    val col = schema.columns.find(_.name == columnName).getOrElse(throw new IllegalArgumentException(s"Column $columnName not found"))
    val idx = schema.columns.indexOf(col)
    val entry = index(idx)
    raf.seek(entry.dataOffset)
    val dataSize = raf.readInt()
    val data = new Array[Byte](dataSize)
    raf.readFully(data)
    decodeColumn(data, col.dataType, header.rowCount)

  private def decodeColumn(data: Array[Byte], dataType: DataType, rowCount: Int): Seq[Option[Any]] =
      val buf = ByteBuffer.wrap(data)
      val bitmapLength = (rowCount + 7) / 8
      val bitmap = new Array[Byte](bitmapLength) // Read null bitmap (1 bit per row; 1 = NULL)
      if buf.remaining() < bitmapLength then return Seq.fill(rowCount)(None)
      buf.get(bitmap)

      inline def isNull(i: Int): Boolean =
          val b = bitmap(i >>> 3)
          val bit = i & 7
          (b & (1 << bit)).toByte != 0

      val out = scala.collection.mutable.ArrayBuffer.empty[Option[Any]]
      out.sizeHint(rowCount)  // Pre-allocate to avoid reallocations
      var i = 0
      while i < rowCount do
        if isNull(i) then out += None
        else
          dataType match
            case DataType.IntegerType =>
              if buf.remaining() >= 4 then out += Some(buf.getInt()) else out += None
            case DataType.BooleanType =>
              if buf.remaining() >= 1 then out += Some(buf.get() == 1.toByte) else out += None
            case DataType.StringType =>
              if buf.remaining() >= 4 then
                val len = buf.getInt()
                if len >= 0 && buf.remaining() >= len then
                  val strBytes = new Array[Byte](len)
                  buf.get(strBytes)
                  out += Some(String(strBytes, java.nio.charset.StandardCharsets.UTF_8))
                else out += None
              else out += None
            case null =>
              out += None
        i += 1
      out.toIndexedSeq  // Use IndexedSeq for O(1) random access

  def readAllColumns(): Seq[Map[String, Option[Any]]] =
    val header = readHeader()
    val index  = readIndexEntries(header.columnCount)
    val schema = readSchema(index)

    val dataByCol = schema.columns.map { col =>
      val entry = index(schema.columns.indexOf(col))
      raf.seek(entry.dataOffset)
      val size = raf.readInt()
      val bytes = new Array[Byte](size)
      raf.readFully(bytes)
      col.name -> decodeColumn(bytes, col.dataType, header.rowCount)
    }.toMap

    val n = header.rowCount
    (0 until n).map { i =>
      dataByCol.map { case (k, v) => k -> v(i) }
    }

  def readColumnNames(): Seq[String] =
    val header = readHeader()
    val index = readIndexEntries(header.columnCount)
    readSchema(index).columns.map(_.name)
