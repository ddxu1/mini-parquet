package columnar.reader

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import columnar.schema.{Column, DataType, Schema}
import columnar.writer.ColumnIndexEntry

import scala.collection.mutable


class FileReader(filePath: String):
  private val raf = new RandomAccessFile(new File(filePath), "r")
  /** Reads 13 byte header */
  private def readHeader(): FileHeader =
    val magic = new Array[Byte](4)
    raf.readFully(magic)
    val magicStr = String(magic)
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
      val name = String(nameBytes, java.nio.charset.StandardCharsets.UTF_8)
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
    dataType match
      case DataType.IntegerType =>
        (0 until rowCount).map {_ =>
          if buf.remaining() >= 4 then Some(buf.getInt()) else None
        }
      case DataType.BooleanType =>
        (0 until rowCount).map { _ =>
          if buf.remaining() >= 1 then Some(buf.get() == 1.toByte) else None
        }
      case DataType.StringType =>
        val values = collection.mutable.Buffer.empty[Option[Any]]
        var i = 0
        while buf.remaining() >= 4 && i < rowCount do
          val len = buf.getInt()
          val strBytes = new Array[Byte](len)
          buf.get(strBytes)
          values.append(Some(String(strBytes)))
          i += 1
        values.toSeq
      case _ =>
        throw new IllegalArgumentException(s"Unsupported type $dataType")


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
