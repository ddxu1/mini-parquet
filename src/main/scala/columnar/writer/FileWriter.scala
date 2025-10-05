package columnar.writer

import java.io.{FileOutputStream, DataOutputStream}
import columnar.schema.{Schema, Column, DataType}

/**
 * Writes columnar data to a file with an index for random access.
 */
class FileWriter(schema: Schema):

    /** Builds a null bitmap (1 bit per row, 1 = NULL) */
  private def buildNullBitmap(values: Seq[Option[Any]]): Array[Byte] =
    val n = values.length
    val bytes = new Array[Byte]((n + 7) / 8)
    var i = 0
    while i < n do
      if values(i).isEmpty then
        val b = i >>> 3 // byte index
        val bit = i & 7 // bit index (0–7)
        bytes(b) = (bytes(b) | (1 << bit)).toByte
      i += 1
    bytes

  /**
   * Convert row data to column data
   */
  def rowsToColumns(rows: Seq[Map[String, Option[Any]]]): Seq[ColumnChunk] =
    schema.columns.map { column =>
      val columnName = column.name
      val allValues = rows.map(row => row.getOrElse(columnName, None))

      val bitmap = buildNullBitmap(allValues)
      val encoded = allValues.flatMap(v => Encoder.encodeValue(v, column.dataType))
      val combined = bitmap ++ encoded.toArray // bitmap, then column data encodings
      ColumnChunk(column, combined, rows.size)
    }

  /**
   * Calculates the size of metadata for a single column.
   */
  def calculateMetadataSize(column: Column): Int =
    val nameBytes = column.name.getBytes("UTF-8")
      4 +                  // name length (Int)
      nameBytes.length +   // name bytes
      1 +                  // type code (Byte)
      1                    // nullable flag (Byte)

  /**
   * Calculates all offsets and builds index entries.
   * This is the key method that determines where everything goes!
   */
  def buildIndexEntries(chunks: Seq[ColumnChunk]): Seq[ColumnIndexEntry] =
    // Header (13 bytes)
    // Index (# Columns * 24 bytes)
    // Metadata (variable length)
    // Data (Variable length)

    // Fixed sizes
    val headerSize = 13
    val indexSize = schema.columnCount * ColumnIndexEntry.ENTRY_SIZE

    // Calculate metadata sizes for all columns
    val metadataSizes = schema.columns.map(calculateMetadataSize)
    val totalMetadataSize = metadataSizes.sum

    // Metadata section starts after header and index
    val metadataStartOffset = headerSize + indexSize

    // Data section starts after metadata
    val dataStartOffset = metadataStartOffset + totalMetadataSize

    // Build index entries
    var currentMetadataOffset = metadataStartOffset
    var currentDataOffset = dataStartOffset

    chunks.zip(metadataSizes).map { case (chunk, metadataSize) =>
      // This column's metadata position
      val metadataOffset = currentMetadataOffset
      currentMetadataOffset += metadataSize

      // This column's data position
      val dataOffset = currentDataOffset
      val dataSize = chunk.sizeInBytes
      currentDataOffset += 4 + dataSize  // 4 bytes for size prefix + actual data

      ColumnIndexEntry(metadataOffset, dataOffset, dataSize)
    }

  /**
   * Writes the file header.
   * Format:
   * - Magic bytes (4 bytes): "COLF"
   * - Version (1 byte): 0x01
   * - Number of columns (4 bytes)
   * - Number of rows (4 bytes)
   */
  def writeHeader(
                           out: DataOutputStream,
                           columnCount: Int,
                           rowCount: Int
                         ): Unit =
    out.writeBytes("COLF")
    out.writeByte(1)
    out.writeInt(columnCount)
    out.writeInt(rowCount)

  /**
   * Writes the index section.
   * Each entry is 24 bytes (fixed size).
   */
  def writeIndex(
                          out: DataOutputStream,
                          indexEntries: Seq[ColumnIndexEntry]
                        ): Unit =
    indexEntries.foreach { entry =>
      entry.writeToStream(out)
    }

  /**
   * Writes schema metadata for one column.
   * Format:
   * - Column name length (4 bytes)
   * - Column name (UTF-8 bytes)
   * - Data type code (1 byte)
   * - Nullable flag (1 byte): 1 = nullable, 0 = not nullable
   */
  private def writeColumnMetadata(out: DataOutputStream, column: Column): Unit =
    val nameBytes = column.name.getBytes("UTF-8")
    out.writeInt(nameBytes.length) // name size
    out.write(nameBytes) // name
    out.writeByte(column.dataType.typeCode) // data type
    out.writeByte(if column.nullable then 1 else 0) // nullable

  /**
   * Writes all column data to the file.
   * For each column:
   * - Size of data (4 bytes)
   * - Actual data bytes
   */
  private def writeColumnData(out: DataOutputStream, chunks: Seq[ColumnChunk]): Unit =
    chunks.foreach { chunk =>
      out.writeInt(chunk.sizeInBytes) // size
      out.write(chunk.values) // data
    }

  /**
   * Main write method - orchestrates everything.
   *
   * Steps:
   * 1. Convert rows to columnar format
   * 2. Calculate offsets and build index
   * 3. Write header
   * 4. Write index
   * 5. Write metadata
   * 6. Write data
   */
  def write(rows: Seq[Map[String, Option[Any]]], filePath: String): Unit =
    val chunks = rowsToColumns(rows)
    val indexEntries = buildIndexEntries(chunks) // Build index with calculated offsets

    val fos = new FileOutputStream(filePath)
    val out = new DataOutputStream(fos)

    try
      writeHeader(out, schema.columnCount, rows.size)
      writeIndex(out, indexEntries)
      schema.columns.foreach(col => writeColumnMetadata(out, col))
      writeColumnData(out, chunks)

      println(s"Successfully wrote file: $filePath")
      println(s"  Columns: ${schema.columnCount}")
      println(s"  Rows: ${rows.size}")
      println(s"  Index entries: ${indexEntries.size}")

    finally
      out.close()
      fos.close()

end FileWriter