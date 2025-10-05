package columnar.writer

import java.io.{FileOutputStream, DataOutputStream}
import columnar.schema.{Schema, Column, DataType}

/**
 * Writes columnar data to a file with an index for random access.
 */
class FileWriter(schema: Schema):

  /**
   * Converts row-oriented data to column-oriented chunks.
   */
  private def rowsToColumns(rows: Seq[Map[String, Option[Any]]]): Seq[ColumnChunk] =
    schema.columns.map { column =>
      val columnName = column.name
      val allValues = rows.map(row => row.getOrElse(columnName, None))

      // Encode each value
      val encodedBytes = allValues.flatMap { value =>
        Encoder.encodeValue(value, column.dataType)
      }

      ColumnChunk(column, encodedBytes.toArray, rows.size)
    }

  /**
   * Calculates the size of metadata for a single column.
   */
  private def calculateMetadataSize(column: Column): Int =
    val nameBytes = column.name.getBytes("UTF-8")
    4 +                  // name length (Int)
      nameBytes.length +   // name bytes
      1 +                  // type code (Byte)
      1                    // nullable flag (Byte)

  /**
   * Calculates all offsets and builds index entries.
   * This is the key method that determines where everything goes!
   */
  private def buildIndexEntries(chunks: Seq[ColumnChunk]): Seq[ColumnIndexEntry] =
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
  private def writeHeader(
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
  private def writeIndex(
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
    out.writeInt(nameBytes.length)
    out.write(nameBytes)
    out.writeByte(column.dataType.typeCode)
    out.writeByte(if column.nullable then 1 else 0)

  /**
   * Writes all column data to the file.
   * For each column:
   * - Size of data (4 bytes)
   * - Actual data bytes
   */
  private def writeColumnData(out: DataOutputStream, chunks: Seq[ColumnChunk]): Unit =
    chunks.foreach { chunk =>
      // Write the size of this column's data
      out.writeInt(chunk.sizeInBytes)

      // Write the actual data bytes
      out.write(chunk.values)
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
    // Step 1: Convert rows to columns
    val chunks = rowsToColumns(rows)

    // Step 2: Build index with calculated offsets
    val indexEntries = buildIndexEntries(chunks)

    // Step 3: Open file and write everything
    val fos = new FileOutputStream(filePath)
    val out = new DataOutputStream(fos)

    try
      // Write header
      writeHeader(out, schema.columnCount, rows.size)

      // Write index (NEW!)
      writeIndex(out, indexEntries)

      // Write schema metadata for each column
      schema.columns.foreach(col => writeColumnMetadata(out, col))

      // Write column data
      writeColumnData(out, chunks)

      println(s"Successfully wrote file: $filePath")
      println(s"  Columns: ${schema.columnCount}")
      println(s"  Rows: ${rows.size}")
      println(s"  Index entries: ${indexEntries.size}")

    finally
      out.close()
      fos.close()

end FileWriter