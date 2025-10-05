package columnar.writer

import columnar.schema.Column

/**
 * Represents a single column's data encoded as bytes.
 * This is the in-memory representation before writing to file.
 *
 * @param column the column metadata (name, type, nullable)
 * @param values the encoded byte data for all values in this column
 * @param rowCount number of rows in this column
 */
case class ColumnChunk(
                        column: Column,
                        values: Array[Byte],
                        rowCount: Int
                      ):
  /**
   * Size of this column's data in bytes.
   */
  def sizeInBytes: Int = values.length

  /**
   * Returns a summary of this chunk.
   */
  def summary: String =
    s"Column '${column.name}': ${rowCount} rows, ${sizeInBytes} bytes"

end ColumnChunk