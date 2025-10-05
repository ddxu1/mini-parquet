package columnar.writer

import java.io.{FileOutputStream, DataOutputStream, DataInputStream}

case class ColumnIndexEntry(metadataOffset: Long, dataOffset: Long, dataSize: Int):
  // metadataOffset: where does metadata start, data offset: where does column data start, data size: how much column dat

  def writeToStream(out: DataOutputStream): Unit =
    out.writeLong(metadataOffset) // bytes 0-7
    out.writeLong(dataOffset) // 8-15
    out.writeInt(dataSize) // 16-19
    out.writeInt(0)  // 20-23, reserved padding

  def summary: String =
    s"ColumnIndexEntry(metadata@$metadataOffset, data@$dataOffset, size=$dataSize)"

end ColumnIndexEntry

object ColumnIndexEntry:
  val ENTRY_SIZE = 24  // 8 + 8 + 4 + 4

  def readFromStream(in: DataInputStream): ColumnIndexEntry =
    val metadataOffset = in.readLong()
    val dataOffset = in.readLong()
    val dataSize = in.readInt()
    in.readInt()  // skip reserved
    ColumnIndexEntry(metadataOffset, dataOffset, dataSize)

end ColumnIndexEntry
