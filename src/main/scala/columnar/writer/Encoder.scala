package columnar.writer

import java.nio.ByteBuffer
import columnar.schema.DataType

object Encoder:

  def encodeInt(value: Int): Array[Byte] =
    ByteBuffer.allocate(4).putInt(value).array

  def encodeString(value: String): Array[Byte] =
    val strBytes = value.getBytes("UTF-8")
    val length = strBytes.size
    val buffer = ByteBuffer.allocate(length + 4)
    buffer.putInt(length)
    buffer.put(strBytes, 0, length)
    buffer.array()

  def encodeBoolean(value: Boolean): Array[Byte] =
      Array[Byte](if value then 1 else 0)

  def encodeValue(value: Option[Any], dataType: DataType): Array[Byte] =
    value match
      case None => Array.empty[Byte]
      case Some(v) => dataType match
        case DataType.IntegerType => encodeInt(v.asInstanceOf[Int])
        case DataType.StringType  => encodeString(v.asInstanceOf[String])
        case DataType.BooleanType => encodeBoolean(v.asInstanceOf[Boolean])

end Encoder
