package columnar.reader

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

object Decoder:
  def decodeInt(bytes: Array[Byte]): Int =
    ByteBuffer.wrap(bytes).getInt

  def decodeString(bytes: Array[Byte]): String =
    val buffer = ByteBuffer.wrap(bytes)
    val length = buffer.getInt()  // reads 4 bytes from pos 0, goes to pos 4
    val strBytes = new Array[Byte](length)
    buffer.get(strBytes) // reads next `length` bytes from position 4 into strBytes
    String(strBytes, StandardCharsets.UTF_8) // decode using utf8

  def decodeBoolean(bytes: Array[Byte]): Boolean =
    bytes.nonEmpty && bytes(0) == 1.toByte

