package columnar.reader

import org.scalatest.funsuite.AnyFunSuite
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

class DecoderSpec extends AnyFunSuite:

  test("decodeInt should convert 4-byte big-endian integers") {
    val bytes = ByteBuffer.allocate(4).putInt(42).array()
    assert(Decoder.decodeInt(bytes) == 42)
  }

  test("decodeBoolean should decode true/false correctly") {
    assert(Decoder.decodeBoolean(Array(1.toByte)))
    assert(!Decoder.decodeBoolean(Array(0.toByte)))
    assert(!Decoder.decodeBoolean(Array.emptyByteArray))
  }

  test("decodeString should decode length-prefixed UTF-8 strings") {
    val s = "Alice"
    val data = s.getBytes(StandardCharsets.UTF_8)
    val encoded = ByteBuffer.allocate(4 + data.length).putInt(data.length).put(data).array()
    assert(Decoder.decodeString(encoded) == "Alice")
  }

  test("decodeString should handle empty strings") {
    val encoded = ByteBuffer.allocate(4).putInt(0).array()
    assert(Decoder.decodeString(encoded) == "")
  }
