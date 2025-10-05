package columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import columnar.writer.Encoder
import columnar.schema.DataType

class EncoderSpec extends AnyFlatSpec with Matchers:

  "Encoder" should "encode integers to 4 bytes big-endian" in {
    val bytes = Encoder.encodeInt(42)

    bytes.length shouldBe 4
    bytes shouldBe Array[Byte](0, 0, 0, 42)
  }

  it should "encode negative integers correctly" in {
    val bytes = Encoder.encodeInt(-1)

    bytes.length shouldBe 4
    // -1 in two's complement = 0xFF FF FF FF
    bytes shouldBe Array[Byte](-1, -1, -1, -1)
  }

  it should "encode large integers" in {
    val bytes = Encoder.encodeInt(256)

    bytes.length shouldBe 4
    bytes shouldBe Array[Byte](0, 0, 1, 0)
  }

  it should "encode strings with length prefix" in {
    val bytes = Encoder.encodeString("Bob")

    // 4 bytes for length + 3 bytes for "Bob"
    bytes.length shouldBe 7

    // First 4 bytes should be length (3)
    bytes.take(4) shouldBe Array[Byte](0, 0, 0, 3)

    // Next 3 bytes should be "Bob"
    bytes.drop(4) shouldBe Array[Byte]('B'.toByte, 'o'.toByte, 'b'.toByte)
  }

  it should "encode empty strings" in {
    val bytes = Encoder.encodeString("")

    bytes.length shouldBe 4  // Just the length prefix
    bytes shouldBe Array[Byte](0, 0, 0, 0)
  }

  it should "encode Unicode strings correctly" in {
    val bytes = Encoder.encodeString("café")

    // "café" in UTF-8: c=1, a=1, f=1, é=2 bytes = 5 bytes total
    bytes.length shouldBe 9  // 4 (length) + 5 (UTF-8)
    bytes.take(4) shouldBe Array[Byte](0, 0, 0, 5)
  }

  it should "encode booleans to 1 byte" in {
    val trueBytes = Encoder.encodeBoolean(true)
    val falseBytes = Encoder.encodeBoolean(false)

    trueBytes shouldBe Array[Byte](1)
    falseBytes shouldBe Array[Byte](0)
  }

  it should "encode values based on data type" in {
    // Test Integer
    val intBytes = Encoder.encodeValue(Some(100), DataType.IntegerType)
    intBytes.length shouldBe 4

    // Test String
    val stringBytes = Encoder.encodeValue(Some("Hi"), DataType.StringType)
    stringBytes.length shouldBe 6  // 4 (length) + 2 (chars)

    // Test Boolean
    val boolBytes = Encoder.encodeValue(Some(true), DataType.BooleanType)
    boolBytes.length shouldBe 1

    // Test NULL
    val nullBytes = Encoder.encodeValue(None, DataType.IntegerType)
    nullBytes.length shouldBe 0  // Empty for null
  }

end EncoderSpec