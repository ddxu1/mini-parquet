package columnar.format

import org.xerial.snappy.Snappy

/**
 * Compression codec for columnar data.
 */
enum CompressionCodec(val codecId: Byte):
  case NoCompression extends CompressionCodec(0)
  case SnappyCompression extends CompressionCodec(1)

  /**
   * Compress data using this codec.
   */
  def compress(data: Array[Byte]): Array[Byte] = this match
    case NoCompression => data
    case SnappyCompression => Snappy.compress(data)

  /**
   * Decompress data using this codec.
   */
  def decompress(compressed: Array[Byte]): Array[Byte] = this match
    case NoCompression => compressed
    case SnappyCompression => Snappy.uncompress(compressed)

  /**
   * Human-readable name.
   */
  def name: String = this match
    case NoCompression => "none"
    case SnappyCompression => "snappy"

end CompressionCodec

object CompressionCodec:
  /**
   * Get codec from byte ID.
   */
  def fromId(id: Byte): Option[CompressionCodec] = id match
    case 0 => Some(NoCompression)
    case 1 => Some(SnappyCompression)
    case _ => None
