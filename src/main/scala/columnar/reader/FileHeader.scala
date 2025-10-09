package columnar.reader

import columnar.format.CompressionCodec

case class FileHeader(
                       version: Byte,
                       compression: CompressionCodec,
                       columnCount: Int,
                       rowCount: Int
                     )
