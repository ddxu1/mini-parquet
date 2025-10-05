package columnar.reader

case class FileHeader(
                       version: Byte,
                       columnCount: Int,
                       rowCount: Int
                     )
