package columnar.schema

import columnar.format.CompressionCodec

// Row-based schema
case class Schema(
                 columns: Seq[Column],
                 tableName: Option[String] = None,
                 compression: CompressionCodec = CompressionCodec.NoCompression
                 ):
  require(columns.nonEmpty)
  private val columnNameSet = columns.map(_.name).toSet
  require(columnNameSet.size == columns.size, s"Column names must be unique, found duplicates: ${findDuplicates}")

  def columnNames: Seq[String] =
    columns.map(_.name)

  def columnCount: Int =
    columns.size

  def findColumn(name: String): Option[Column] =
    columns.find(_.name == name)

  def hasColumn(name: String): Boolean =
    columnNameSet.contains(name)

  def fixedSizeColumns: Seq[Column] =
    columns.filter(_.isFixedSize)

  def variableSizeColumns: Seq[Column] =
    columns.filter(!_.isFixedSize)

  def description: String =
    val header = tableName.map(n => s"Table: $n\n").getOrElse("")
    val columnDescs = columns.map(c => s"  - ${c.description}").mkString("\n")
    header + "Columns:\n" + columnDescs

  private def findDuplicates: Seq[String] =
    columns.groupBy(_.name).filter(_._2.size > 1).keys.toSeq

