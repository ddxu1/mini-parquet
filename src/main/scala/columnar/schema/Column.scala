package columnar.schema

case class Column(
  name: String,
  dataType: DataType,
  nullable: Boolean = true
                 ):
  require(name.nonEmpty, "Column name cannot be empty")

   def description: String =
     val nullStr = if nullable then "NULL" else "NOT NULL"
     s"$name: ${dataType.typeName} ($nullStr)"

   def isFixedSize: Boolean = dataType.isFixedSize

   def sizeBytes: Option[Int] = dataType.fixedSizeBytes

end Column
