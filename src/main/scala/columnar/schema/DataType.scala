package columnar.schema

enum DataType(val typeCode: Int):
  case IntegerType extends DataType(1)
  case StringType extends DataType(2)
  case BooleanType extends DataType(3)

  def isFixedSize: Boolean = this match
    case IntegerType => true
    case StringType => false
    case BooleanType => true

  def fixedSizeBytes: Option[Int] = this match
    case IntegerType => Some(4)
    case StringType => None
    case BooleanType => Some(1)

  def typeName: String = this match
    case IntegerType => "Integer"
    case StringType => "String"
    case BooleanType => "Boolean"

object DataType:
  def fromTypeCode(code: Int): Option[DataType] = code match
    case 1 => Some(IntegerType)
    case 2 => Some(StringType)
    case 3 => Some(BooleanType)
    case _ => None
