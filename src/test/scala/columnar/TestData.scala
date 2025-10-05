package columnar

import columnar.schema.*

/**
 * Helper object to create test data for testing.
 * Only available in test scope.
 */
object TestData:

  /**
   * Creates a simple schema with three common columns.
   * Table: "users"
   * Columns:
   *   - id: IntegerType, not nullable
   *   - name: StringType, nullable
   *   - active: BooleanType, not nullable
   */
  def simpleSchema: Schema = Schema(
    Seq(
      Column("id", DataType.IntegerType, nullable = false),
      Column("name", DataType.StringType, nullable = true),
      Column("active", DataType.BooleanType, nullable = false)
    ),
    Some("users")
  )

  /**
   * Creates sample row data as maps.
   * Each map represents one row.
   *
   * Returns 3 rows:
   *   Row 1: id=1, name="Alice", active=true
   *   Row 2: id=2, name="Bob", active=false
   *   Row 3: id=3, name="Carol", active=true
   */
  def sampleRows: Seq[Map[String, Option[Any]]] = Seq(
    Map("id" -> Some(1), "name" -> Some("Alice"), "active" -> Some(true)),
    Map("id" -> Some(2), "name" -> Some("Bob"), "active" -> Some(false)),
    Map("id" -> Some(3), "name" -> Some("Carol"), "active" -> Some(true))
  )

  /**
   * Creates sample data with a NULL value.
   *
   * Returns 2 rows:
   *   Row 1: id=1, name="Alice", active=true
   *   Row 2: id=2, name=NULL, active=false
   */
  def rowsWithNull: Seq[Map[String, Option[Any]]] = Seq(
    Map("id" -> Some(1), "name" -> Some("Alice"), "active" -> Some(true)),
    Map("id" -> Some(2), "name" -> None, "active" -> Some(false))
  )

  /**
   * Creates a schema with only one integer column.
   */
  def singleColumnSchema: Schema = Schema(
    Seq(Column("value", DataType.IntegerType, nullable = false)),
    Some("simple")
  )

  /**
   * Creates rows for single column schema.
   */
  def singleColumnRows(count: Int): Seq[Map[String, Option[Any]]] =
    (1 to count).map(i => Map("value" -> Some(i)))

  /**
   * Creates a schema with all data types.
   */
  def allTypesSchema: Schema = Schema(
    Seq(
      Column("int_col", DataType.IntegerType, nullable = false),
      Column("str_col", DataType.StringType, nullable = true),
      Column("bool_col", DataType.BooleanType, nullable = false)
    ),
    Some("all_types")
  )

  /**
   * Creates rows with various data.
   */
  def variedRows: Seq[Map[String, Option[Any]]] = Seq(
    Map("int_col" -> Some(100), "str_col" -> Some("hello"), "bool_col" -> Some(true)),
    Map("int_col" -> Some(-50), "str_col" -> Some(""), "bool_col" -> Some(false)),
    Map("int_col" -> Some(0), "str_col" -> None, "bool_col" -> Some(true))
  )

end TestData