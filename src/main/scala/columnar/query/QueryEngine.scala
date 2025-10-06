package columnar.query

import columnar.reader.FileReader

/**
 * Simple query engine for columnar files.
 * Operates on data after reading (no predicate pushdown in this version).
 *
 * Usage:
 *   val engine = new QueryEngine(new FileReader("data.colf"))
 *   val results = engine
 *     .filter(GreaterThan("age", 18))
 *     .select("name", "age")
 *     .limit(10)
 */
class QueryEngine(reader: FileReader):

  private var currentData: Seq[Map[String, Option[Any]]] = null

  /**
   * Lazily load all data from file.
   */
  private def loadData(): Seq[Map[String, Option[Any]]] =
    if currentData == null then
      currentData = reader.readAllColumns()
    currentData

  /**
   * Filter rows based on a predicate.
   * Returns a new QueryEngine with filtered data.
   */
  def filter(predicate: Predicate): QueryEngine =
    val filtered = loadData().filter(predicate.evaluate)
    QueryEngine.fromData(filtered, reader)

  /**
   * Select specific columns (projection).
   * Returns a new QueryEngine with only selected columns.
   */
  def select(columns: String*): QueryEngine =
    val columnSet = columns.toSet
    val projected = loadData().map { row =>
      row.filter { case (colName, _) => columnSet.contains(colName) }
    }
    QueryEngine.fromData(projected, reader)

  /**
   * Limit the number of rows returned.
   */
  def limit(n: Int): QueryEngine =
    val limited = loadData().take(n)
    QueryEngine.fromData(limited, reader)

  /**
   * Skip the first n rows.
   */
  def skip(n: Int): QueryEngine =
    val skipped = loadData().drop(n)
    QueryEngine.fromData(skipped, reader)

  /**
   * Count the number of rows.
   */
  def count(): Long =
    loadData().size

  /**
   * Count non-null values in a column.
   */
  def countNonNull(column: String): Long =
    loadData().count { row =>
      row.get(column).flatten.isDefined
    }

  /**
   * Sum values in an integer column.
   */
  def sum(column: String): Long =
    loadData().flatMap { row =>
      row.get(column).flatten match
        case Some(v: Int) => Some(v.toLong)
        case _ => None
    }.sum

  /**
   * Average of values in an integer column.
   */
  def avg(column: String): Option[Double] =
    val values = loadData().flatMap { row =>
      row.get(column).flatten match
        case Some(v: Int) => Some(v.toDouble)
        case _ => None
    }
    if values.isEmpty then None
    else Some(values.sum / values.size)

  /**
   * Minimum value in an integer column.
   */
  def min(column: String): Option[Int] =
    loadData().flatMap { row =>
      row.get(column).flatten match
        case Some(v: Int) => Some(v)
        case _ => None
    }.minOption

  /**
   * Maximum value in an integer column.
   */
  def max(column: String): Option[Int] =
    loadData().flatMap { row =>
      row.get(column).flatten match
        case Some(v: Int) => Some(v)
        case _ => None
    }.maxOption

  /**
   * Get distinct values from a column.
   */
  def distinct(column: String): Seq[Option[Any]] =
    loadData().map { row =>
      row.get(column) match
        case Some(opt) => opt
        case None => None
    }.distinct

  /**
   * Group by a column and count rows in each group.
   * Returns a map of value -> count.
   */
  def groupByCount(column: String): Map[Option[Any], Int] =
    loadData()
      .groupBy { row =>
        row.get(column) match
          case Some(opt) => opt
          case None => None
      }
      .view
      .mapValues(_.size)
      .toMap

  /**
   * Collect results as a sequence of rows.
   */
  def collect(): Seq[Map[String, Option[Any]]] =
    loadData()

  /**
   * Get column names from the schema.
   */
  def columnNames(): Seq[String] =
    reader.readColumnNames()

  /**
   * Pretty print results to console.
   */
  def show(numRows: Int = 20): Unit =
    val data = loadData().take(numRows)
    if data.isEmpty then
      println("No data")
      return

    val columns = data.head.keys.toSeq.sorted

    // Calculate column widths
    val widths = columns.map { col =>
      val dataWidth = data.map { row =>
        row.get(col).flatten.map(_.toString).getOrElse("NULL").length
      }.maxOption.getOrElse(0)
      col -> math.max(col.length, dataWidth)
    }.toMap

    // Print header
    val header = columns.map { col =>
      col.padTo(widths(col), ' ')
    }.mkString(" | ")
    println(header)
    println("-" * header.length)

    // Print rows
    data.foreach { row =>
      val rowStr = columns.map { col =>
        val value = row.get(col).flatten.map(_.toString).getOrElse("NULL")
        value.padTo(widths(col), ' ')
      }.mkString(" | ")
      println(rowStr)
    }

    val total = loadData().size
    if total > numRows then
      println(s"... (${total - numRows} more rows)")

    println(s"\n$total rows total")

end QueryEngine

object QueryEngine:
  /**
   * Create a QueryEngine from pre-loaded data.
   * Used internally for chaining operations.
   */
  private def fromData(data: Seq[Map[String, Option[Any]]], reader: FileReader): QueryEngine =
    val engine = new QueryEngine(reader)
    engine.currentData = data
    engine
