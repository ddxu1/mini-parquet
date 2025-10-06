package columnar.query

/**
 * Trait for filter predicates that can be applied to rows.
 * All predicates operate on rows read from the file (no file format changes needed).
 */
trait Predicate:
  def evaluate(row: Map[String, Option[Any]]): Boolean

/**
 * Equality predicate: column = value
 */
case class Equals(column: String, value: Any) extends Predicate:
  def evaluate(row: Map[String, Option[Any]]): Boolean =
    row.get(column).flatten.contains(value)

/**
 * Inequality predicate: column != value
 */
case class NotEquals(column: String, value: Any) extends Predicate:
  def evaluate(row: Map[String, Option[Any]]): Boolean =
    !row.get(column).flatten.contains(value)

/**
 * Greater than predicate: column > value
 * Works with Int comparisons.
 */
case class GreaterThan(column: String, value: Int) extends Predicate:
  def evaluate(row: Map[String, Option[Any]]): Boolean =
    row.get(column).flatten match
      case Some(v: Int) => v > value
      case _ => false

/**
 * Less than predicate: column < value
 */
case class LessThan(column: String, value: Int) extends Predicate:
  def evaluate(row: Map[String, Option[Any]]): Boolean =
    row.get(column).flatten match
      case Some(v: Int) => v < value
      case _ => false

/**
 * Greater than or equal predicate: column >= value
 */
case class GreaterThanOrEqual(column: String, value: Int) extends Predicate:
  def evaluate(row: Map[String, Option[Any]]): Boolean =
    row.get(column).flatten match
      case Some(v: Int) => v >= value
      case _ => false

/**
 * Less than or equal predicate: column <= value
 */
case class LessThanOrEqual(column: String, value: Int) extends Predicate:
  def evaluate(row: Map[String, Option[Any]]): Boolean =
    row.get(column).flatten match
      case Some(v: Int) => v <= value
      case _ => false

/**
 * NULL check predicate: column IS NULL
 */
case class IsNull(column: String) extends Predicate:
  def evaluate(row: Map[String, Option[Any]]): Boolean =
    row.get(column).flatten.isEmpty

/**
 * NOT NULL check predicate: column IS NOT NULL
 */
case class IsNotNull(column: String) extends Predicate:
  def evaluate(row: Map[String, Option[Any]]): Boolean =
    row.get(column).flatten.isDefined

/**
 * Logical AND combinator
 */
case class And(left: Predicate, right: Predicate) extends Predicate:
  def evaluate(row: Map[String, Option[Any]]): Boolean =
    left.evaluate(row) && right.evaluate(row)

/**
 * Logical OR combinator
 */
case class Or(left: Predicate, right: Predicate) extends Predicate:
  def evaluate(row: Map[String, Option[Any]]): Boolean =
    left.evaluate(row) || right.evaluate(row)

/**
 * Logical NOT combinator
 */
case class Not(predicate: Predicate) extends Predicate:
  def evaluate(row: Map[String, Option[Any]]): Boolean =
    !predicate.evaluate(row)

/**
 * String contains predicate (case-sensitive)
 */
case class Contains(column: String, substring: String) extends Predicate:
  def evaluate(row: Map[String, Option[Any]]): Boolean =
    row.get(column).flatten match
      case Some(s: String) => s.contains(substring)
      case _ => false

/**
 * String starts with predicate
 */
case class StartsWith(column: String, prefix: String) extends Predicate:
  def evaluate(row: Map[String, Option[Any]]): Boolean =
    row.get(column).flatten match
      case Some(s: String) => s.startsWith(prefix)
      case _ => false

/**
 * IN predicate: column IN (values)
 */
case class In(column: String, values: Set[Any]) extends Predicate:
  def evaluate(row: Map[String, Option[Any]]): Boolean =
    row.get(column).flatten match
      case Some(v) => values.contains(v)
      case None => false
