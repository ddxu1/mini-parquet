package examples

import columnar.query.*
import columnar.schema.{Schema, Column, DataType}
import columnar.writer.FileWriter
import columnar.reader.FileReader
import java.nio.file.Files

/**
 * Example demonstrating the Arquet query engine.
 */
@main
def queryExample(): Unit =
  println("=== Arquet Query Engine Example ===\n")

  // Create sample data
  val schema = Schema(List(
    Column("id", DataType.IntegerType, false),
    Column("name", DataType.StringType, true),
    Column("age", DataType.IntegerType, true),
    Column("department", DataType.StringType, true),
    Column("active", DataType.BooleanType, false)
  ))

  val employees = Seq(
    Map("id" -> Some(1), "name" -> Some("Alice"), "age" -> Some(30), "department" -> Some("Engineering"), "active" -> Some(true)),
    Map("id" -> Some(2), "name" -> Some("Bob"), "age" -> Some(25), "department" -> Some("Sales"), "active" -> Some(true)),
    Map("id" -> Some(3), "name" -> Some("Carol"), "age" -> Some(35), "department" -> Some("Engineering"), "active" -> Some(false)),
    Map("id" -> Some(4), "name" -> Some("Dave"), "age" -> Some(28), "department" -> Some("Marketing"), "active" -> Some(true)),
    Map("id" -> Some(5), "name" -> Some("Eve"), "age" -> Some(42), "department" -> Some("Engineering"), "active" -> Some(true)),
    Map("id" -> Some(6), "name" -> Some("Frank"), "age" -> None, "department" -> Some("Sales"), "active" -> Some(false)),
    Map("id" -> Some(7), "name" -> None, "age" -> Some(31), "department" -> Some("Engineering"), "active" -> Some(true))
  )

  // Write to file
  val tmpFile = Files.createTempFile("employees", ".colf").toFile
  val writer = new FileWriter(schema)
  writer.write(employees, tmpFile.getAbsolutePath)
  println(s"✓ Written ${employees.size} employees to ${tmpFile.getName}\n")

  // Create query engine
  val reader = new FileReader(tmpFile.getAbsolutePath)
  val engine = new QueryEngine(reader)

  // Example 1: Simple filter
  println(">>> Query 1: Find active employees")
  println("SQL equivalent: SELECT * FROM employees WHERE active = true\n")
  engine.filter(Equals("active", true)).show(10)

  // Example 2: Filter with comparison
  println("\n>>> Query 2: Find employees older than 30")
  println("SQL equivalent: SELECT * FROM employees WHERE age > 30\n")
  engine.filter(GreaterThan("age", 30)).show(10)

  // Example 3: Complex filter with AND
  println("\n>>> Query 3: Find active engineers")
  println("SQL equivalent: SELECT * FROM employees WHERE active = true AND department = 'Engineering'\n")
  engine
    .filter(And(Equals("active", true), Equals("department", "Engineering")))
    .show(10)

  // Example 4: Column projection
  println("\n>>> Query 4: Get names and departments")
  println("SQL equivalent: SELECT name, department FROM employees\n")
  engine.select("name", "department").show(10)

  // Example 5: Filter + projection + limit
  println("\n>>> Query 5: Top 3 active employees (name and age only)")
  println("SQL equivalent: SELECT name, age FROM employees WHERE active = true LIMIT 3\n")
  engine
    .filter(Equals("active", true))
    .select("name", "age")
    .limit(3)
    .show(10)

  // Example 6: NULL handling
  println("\n>>> Query 6: Find employees with missing age")
  println("SQL equivalent: SELECT * FROM employees WHERE age IS NULL\n")
  engine.filter(IsNull("age")).show(10)

  // Example 7: Aggregations
  println("\n>>> Aggregations:")
  println(s"Total employees: ${engine.count()}")
  println(s"Employees with age data: ${engine.countNonNull("age")}")
  println(s"Average age: ${engine.avg("age").map(a => f"$a%.1f").getOrElse("N/A")}")
  println(s"Min age: ${engine.min("age").getOrElse("N/A")}")
  println(s"Max age: ${engine.max("age").getOrElse("N/A")}")

  // Example 8: Group by
  println("\n>>> Query 8: Count employees by department")
  val byDept = engine.groupByCount("department")
  byDept.foreach { case (dept, count) =>
    println(s"  ${dept.getOrElse("Unknown")}: $count employees")
  }

  // Example 9: Complex chained query
  println("\n>>> Query 9: Active engineers with age > 25 (name and age only)")
  println("SQL equivalent: SELECT name, age FROM employees WHERE active = true AND department = 'Engineering' AND age > 25\n")
  engine
    .filter(Equals("active", true))
    .filter(Equals("department", "Engineering"))
    .filter(GreaterThan("age", 25))
    .select("name", "age")
    .show(10)

  // Cleanup
  tmpFile.delete()
  println("\n✓ Query example completed")
