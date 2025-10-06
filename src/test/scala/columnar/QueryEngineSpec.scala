package columnar

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import columnar.query.*
import columnar.schema.{Schema, Column, DataType}
import columnar.writer.FileWriter
import columnar.reader.FileReader
import java.nio.file.Files
import java.io.File

class QueryEngineSpec extends AnyFlatSpec with Matchers:

  // Helper to create a test file with sample data
  def withTestData(test: (QueryEngine, File) => Unit): Unit =
    val tmpFile = Files.createTempFile("query_test", ".colf").toFile

    val schema = Schema(List(
      Column("id", DataType.IntegerType, false),
      Column("name", DataType.StringType, true),
      Column("age", DataType.IntegerType, true),
      Column("active", DataType.BooleanType, false)
    ))

    val rows: Seq[Map[String, Option[Any]]] = Seq(
      Map("id" -> Some(1), "name" -> Some("Alice"), "age" -> Some(30), "active" -> Some(true)),
      Map("id" -> Some(2), "name" -> Some("Bob"), "age" -> Some(25), "active" -> Some(false)),
      Map("id" -> Some(3), "name" -> Some("Carol"), "age" -> Some(35), "active" -> Some(true)),
      Map("id" -> Some(4), "name" -> None, "age" -> Some(28), "active" -> Some(false)),
      Map("id" -> Some(5), "name" -> Some("Dave"), "age" -> None, "active" -> Some(true))
    )

    try
      val writer = new FileWriter(schema)
      writer.write(rows, tmpFile.getAbsolutePath)
      val reader = new FileReader(tmpFile.getAbsolutePath)
      val engine = new QueryEngine(reader)
      test(engine, tmpFile)
    finally
      tmpFile.delete()

  "QueryEngine" should "filter rows with Equals predicate" in withTestData { (engine, _) =>
    val results = engine.filter(Equals("name", "Alice")).collect()
    results.size shouldBe 1
    results.head("name") shouldBe Some("Alice")
  }

  it should "filter rows with GreaterThan predicate" in withTestData { (engine, _) =>
    val results = engine.filter(GreaterThan("age", 30)).collect()
    results.size shouldBe 1
    results.head("name") shouldBe Some("Carol")
    results.head("age") shouldBe Some(35)
  }

  it should "filter rows with LessThan predicate" in withTestData { (engine, _) =>
    val results = engine.filter(LessThan("age", 30)).collect()
    results.size shouldBe 2
    results.map(_("name")) should contain allOf (Some("Bob"), None)
  }

  it should "filter rows with And combinator" in withTestData { (engine, _) =>
    val predicate = And(GreaterThan("age", 25), Equals("active", true))
    val results = engine.filter(predicate).collect()
    results.size shouldBe 2
    results.map(_("name")) should contain allOf (Some("Alice"), Some("Carol"))
  }

  it should "filter rows with Or combinator" in withTestData { (engine, _) =>
    val predicate = Or(Equals("name", "Alice"), Equals("name", "Bob"))
    val results = engine.filter(predicate).collect()
    results.size shouldBe 2
  }

  it should "filter rows with Not combinator" in withTestData { (engine, _) =>
    val results = engine.filter(Not(Equals("active", true))).collect()
    results.size shouldBe 2
    results.forall(_("active") == Some(false)) shouldBe true
  }

  it should "filter rows with IsNull predicate" in withTestData { (engine, _) =>
    val results = engine.filter(IsNull("name")).collect()
    results.size shouldBe 1
    results.head("id") shouldBe Some(4)
  }

  it should "filter rows with IsNotNull predicate" in withTestData { (engine, _) =>
    val results = engine.filter(IsNotNull("age")).collect()
    results.size shouldBe 4
  }

  it should "select specific columns" in withTestData { (engine, _) =>
    val results = engine.select("id", "name").collect()
    results.size shouldBe 5
    results.head.keys should contain only ("id", "name")
  }

  it should "chain filter and select operations" in withTestData { (engine, _) =>
    val results = engine
      .filter(GreaterThan("age", 25))
      .select("name", "age")
      .collect()

    results.size shouldBe 3
    results.head.keys should contain only ("name", "age")
    results.forall { row =>
      row.get("age") match
        case Some(Some(a: Int)) => a > 25
        case _ => false
    } shouldBe true
  }

  it should "limit results" in withTestData { (engine, _) =>
    val results = engine.limit(2).collect()
    results.size shouldBe 2
  }

  it should "skip results" in withTestData { (engine, _) =>
    val results = engine.skip(3).collect()
    results.size shouldBe 2
  }

  it should "count rows" in withTestData { (engine, _) =>
    engine.count() shouldBe 5
  }

  it should "count rows after filtering" in withTestData { (engine, _) =>
    val count = engine.filter(Equals("active", true)).count()
    count shouldBe 3
  }

  it should "count non-null values in a column" in withTestData { (engine, _) =>
    engine.countNonNull("name") shouldBe 4
    engine.countNonNull("age") shouldBe 4
  }

  it should "calculate sum of integer column" in withTestData { (engine, _) =>
    val total = engine.sum("age")
    total shouldBe 118  // 30 + 25 + 35 + 28 (excluding NULL)
  }

  it should "calculate average of integer column" in withTestData { (engine, _) =>
    val average = engine.avg("age")
    average shouldBe Some(29.5)  // (30 + 25 + 35 + 28) / 4
  }

  it should "find minimum value" in withTestData { (engine, _) =>
    engine.min("age") shouldBe Some(25)
  }

  it should "find maximum value" in withTestData { (engine, _) =>
    engine.max("age") shouldBe Some(35)
  }

  it should "get distinct values" in withTestData { (engine, _) =>
    val distinctActive = engine.distinct("active")
    distinctActive should contain theSameElementsAs Seq(Some(true), Some(false))
  }

  it should "group by and count" in withTestData { (engine, _) =>
    val grouped = engine.groupByCount("active")
    grouped(Some(true)) shouldBe 3
    grouped(Some(false)) shouldBe 2
  }

  it should "work with Contains predicate" in withTestData { (engine, _) =>
    val results = engine.filter(Contains("name", "a")).collect()
    // "Carol" and "Dave" contain "a"
    results.size shouldBe 2
  }

  it should "work with StartsWith predicate" in withTestData { (engine, _) =>
    val results = engine.filter(StartsWith("name", "A")).collect()
    results.size shouldBe 1
    results.head("name") shouldBe Some("Alice")
  }

  it should "work with In predicate" in withTestData { (engine, _) =>
    val results = engine.filter(In("name", Set("Alice", "Bob", "Unknown"))).collect()
    results.size shouldBe 2
  }

  it should "handle complex chained operations" in withTestData { (engine, _) =>
    val results = engine
      .filter(IsNotNull("age"))
      .filter(GreaterThan("age", 25))
      .select("name", "age")
      .limit(2)
      .collect()

    results.size shouldBe 2
    results.head.keys should contain only ("name", "age")
  }

  it should "get column names from schema" in withTestData { (engine, _) =>
    val columns = engine.columnNames()
    columns should contain theSameElementsAs Seq("id", "name", "age", "active")
  }

  it should "handle empty results" in withTestData { (engine, _) =>
    val results = engine.filter(Equals("name", "NonExistent")).collect()
    results shouldBe empty
  }

  it should "handle aggregations on empty results" in withTestData { (engine, _) =>
    val filtered = engine.filter(Equals("name", "NonExistent"))
    filtered.count() shouldBe 0
    filtered.sum("age") shouldBe 0
    filtered.avg("age") shouldBe None
    filtered.min("age") shouldBe None
    filtered.max("age") shouldBe None
  }

end QueryEngineSpec
