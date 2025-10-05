package columnar.integration

import org.scalatest.funsuite.AnyFunSuite
import columnar.schema.{Schema, Column, DataType}
import columnar.writer.FileWriter
import columnar.reader.FileReader
import java.nio.file.Files

class RoundTripSpec extends AnyFunSuite:

  test("FileWriter then FileReader should round-trip correctly") {
    val tmpFile = Files.createTempFile("roundtrip", ".colf").toFile

    val schema = Schema(List(
      Column("id", DataType.IntegerType, false),
      Column("name", DataType.StringType, true),
      Column("active", DataType.BooleanType, false)
    ))

    val rows = Seq(
      Map("id" -> Some(1), "name" -> Some("Alice"), "active" -> Some(true)),
      Map("id" -> Some(2), "name" -> Some("Bob"),   "active" -> Some(false))
    )

    println(s"Before write:\n${rows.mkString("\n")}")
    val writer = new FileWriter(schema)
    writer.write(rows, tmpFile.getAbsolutePath)

    val reader = new FileReader(tmpFile.getAbsolutePath)
    val readRows = reader.readAllColumns()
    println(s"\nAfter read:\n${readRows.mkString("\n")}")

    assert(readRows == rows)
    tmpFile.delete()
  }


  test("FileWriter/FileReader complex round-trip with nulls, UTF-8, and edge values") {
    val tmpFile = Files.createTempFile("roundtrip_complex", ".colf").toFile

    val schema = Schema(List(
      Column("user_id", DataType.IntegerType, false),
      Column("name", DataType.StringType, true),
      Column("age", DataType.IntegerType, true),
      Column("subscribed", DataType.BooleanType, false)
    ))

    val rows: Seq[Map[String, Option[Any]]] = Seq(
      Map("user_id" -> Some(1), "name" -> Some("Alice 😊"), "age" -> Some(25), "subscribed" -> Some(true)),
      Map("user_id" -> Some(2), "name" -> Some("Bob"), "age" -> None, "subscribed" -> Some(false)),
      Map("user_id" -> Some(3), "name" -> Some("Chloé"), "age" -> Some(30), "subscribed" -> Some(true)),
      Map("user_id" -> Some(4), "name" -> None, "age" -> Some(-5), "subscribed" -> Some(false)),
      Map("user_id" -> Some(5), "name" -> Some("世界"), "age" -> Some(42), "subscribed" -> Some(true))
    )


    println(s"Before write:\n${rows.mkString("\n")}")
    val writer = new FileWriter(schema)
    writer.write(rows, tmpFile.getAbsolutePath)

    val reader = new FileReader(tmpFile.getAbsolutePath)
       val readRows = reader.readAllColumns()
    println(s"\nAfter read:\n${readRows.mkString("\n")}")

    assert(readRows == rows)
    tmpFile.delete()
  }