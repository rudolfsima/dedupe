package dedupe

import dedupe.RecursiveChungusDedupeTest.{Company, Employee}
import org.scalatest.funsuite.AnyFunSuite

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.file.Files
import scala.util.Random
import scala.util.hashing.MurmurHash3

class RecursiveChungusDedupeTest extends AnyFunSuite {

  private implicit val companyCodec = new DedupeCodec[Company] {
    override def encode(record: Company): Array[Byte] = {
      val bOut = new ByteArrayOutputStream()
      val oOut = new ObjectOutputStream(bOut)
      try {
        oOut.writeObject(record)
        oOut.flush()
        bOut.toByteArray
      } finally oOut.close()
    }
    override def decode(bytes: Array[Byte], offset: Int, length: Int): Company = {
      val bIn = new ByteArrayInputStream(bytes, offset, length)
      val oIn = new ObjectInputStream(bIn)
      try {
        oIn.readObject().asInstanceOf[Company]
      } finally oIn.close()
    }
  }

  private def newMemStore(i: Int) = new MemoryChunkStore[Company]()
  private val baseDir = Files.createTempDirectory("recursive-chungus-dedupe-dedupe-test-")
  private def newFileStore(i: Int) = new FileChunkStore[Company](baseDir resolve i.toString)

  private def runParametrized(maxSortableSize: Int, chunkStoreFactory: Int => ChunkStore[Company]) = {
    val expectedResult = Seq(
      Company("Acme", Seq(
        Employee("Alice", 100),
        Employee("Bob", 200),
        Employee("Charlie", 300),
        Employee("Dave", 400),
        Employee("Eve", 500),
        Employee("Frank", 600),
        Employee("Grace", 700),
        Employee("Heidi", 800),
        Employee("Ivan", 900),
        Employee("Judy", 1000)
      )),
      Company("Globex", Seq(
        Employee("Judy", 1000),
        Employee("Karl", 1100),
        Employee("Linda", 1200),
        Employee("Mike", 1300),
        Employee("Nancy", 1400),
        Employee("Oscar", 1500),
        Employee("Peggy", 1600),
        Employee("Quentin", 1700),
        Employee("Rita", 1800),
        Employee("Steve", 1900),
        Employee("Tina", 2000)
      )),
      Company("Initech", Seq(
        Employee("Tina", 2000),
        Employee("Ursula", 2100),
        Employee("Victor", 2200),
        Employee("Wendy", 2300),
        Employee("Xavier", 2400),
        Employee("Yvonne", 2500),
        Employee("Zach", 2600)
      )),
      Company("Umbrella", Seq(
        Employee("Zach", 2600),
        Employee("Alice", 100),
        Employee("Bob", 200),
        Employee("Charlie", 300),
        Employee("Dave", 400),
        Employee("Eve", 500),
        Employee("Frank", 600),
        Employee("Grace", 700),
        Employee("Heidi", 800),
        Employee("Ivan", 900),
        Employee("Judy", 1000)
      )),
      Company("Virtucon", Seq(
        Employee("Judy", 1000),
        Employee("Karl", 1100),
        Employee("Linda", 1200),
        Employee("Mike", 1300),
        Employee("Nancy", 1400),
        Employee("Oscar", 1500),
        Employee("Peggy", 1600),
        Employee("Quentin", 1700),
        Employee("Rita", 1800),
        Employee("Steve", 1900),
        Employee("Tina", 2000)
      )),
      Company("Yoyodyne", Seq(
        Employee("Tina", 2000),
        Employee("Ursula", 2100),
        Employee("Victor", 2200),
        Employee("Wendy", 2300),
        Employee("Xavier", 2400),
        Employee("Yvonne", 2500),
        Employee("Zach", 2600)
      )),
      Company("Zorg", Seq(
        Employee("Zach", 2600),
        Employee("Alice", 100),
        Employee("Bob", 200),
        Employee("Charlie", 300),
        Employee("Dave", 400),
        Employee("Eve", 500),
        Employee("Frank", 600),
        Employee("Grace", 700),
        Employee("Heidi", 800),
        Employee("Ivan", 900),
        Employee("Judy", 1000)
      )),
      Company("Acmex", Seq(
        Employee("Judy", 1000),
        Employee("Karl", 1100),
        Employee("Linda", 1200),
        Employee("Mike", 1300),
        Employee("Nancy", 1400),
        Employee("Oscar", 1500),
        Employee("Peggy", 1600),
        Employee("Quentin", 1700),
        Employee("Rita", 1800),
        Employee("Steve", 1900),
        Employee("Tina", 2000)
      )),
      Company("Undep", Seq(
        Employee("Tina", 2000),
        Employee("Ursula", 2100),
        Employee("Victor", 2200),
        Employee("Wendy", 2300),
        Employee("Xavier", 2400),
        Employee("Yvonne", 2500),
        Employee("Zach", 2600)
      ))
    )

    val testInput = expectedResult.flatMap { company =>
      val chunks = Random.shuffle(company.employees).grouped(Random.nextInt(3) + 1).toSeq
      chunks map { chunk =>
        Company(company.name, chunk)
      }
    }

    implicit val trinity = new DedupeTrinity[Company] {
      override def calculateHash(record: Company, salt: Int): Int = MurmurHash3.stringHash(record.name, salt)
      override def areDuplicate(a: Company, b: Company): Boolean = a.name == b.name
      override def merge(a: Company, b: Company): Company = Company(a.name, a.employees ++ b.employees, a.totalSalaries + b.totalSalaries)
    }

    def normalize(cs: Seq[Company]): Seq[Company] = {
      cs
        .map(c => c.copy(employees = c.employees.sortBy(_.name)))
        .sortBy(_.name)
    }

    val serializedSize = testInput.map(c => companyCodec.encode(c).length).sum

    val dedupe = new RecursiveChungusDedupe[Company](serializedSize, maxSortableSize, chunkStoreFactory, DeduplicationEvent.noDebug(println) _)
    val actualResult = try {
      testInput foreach dedupe.add
      dedupe.iterator.toIndexedSeq
    } finally dedupe.close()
    assert(normalize(actualResult) === normalize(expectedResult))
  }

  test("merge test input using in memory chunk store") {
    runParametrized(8192, newMemStore)
  }


  test("merge test input using filesystem chunk store") {
    runParametrized(8192, newFileStore)
  }

}

object RecursiveChungusDedupeTest extends App {

  private case class Employee(name: String, salary: Int)
  private case class Company(name: String, employees: Seq[Employee], totalSalaries: Int) {
    override def toString = {
      s"""Company $name, total salaries: $totalSalaries
         |${employees.map(e => s"  ${e.name} ${e.salary}").mkString("\n")}
         |""".stripMargin
    }
  }
  private object Company {
    def apply(name: String, employees: Seq[Employee]): Company = Company(name, employees, employees.map(_.salary).sum)
  }

}