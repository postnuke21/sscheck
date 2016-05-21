package es.ucm.fdi.tfg
import org.specs2.Specification
import org.specs2.ScalaCheck

import org.scalacheck.Gen.Parameters
import org.scalacheck._

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import org.specs2.execute._

import java.io._
import java.nio.file.{ Paths, Files }

import es.ucm.fdi.sscheck.spark.SharedSparkContextBeforeAfterAll

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import com.databricks.spark.avro._

class IncisiveProp(impSC: SparkContext) {

  /** val where counterexamples will be stored */
  val counterExamplePath = "counterExamples.txt"

  /** Data necessary to define the enviroment where the sampling (using FromRDDReplacementGen)will take place*/
  val sqlContext = new SQLContext(impSC)
  val defaultToLast = true
  val withRepl = false
  val bufferSize = 5

  def forAll[A](g1: Gen[A], filePath: String)(f: A => Prop): Prop = {

    /**
     * We need zipWithUniqueId() because we might encounter rows with the same value , despite the structure of the data we
     * might encounter rows with the same values by using transformations upon the data , that's why we neeed a unique id for each single entry.
     */
   
    /**Commented these 2 lines to test it first with a simple generator*/  
    //var rdd = sqlContext.read.avro(filePath).rdd.zipWithUniqueId()
    //var mixedGen : Gen[A] = FromRDDReplacementGen(rdd, bufferSize, defaultToLast, withRepl, impSC).asInstanceOf[A]
    
    //Asigned to Gen.fail to test if the generator switching implemented below works
    var mixedGen : Gen[A] = Gen.fail
    //Llamar al generador y en cuanto obtengamos un NONE coger de G1 
    mixedGen.sample match{
      case None =>  println("No remaining samples left switching to given Gen"); mixedGen = g1    
      case Some(_) => println("There are still val 	ues")
    }
    
    val prop = Prop.forAll(mixedGen)(f) 
    new Prop {
      def apply(prms: org.scalacheck.Gen.Parameters): org.scalacheck.Prop.Result = {
        val res = prop.apply(prms)
        println(s"\tres ${res}")

        if ((res.status == Prop.False) || (res.status.isInstanceOf[Prop.Exception])) {
          // NOTE using res.args(0) because there is just one argument here
          // NOTE we can safely cast to A because we know the test case has been generated with gen: Gen[A]
          val counterexample: A = res.args(0).origArg.asInstanceOf[A]
          println(s"\tCOUNTEREXAMPLE was: ${counterexample}")
          writeToFile(counterexample, counterExamplePath)
        }
        res
      }
    }
  }

  /**
   * This function evaluates for 2 generators and only return a counterexample if it finds one value for each
   * generator whom reject their specified properties
   */
  def forAll[A, B](g1: Gen[A], g2: Gen[B], filePath1: String, filePath2: String)
                  (f: (A, B) => Prop): Prop = forAll(g1, filePath1)(t => forAll(g2, filePath2)(f(t, _: B)))

  /**
   * This function evaluates for 2 generators and only return a counterexample if it finds one value for each
   * generator whom reject their specified properties
   */
  def forAll[A, B, C](g1: Gen[A], g2: Gen[B], g3: Gen[C], filePath1: String, filePath2: String, filePath3: String)(f: (A, B, C) => Prop): Prop = forAll(g1, filePath1)(t => forAll(g2, g3, filePath2, filePath3)(f(t, _: B, _: C)))

  /**
   * This function evaluates for 2 generators and only return a counterexample if it finds one value for each
   * generator whom reject their specified properties
   */
  def forAll[A, B, C, D](g1: Gen[A], g2: Gen[B], g3: Gen[C], g4: Gen[D], filePath1: String, filePath2: String, filePath3: String, filePath4: String)(f: (A, B, C, D) => Prop): Prop = forAll(g1, filePath1)(t => forAll(g2, g3, g4, filePath2, filePath3, filePath4)(f(t, _: B, _: C, _: D)))

  /**
   * Saves the result in the file
   * Could be later extended to return a boolean whether the call was successful or not
   * when the file reaches a number of examples written and implement an LRU system?
   */

  def writeToFile[A](c: A, ruta: String) {

    val out = new FileWriter(new File(ruta), true)
    try {
      out.write(c.toString())
      out.write("\n")
    } finally
      out.close()
  }
}

/**
 * Companion object
 */
object IncisiveProp {
  def apply(sc: SparkContext): IncisiveProp = new IncisiveProp(sc)
}

@RunWith(classOf[JUnitRunner])
class IncisivePropTest
    extends Specification
    with ScalaCheck
    with SharedSparkContextBeforeAfterAll {

  def is = sequential ^ s2"""
       - where ${p2(-7)}
       - where ${p2(-4)}
       - where $p3 
     """

  /**Spark context definition */
  override def defaultParallelism: Int = 3
  override def sparkMaster: String = "local[5]"
  override def sparkAppName = "IncisivePropTest" //this.getClass().getName()

  /**
   * This specific file contains 10 tweets
   */
  val filePath = "src/test/resources/twitter.avro"
  val sqlContext = new SQLContext(impSC)

  //
  val incisiveProp = IncisiveProp(impSC)

  //Testing for a forall func with 2 gens
    def p2(v: Int) = incisiveProp.forAll(Gen.oneOf(-5, -10), Gen.oneOf(2, -3), "path.avro", "path2") { (x, y) =>
    println(s"x (for v = $v): $x")
    println(s"y : $y")

    x * y must be_>(v)

  }.set(minTestsOk = 3).verbose
  
  
  def p3 = incisiveProp.forAll(Gen.oneOf("hola", "adios"), filePath) { sentence =>
    println(s"sentence: $sentence")
     sentence == "adios"
  }.set(minTestsOk = 3).verbose
}

