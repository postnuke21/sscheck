package es.ucm.fdi.tfg

import org.junit.runner.RunWith

import org.specs2.runner.JUnitRunner
import org.specs2.Specification
import org.specs2.ScalaCheck
import org.specs2.scalacheck.Parameters
import org.specs2.execute._

import org.apache.spark.sql.SQLContext

import org.scalacheck.Prop
import org.scalacheck.Gen

import com.databricks.spark.avro._

import java.io._

import es.ucm.fdi.sscheck.spark.SharedSparkContextBeforeAfterAll

@RunWith(classOf[JUnitRunner])
class FromPropGenTest extends Specification
    with ScalaCheck
    with SharedSparkContextBeforeAfterAll {

  //Spark context definition
  override def defaultParallelism: Int = 3
  override def sparkMaster: String = "local[5]"
  override def sparkAppName = "FromRDDGen" //this.getClass().getName()

  def is = s2"""FromPropGenTest where
    -  prop1 $test
    """

  //ERROR : Trying to get the file from the classpath IS NOT working , using absolute paths instead
  val filePath = "src/test/resources/episodes.avro"

  //We need zipWithUniqueId() because we might encounter rows with the same value , regarding the estructure of the data
  //by using transformations upon the data , that's why we neeed a unique id for each single entry. 
  val sqlContext = new SQLContext(impSC)
  var rdd = sqlContext.read.avro(filePath).rdd.zipWithUniqueId()
  println(rdd.count())

  //Define all the control variables needed to execute the test 
  val defaultToLast = false
  val bufferSize = 5
  val withRepl = false
  

  def test = {
    //Defines a generator. In this particulary case we reuse te FromRDDReplacement class to
    // get a Gen[A] which returns a row!
    val gen  = FromRDDReplacementGen(rdd, bufferSize, defaultToLast, withRepl, impSC)
    property(gen, "prop1.txt") {
      //Define the conditions where the property will pass or not
     (r) => r mustEqual null
    }
  }

  //Defines a property given a generator and a filepath and returns a Result from
  //applying the property to the generator
  def property[A, R: AsResult](g: Gen[A], filePath: String)(prop: A => R): Result =
    saveResult(filePath)(Prop.forAll(g)(prop))

  //Given a path  and a result checks if the result (being result anything parsable to Result)
  //is not a success and calls the writeToFile function to save it
  def saveResult[R: AsResult](filePath: String)(r: R): Result = {
    val result = AsResult(r)
    if (!result.isSuccess)
      writeToFile(result, filePath)
    result
  }

  //Saves the result in the file
  //Could be later expanded to return a boolean whether the call was successful or not
  //when the file reaches a number of examples written and implement an LRU system?
  def writeToFile(result: Result, filePath: String){
    val writer = new PrintWriter(new File(filePath))
      println("Wrote a result")
      writer.write(result.toString())
      writer.close()
  }
  
}