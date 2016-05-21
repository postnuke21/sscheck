package es.ucm.fdi.tfg

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner

import org.specs2.Specification
import org.specs2.ScalaCheck
import org.specs2.scalacheck.Parameters

import org.apache.spark.sql.SQLContext

import com.databricks.spark.avro._

import org.scalacheck.Prop

import java.io._

import es.ucm.fdi.sscheck.spark.SharedSparkContextBeforeAfterAll

@RunWith(classOf[JUnitRunner])
class FromRDDReplacementGenTest
    extends Specification
    with ScalaCheck
    with SharedSparkContextBeforeAfterAll {

  //Spark context definition
  override def defaultParallelism: Int = 3
  override def sparkMaster: String = "local[5]"
  override def sparkAppName = "FromRDDReplacementGen" //this.getClass().getName()

  /**
   * This specific file contains 10 tweets
   */
  val filePath = "src/test/resources/twitter.avro" 

  /**
  * We need zipWithUniqueId() because we might encounter rows with the same value , regarding the estructure of the data
  * by using transformations upon the data , that's why we neeed a unique id for each single entry. 
  */
  val sqlContext = new SQLContext(impSC)
  var rdd = sqlContext.read.avro(filePath).rdd.zipWithUniqueId()
  println("RDD size: " +rdd.count())
  
  def is = s2"""FromRDDReplacementGenTest where
    -  prop1 $prop1
    """
  
  //ERROR: Al intentar cargar el archivo nos topamos con una excepcion fileNotFound
  //val filePath = getClass.getResource("/tweetList.avro").toString()
  val defaultToLast = true
  val withRepl = false
  val bufferSize = 5

  def prop1 = Prop.forAll(FromRDDReplacementGen(rdd, bufferSize, defaultToLast, withRepl , impSC)) {
  
    (reg) =>
      println(s"reg = $reg")
      defaultToLast must_== true
  
  }.set(minTestsOk = 10).verbose
}