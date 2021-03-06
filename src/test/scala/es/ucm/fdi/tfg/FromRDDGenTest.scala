package es.ucm.fdi.tfg


import org.junit.runner.RunWith

import org.specs2.runner.JUnitRunner
import org.specs2.Specification
import org.specs2.ScalaCheck
import org.specs2.scalacheck.Parameters

import org.apache.spark.sql.SQLContext

import org.scalacheck.Prop

import com.databricks.spark.avro._

import java.io._

import es.ucm.fdi.sscheck.spark.SharedSparkContextBeforeAfterAll

@RunWith(classOf[JUnitRunner])
class FromRDDGenTest extends Specification
    with ScalaCheck
    with SharedSparkContextBeforeAfterAll {

  //Spark context definition
  override def defaultParallelism: Int = 3
  override def sparkMaster: String = "local[5]"
  override def sparkAppName = "FromRDDGen" //this.getClass().getName()

  def is = s2"""FromRDDGenTest where
    -  prop1 $prop1
    """
    
  /**
  *Using SparkSQL to load the data into a  DataFrame , once we have it, we convert it to RDD[A]
  */
  val filePath = "src/test/resources/twitter.avro"
  val sqlContext = new SQLContext(impSC)
  val rdd = sqlContext.read.avro(filePath).rdd
  val defaultToLast = true

  def prop1 = Prop.forAll(FromRDDGen(rdd, 4, defaultToLast)) {
    (reg) =>
      println(s"reg = $reg")
      defaultToLast must_== true
    //Property reduced to one sampling? 

  }.set(minTestsOk = 6).verbose
}