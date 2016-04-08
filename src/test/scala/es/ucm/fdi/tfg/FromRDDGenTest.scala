package es.ucm.fdi.tfg

import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner 

import org.specs2.Specification
import org.specs2.ScalaCheck
import org.specs2.scalacheck.Parameters

import org.scalacheck.Arbitrary.arbitrary
import org.scalacheck.{ Prop, Gen }

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import es.ucm.fdi.sscheck.spark.SharedSparkContextBeforeAfterAll

import com.databricks.spark.avro._


@RunWith(classOf[JUnitRunner])
class FromRDDGenTest  extends Specification
  with SharedSparkContextBeforeAfterAll 
  with ScalaCheck{

  
  override def defaultParallelism: Int = 3
  override def sparkMaster : String = "local[5]"
  override def sparkAppName = this.getClass().getName()
  
  
 //Debemos ahora encontrar la forma de poder cargar el fichero avro a un rdd , conservando la estructura 
  //de los datos guardados
  //SPARK SQL!!!
 val filePath = "../Binaries/tweetList.avro"
 val sqlContext = new SQLContext(sc)
 val df = sqlContext.read.avro(filePath)   
  
  def is = s2"""FromRDDGenTest where
    -  prop1 $prop1
    """
  
  
  def fromRDDGen[A](rdd: RDD[A], bufferSize : Int): Gen[A] = {

    rdd.cache()
    println("counting rdd")
    Console.flush()
    
    
    //el for yields una lista con el primera valor dl vector xs
    for {
      seed <- arbitrary[Long]
      xs = {
        println("sampling rdd")
        Console.flush()
        rdd.sample(withReplacement = true, fraction = 1 / bufferSize.toFloat, seed).take(1)
      }
     if xs.length > 0
    } yield xs(0)
    
   
  }
  
  
   def prop1 = Prop.forAll(fromRDDGen(df.rdd,3)){ 
     (x) => x.toString()
      println(s"x = $x")
    x.length must beGreaterThan(0)
  }.set(minTestsOk = 2).verbose
  
  
}
  
