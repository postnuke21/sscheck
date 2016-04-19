package es.ucm.fdi.tfg

import scala.language.implicitConversions

import org.scalacheck.{ Prop, Gen }

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

import es.ucm.fdi.sscheck.spark.SharedSparkContextBeforeAfterAll

import com.databricks.spark.avro._

import java.util.concurrent.{ ConcurrentLinkedQueue => JConcurrentLinkedQueue }
import java.util.{ Queue => JQueue }
import java.util.Random

class FromRDDGen[A](filePath: String, bufferSize: Int, defaultToLast: Boolean)
    extends SharedSparkContextBeforeAfterAll {

  //Spark context definition
  override def defaultParallelism: Int = 3
  override def sparkMaster: String = "local[5]"
  override def sparkAppName = "FromRDDGenTest" //this.getClass().getName()

  //USAMOS SPARK SQL! load the data into a  DataFrame , once we have this we convert it to RDD
  val sqlContext = new SQLContext(impSC)
  val rdd = sqlContext.read.avro(filePath).rdd

  //Persist rdd in memory
  rdd.cache()
  println("Generating list with " + bufferSize + " size")
  Console.flush()

  //Generate a list with random samples of size bufferSize with replacement
  val samplingSeed = new Random(System.currentTimeMillis()).nextLong()
  val sampled = rdd.takeSample(true, bufferSize, samplingSeed)
  val buffer: JQueue[A] = new JConcurrentLinkedQueue[A]
  
  for (i <- 0 to sampled.length - 1)
    buffer.add(sampled(i).asInstanceOf[A])

  def gen(): Gen[A] = {
    
    val nextCase = Option(buffer.peek())
    nextCase match {

      case None    => Gen.fail
      //If buffer size has only 1 remmainign element and 
      //defaultToLast is set to TRUE  we remove it and return the Gen
      //otherwise we keep returig the same last value
      case Some(x) => { if(buffer.size() >1)
                          buffer.remove()
                          else if(buffer.size==1 && defaultToLast)
                            buffer.remove()         
                        Gen.const(x) 
                      }
    }
  }

}

//We define an object companion which let us  create a FromRDDGen object calling the apply 
//function and using the parameters described. For each test only the fromRDDGen2Gen will be called
object FromRDDGen {
  def apply[A](path: String, bufferSize: Int, defaultToLast: Boolean): FromRDDGen[A] = new FromRDDGen[A](path, bufferSize, defaultToLast)
  implicit def fromRDDGen2Gen[A](frG: FromRDDGen[A]): Gen[A] = Gen.wrap(frG.gen())
}