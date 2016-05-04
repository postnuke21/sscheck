package es.ucm.fdi.tfg

import scala.language.implicitConversions

import org.scalacheck.{ Prop, Gen }

import org.apache.spark._
import org.apache.spark.rdd.RDD

import java.util.concurrent.{ ConcurrentLinkedQueue => JConcurrentLinkedQueue }
import java.util.{ Queue => JQueue }
import java.util.Random

//import  scala.language.postfixOps

class FromRDDReplacementGen[A, Long](var rdd: RDD[(A, Long)], bufferSize: Int, defaultToLast: Boolean, withRepl: Boolean, impSC: SparkContext) 
extends Serializable{

  //Persist rdd in memory , as in the metadata necessary to manipulate the rdd , not the data itself
  rdd.cache()
  println("Generating list with size " + bufferSize + " and replacement set to " + withRepl)
  Console.flush()

  //Generate a list with random samples of size bufferSize with replacement
  val samplingSeed = new Random(System.currentTimeMillis()).nextLong()
  val sampled = rdd.takeSample(withReplacement = withRepl, bufferSize, samplingSeed)
  val buffer: JQueue[(A, Long)] = new JConcurrentLinkedQueue[(A, Long)]

  //Sampled as array converts to queue
  for (i <- 0 to sampled.length - 1)
    buffer.add(sampled(i).asInstanceOf[(A, Long)])

  //Collects the ids from the RDD using broadcast and filters the rdd with these values 
  //leaving the rdd only with those registers not sampled
  //it was necessary to extend the Serializable class otherwise and Spark exception was being ecountered
  if (!withRepl) {
    var  ids = impSC.broadcast(sampled.map(_._2))
    rdd.filter(v => !ids.value.contains(v._2))    
  }

  def gen(): Gen[A] = {

    val nextCase = Option(buffer.peek())
    nextCase match {

      case None => Gen.fail
      //If buffer size has only 1 remmainign element and 
      //defaultToLast is set to TRUE  we remove it and return the Gen
      //otherwise we keep returig the same last value
      case Some(x) => {
        if (buffer.size() > 1)
          buffer.remove()
        else if (buffer.size == 1 && defaultToLast)
          buffer.remove()
        Gen.const(x._1)
      }
    }
  }

}

//We define an object companion which let us  create a FromRDDGen object calling the apply 
//function and using the parameters described. For each test only the fromRDDGen2Gen will be called
object FromRDDReplacementGen {
  def apply[A, Long](rdd: RDD[(A, Long)], bufferSize: Int, defaultToLast: Boolean, withRepl: Boolean, impSC: SparkContext): FromRDDReplacementGen[A, Long] = new FromRDDReplacementGen[A, Long](rdd, bufferSize, defaultToLast, withRepl, impSC)
  implicit def FromRDDGenNoReplacement[A, Long](frG: FromRDDReplacementGen[A, Long]): Gen[A] = Gen.wrap(frG.gen())
}