package es.ucm.fdi.tfg


import scala.language.implicitConversions

import org.scalacheck.{ Prop, Gen }

import org.apache.spark.rdd.RDD

import java.util.concurrent.{ ConcurrentLinkedQueue => JConcurrentLinkedQueue }
import java.util.{ Queue => JQueue }
import java.util.Random

class FromRDDGen[A](rdd: RDD[A], bufferSize: Int, defaultToLast: Boolean) {

  //Persist rdd in memory
  rdd.cache()
  println("Generating list with " + bufferSize + " size")
  //Console.flush()

  //Generates a list with random samples of size bufferSize with replacement
  val samplingSeed = new Random(System.currentTimeMillis()).nextLong()
  val sampled = rdd.takeSample(true, bufferSize, samplingSeed)
  val buffer: JQueue[A] = new JConcurrentLinkedQueue[A]

  //Converting array to concurrent queue
  for (i <- 0 to sampled.length - 1)
    buffer.add(sampled(i).asInstanceOf[A])

    
  def gen(): Gen[A] = {
    //Checks next value in the queue without extracting it
    val nextCase = Option(buffer.peek())
    
    nextCase match {
      case None => Gen.fail
      //If buffer size has only 1 remmaining element and 
      //defaultToLast is set to TRUE  we remove it and return the Gen
      //otherwise we keep returning the same last value without extracting it
      case Some(x) => {
        if (buffer.size() > 1)
          buffer.remove()
        else if (buffer.size == 1 && !defaultToLast)
          buffer.remove()
        Gen.const(x)
      }
    }
  }

}

// We define an object companion which let us  create a FromRDDGen object calling the apply function 
// and using the parameters described. For each test only the fromRDDGen2Gen will be called
object FromRDDGen {
  def apply[A](rdd: RDD[A], bufferSize: Int, defaultToLast: Boolean): FromRDDGen[A] = new FromRDDGen[A](rdd, bufferSize, defaultToLast)
  implicit def fromRDDGen2Gen[A](frG: FromRDDGen[A]): Gen[A] = Gen.wrap(frG.gen())
}