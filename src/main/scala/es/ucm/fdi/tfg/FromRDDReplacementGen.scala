  package es.ucm.fdi.tfg

import scala.language.implicitConversions

import org.scalacheck.{ Prop, Gen }

import org.apache.spark._
import org.apache.spark.rdd.RDD

import java.util.concurrent.{ ConcurrentLinkedQueue => JConcurrentLinkedQueue }
import java.util.{ Queue => JQueue }
import java.util.Random

//import  scala.language.postfixOps

class FromRDDReplacementGen[A, B](var rdd: RDD[(A, B)], bufferSize: Int, defaultToLast: Boolean, withRepl: Boolean, impSC: SparkContext)
    extends Serializable {

  rdd.cache()
  Console.flush()
  println("Generating list with size " + bufferSize + " and replacement set to " + withRepl)
 
  val buffer: JQueue[(A, B)] = new JConcurrentLinkedQueue[(A, B)]
   

   def generateSampling():Unit = { //PUEDE DEVOLVER BOOLEAN SI EL RDD SE QUEDA SIN ELEMS  
    /**
     * Generate a list with random samples of size bufferSize with or w/o replacement
     * buffer stays as val since we will only extract elements without reassigning it to a 
     * new collection.
     * Sampled array is then copied to the JQueue
     */  
    var sampled = rdd.takeSample(withRepl, bufferSize, new Random(System.currentTimeMillis()).nextLong())
    for (i <- 0 to sampled.length - 1)
      buffer.add(sampled(i))

    /**Collects the ids from the RDD using broadcast and filters the rdd 
     	*with these values, leaving the rdd only with those registers not sampled
      *It was necessary to extend the Serializable class otherwise a Spark 
      *exception was being encountered
     */
    if (!withRepl) {
      var ids = impSC.broadcast(sampled.map(_._2))
      rdd=rdd.filter(v => !ids.value.contains(v._2))
    }
    
  }
  
  /**
   * Sampling for the first time
   */
  generateSampling()
 
  def gen(): Gen[A] = {
    
    /**
     * We will sample only if the buffer is depleted and the number of registers in the rdd
     * is large enough.
     * The only way the buffer can be emptied out is when defaultToLast is set to true
     * otherwise the generator will keep returning the last element regardless of the number of test cases
     */
    if(buffer.isEmpty() && rdd.count() >= bufferSize){
      println("Buffer depleted , generating a new batch of samples")
      generateSampling()
    }
    else if (buffer.isEmpty() &&  rdd.count() < bufferSize )
      println("Couldn't generate more samples")
    
    val nextCase = Option(buffer.peek())
    nextCase match {

      case None => Gen.fail
      /**If buffer size has only 1 remmainign element and 
        *defaultToLast is set to TRUE  we remove it and return the Gen
        *otherwise we keep returning the same last value      
       */
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

/**We define an object companion which let us  create a FromRDDGen object calling the apply 
  *function and using the parameters described. For each test only the fromRDDGen2Gen will be called
  */
object FromRDDReplacementGen {
  def apply[A, B](rdd: RDD[(A, B)], bufferSize: Int, defaultToLast: Boolean, withRepl: Boolean, impSC: SparkContext): FromRDDReplacementGen[A, B] = new FromRDDReplacementGen[A, B](rdd, bufferSize, defaultToLast, withRepl, impSC)
  implicit def FromRDDGenNoReplacement[A, B](frG: FromRDDReplacementGen[A, B]): Gen[A] = Gen.wrap(frG.gen())
}