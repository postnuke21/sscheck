package es.ucm.fdi.tfg

import scala.language.implicitConversions

import org.scalacheck.Gen
import java.io._

import scala.collection.JavaConverters._
import java.util.concurrent.{ConcurrentLinkedQueue => JConcurrentLinkedQueue}
import java.util.{Queue => JQueue}
import scala.util.{ Try, Success, Failure }


import com.gensler.scalavro.types.AvroType



// lista de tweets que recib eel generador y que va iterando y leyendo y generando 
class FromFileGen[A]  (path : String, defaultToLast : Boolean) {
    //val caseQueue : JQueue[A] = new JConcurrentLinkedQueue[A](xs.asJavaCollection)
    //val filestream = new File(path)
    val tweetList = AvroType[twitter_schema]
    val fileStream = new File(path)
    val inStream: java.io.InputStream = new FileInputStream(fileStream)
    var defaultVal : Boolean
    
    def gen(): Gen[twitter_schema] = {
     /* val nextCase = Option(twitter_schema)
      println(s"Aun hay registros por leer")
      nextCase match {*/
      
   
      
      tweetList.io.read(inStream) match {
        // NOTE this implies that if the resulting generator is called more 
        // than xs.length times then the property will fail. TODO configure 
        // to allow a boolean param to return the last value of xs otherwise 
        // when the queue is consumed
        
      case Success(readResult) =>  {defaultVal = false; Gen.const(readResult)}
      case Failure(cause)      =>  {defaultVal = true;Gen.fail}
      }
      
        /*case None    => if (defaultToLast)  Gen.fail
        else  
          Gen.const(tweet)
         
        } 
        // //If defaultTolast es true = devuelvo el ultim oque lei y si es false devuelve fail
       //devuelvo el ultimo leido segun defaulttolast
        case Some(tweet) => Gen.const(tweet) //actualizar el valor del bool y  */
        
        
    }
  }

 object FromFileGen {
    def apply[A](path : String, defaultToLast : Boolean): FromFileGen[A] = new FromFileGen[A](path , defaultToLast)
    implicit def fromListGen2Gen[A](flG: FromFileGen[A]): Gen[twitter_schema] = Gen.wrap(flG.gen())
  }