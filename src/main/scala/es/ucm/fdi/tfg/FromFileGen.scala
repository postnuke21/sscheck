package es.ucm.fdi.tfg

import scala.language.implicitConversions

import org.scalacheck.Gen
import java.io._

import scala.collection.JavaConverters._
import java.util.concurrent.{ConcurrentLinkedQueue => JConcurrentLinkedQueue}
import java.util.{Queue => JQueue}
import scala.util.{ Try, Success, Failure }


import com.gensler.scalavro.types.AvroType




class FromFileGen[A]  (path : String, defaultToLast : Boolean) {
    
    val tweetList = AvroType[twitter_schema]
    val fileStream = new File(path)
    val inStream: java.io.InputStream = new FileInputStream(fileStream)
    var defaultVal = false;
  
    
    def gen(): Gen[twitter_schema] = {
       
      
      tweetList.io.read(inStream) match {
        
        case Success(readResult) =>  {defaultVal = true; Gen.const(readResult)}
        case Failure(cause)      =>  {defaultVal = false;Gen.fail}
      }
            
      
        // //If defaultTolast es true = devuelvo el ultim oque lei y si es false devuelve fail
       //devuelvo el ultimo leido segun defaulttolast
         //actualizar el valor del bool y  */
        
        
    }
  }

 object FromFileGen {
    def apply[A](path : String, defaultToLast : Boolean): FromFileGen[A] = new FromFileGen[A](path , defaultToLast)
    implicit def fromListGen2Gen[A](flG: FromFileGen[A]): Gen[twitter_schema] = Gen.wrap(flG.gen())
  }