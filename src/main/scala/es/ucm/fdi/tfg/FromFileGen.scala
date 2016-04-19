package es.ucm.fdi.tfg

import scala.language.implicitConversions

import org.scalacheck.Gen
import java.io._

import scala.collection.JavaConverters._
import java.util.concurrent.{ConcurrentLinkedQueue => JConcurrentLinkedQueue}
import java.util.{Queue => JQueue}
import scala.util.{ Try, Success, Failure }


import com.gensler.scalavro.types.AvroType


class FromFileGen[A]  (path : String, val defaultToLast : Boolean) {
    
  //Variable necessary to parse the register read, it determinates the schema the data will have
  //we also define a lastRegRead variable to be able to generate the last value read
  //when we reach EOF
    val tweetList = AvroType[twitter_schema]
    val fileStream = new File(path)
    val inStream: java.io.InputStream = new FileInputStream(fileStream)
    var lastRegRead = new twitter_schema(null,null,0)
        
    def gen(): Gen[twitter_schema] = {
       
      //Reads an avro file and returns a generator containing that data
      //for each register, defaultToLast = FALSE determinates if we return
      //the last register read or a Gen.fail
      tweetList.io.read(inStream) match {
        case Success(readResult) =>  {  lastRegRead = readResult;
                                        Gen.const(readResult)
                                      }
        case Failure(cause)      =>  {  if (!defaultToLast)
                                           Gen.const(lastRegRead)
                                        else 
                                          Gen.fail
                                      }
      }        
    }
  }


 object FromFileGen {
    def apply[A](path : String, defaultToLast : Boolean): FromFileGen[A] = new FromFileGen[A](path , defaultToLast)
    implicit def fromAvroGen2Gen[A](flG: FromFileGen[A]): Gen[twitter_schema] = Gen.wrap(flG.gen())
  }