package es.ucm.fdi.tfg

import java.io._
import com.gensler.scalavro.types.AvroType
import com.gensler.scalavro.io.AvroTypeIO
import scala.util.{ Try, Success, Failure }

import org.apache.avro._
import org.apache.avro.generic._
import org.apache.avro.specific._
import org.apache.avro.Schema
import org.apache.avro.file._

object SchGn extends App {
  
  

  def test() {
    
    val tweet1 = twitter_schema("human1", "hola mundo", 1366154481)
    val tweet2 = twitter_schema("human2", "adios mundo", 1366154482)
    val tweet3 = twitter_schema("human3", "probando tweets", 1366154483)
    val tweet4 = twitter_schema("human4", "hay un limite de 140 caracteres", 1366154484)
    val tweet5 = twitter_schema("human5", "favorito", 1366154485)
    val tweet6 = twitter_schema("human6", "writing random tweets", 1366154486)
    val tweet7 = twitter_schema("human7", "bbye every1", 1366154487)


    val tweetType = AvroType[twitter_schema]
    val schemaEx = tweetType.schema().toString()
    
    
    val fileStream = new File("../Binaries/tweetList.avro")
    
    val outStream = new FileOutputStream(fileStream)
      
   /*  val userDatumWriter = new SpecificDatumWriter(tweet1.getClass)
    val dataFileWriter = new DataFileWriter(userDatumWriter)
    dataFileWriter.create( schemaEx, outStream)
    dataFileWriter.append(tweet1)
    dataFileWriter.close*/
    

    
      
    val outStream1: java.io.OutputStream = new FileOutputStream("")
    
  
    
    tweetType.io.write(tweet1, outStream)
    tweetType.io.write(tweet2, outStream)
    tweetType.io.write(tweet3, outStream)
    tweetType.io.write(tweet4, outStream)
    tweetType.io.write(tweet5, outStream)
    tweetType.io.write(tweet6, outStream)
    tweetType.io.write(tweet7, outStream)

    println("Done")
    /*
    val inStream: java.io.InputStream = new FileInputStream(filestream)

    tweetType.io.read(inStream) match {
      case Success(readResult) => println("Successfully deserialized: " + readResult)
      case Failure(cause)      => println("Failure")
    }*/

  }
  
  test()

}