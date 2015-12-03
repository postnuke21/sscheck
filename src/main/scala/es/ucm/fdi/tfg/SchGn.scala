package es.ucm.fdi.tfg

import java.io._
import com.gensler.scalavro.types.AvroType
import com.gensler.scalavro.io.AvroTypeIO
import scala.util.{ Try, Success, Failure }

object SchGn extends App {

  def test() {
    
    val tweet1 = twitter_schema("human1", "hola mundo", 1366154481)
    val tweet2 = twitter_schema("human2", "adios mundo", 1366154482)

    val tweet1Type = AvroType[twitter_schema]
    
    val filestream = new File("../Binaries/tweet1.avro")
    val outStream = new FileOutputStream(filestream)

    tweet1Type.io.write(tweet1, outStream)
    
    val inStream: java.io.InputStream = new FileInputStream(filestream)

    tweet1Type.io.read(inStream) match {
      case Success(readResult) => println("Successfully deserialized: " + readResult)
      case Failure(cause)      => println("Failure")
    }

  }
  
  test()

}