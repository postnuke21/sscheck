package es.ucm.fdi.tfg


import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner 

import org.specs2.Specification
import org.specs2.ScalaCheck
import org.specs2.scalacheck.Parameters

import org.scalacheck.Prop

import java.io._


@RunWith(classOf[JUnitRunner])
class FromFileGenTest
  extends Specification
  with ScalaCheck { 
  
  def is = s2"""FromFileGenTest where
    -  prop1 $prop1
    """
  
  // The file being read here doesn't cointain the schema of the data, the file 
  // was generated with Scalavro which let us serialize data but its still unable to attach the schema
 val filePath = "src/test/resources/tweetList.avro"
 
 //ERROR: Al intentar cargar el archivo nos topamos con una excepcion fileNotFound
 //val filePath = getClass.getResource("/tweetList.avro").toString()
 
  val defaultToLast = true
  
  def prop1 =  Prop.forAll(FromFileGen(filePath, defaultToLast)){ 
    
    (reg) => println(s"reg = $reg")
    defaultToLast must_== true  
    
  }.set(minTestsOk = 10).verbose
}