package es.ucm.fdi.tfg


import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner 

import org.specs2.Specification
import org.specs2.ScalaCheck
import org.specs2.scalacheck.Parameters

import org.scalacheck.Prop

import java.io._


@RunWith(classOf[JUnitRunner])
class FromRDDGenTest extends Specification
  with ScalaCheck { 
  
  def is = s2"""FromRDDGenTest where
    -  prop1 $prop1
    """
  
 //ERROR : Trying to get the file from the classpath IS NOT working , using absolute paths instead
 //val filePath = getClass.getResource("/episodes.avro").toString
 val filePath = "src/test/resources/twitter.avro"
 
 //ERROR: Al intentar cargar el archivo nos topamos con una excepcion fileNotFound
 //val filePath = getClass.getResource("/tweetList.avro").toString()
  val defaultToLast = false
  
  def prop1 =  Prop.forAll(FromRDDGen(filePath, 5, defaultToLast)){ 
    
    (reg : String) => println(s"reg = $reg")
    defaultToLast must_== false
    //Property reduced to one sampling? 
    
  }.set(minTestsOk = 4).verbose
}