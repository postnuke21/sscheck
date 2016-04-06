package es.ucm.fdi.tfgTest


import org.junit.runner.RunWith
import org.specs2.runner.JUnitRunner 

import org.specs2.Specification
import org.specs2.ScalaCheck
import org.specs2.scalacheck.Parameters

import es.ucm.fdi.tfg.twitter_schema
import org.scalacheck.Prop

import es.ucm.fdi.tfg.FromFileGen
import java.io._


@RunWith(classOf[JUnitRunner])
class FromFileGenTest
  extends Specification
  with ScalaCheck { 
  
  def is = s2"""FromFileGenTest where
    -  prop1 $prop1
    """
  
  val filePath = "../Binaries/tweetList.avro"
  val defaultToLast = false
  
  def prop1 =  Prop.forAll(FromFileGen(filePath, defaultToLast)){ (reg : twitter_schema) =>
    println(s"reg = $reg")
    
    defaultToLast must_== false
  }.set(minTestsOk = 5).verbose
}