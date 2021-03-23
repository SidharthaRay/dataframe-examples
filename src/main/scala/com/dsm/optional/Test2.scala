package com.dsm.optional
import scala.collection.mutable
object Test2 {
  def main(args: Array[String]): Unit = {
    val Q = scala.io.StdIn.readInt()
    val lines: mutable.Buffer[String] = mutable.Buffer[String]()
    for(i <- 1 to Q){
      lines.append(scala.io.StdIn.readLine())
    }

    for(line <- lines) {
      val input = line.split("\\s+").map(_.toInt)
      val S = input(0).toInt
      val X = input(1).toInt
      val out = (X / S) % 12
      println(out)
    }


  }
}
