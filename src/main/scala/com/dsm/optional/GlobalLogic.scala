package com.dsm.optional
import scala.collection.mutable
import util.control.Breaks._

object GlobalLogic {
  def main(args: Array[String]): Unit = {
    println(passExam(Array(Array(2), Array(2), Array(1, 2), Array(2, 3)), 75))
  }

  def passExam(scores: Array[Array[Int]], r: Int): Int = {
    var details = mutable.Map[Int, (Int, Int, Double, Boolean)]()
    for(task <- 2 to scores.size - 1) {
      val isPassed = if((scores(task)(0).toDouble / scores(task)(1).toDouble) * 100.0 >= r) true else false
      if(!isPassed) {
        details +=
          (task -> (scores(task)(0), scores(task)(1), ((scores(task)(0)).toDouble / (scores(task)(1)).toDouble) * 100.0, isPassed))
      }
    }
    println(details)
    println("Failed tasks = " + details.filter(rec => !rec._2._4).size)
    var extraHrs = 0
    breakable {
      while (true) {
        //Thread.sleep(5000)
        details = details.map(rec => (rec._1 -> (rec._2._1 + 1, rec._2._2 + 1, ((rec._2._1 + 1).toDouble / (rec._2._2 + 1).toDouble) * 100, if (((rec._2._1 + 1).toDouble / (rec._2._2 + 1).toDouble) * 100.0 >= r) true else false)))
        extraHrs += 1
        println(details)
        println("Failed tasks = " + details.filter(rec => !rec._2._4).size)
        details = details.filter(rec => !rec._2._4)
        if (details.filter(rec => !rec._2._4).size == 0)
          break
      }
    }
    extraHrs
  }
}
