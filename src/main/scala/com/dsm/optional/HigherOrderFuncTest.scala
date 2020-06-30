package com.dsm.optional

object HigherOrderFuncTest {
  def main(args: Array[String]): Unit = {
    val numList = List(1, 2, 3, 4)
    println(reduce(numList, sum))
  }

  def reduce(numList: List[Int], f: List[Int] => Int): Int = {
    f(numList)
  }

  def sum(numList: List[Int]): Int = {
    var sum = 0
    for(num <- numList) {
      sum = sum + num
    }
    sum
  }
}
