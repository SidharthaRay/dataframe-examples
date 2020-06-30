package com.dsm.optional
import scala.collection.mutable
object TestGL {
  def main(args: Array[String]): Unit = {

//   val arr = Array(3, 2, 1, 2, 7)
    val arr = Array(3, 1, 2, 2)
   println(foo(arr))

  }

  def foo(arr: Array[Int]): Int = {
    var set = arr.toSet
    var list = mutable.Buffer[Int]()
    for(num <- arr) {
      var newNum = 0
      if(set.contains(num) & !list.contains(num)) {
        list += num
      } else {
        if(list.max < set.max) {
          newNum = list.max + 1
        } else {
          newNum = set.max + 1
        }
        set +=  newNum
        list += newNum
      }
    }
    list.sum
  }
}
