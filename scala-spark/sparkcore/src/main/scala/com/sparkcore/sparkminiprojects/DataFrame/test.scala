package com.sparkcore.sparkminiprojects.DataFrame

object test {
  def main(args: Array[String]): Unit = {
    def twoSum(nums: Array[Int], target: Int) = {
      val numMap = nums.zipWithIndex.toMap
      /*
      nums.indices
        .find(i => numMap.contains(target - nums(i)))
        .map(i => Array(i, numMap(target - nums(i))))
        .getOrElse(Array.empty[Int])
     */
      val num1 = nums.indices
        .filter(i => numMap.contains(target - nums(i)))
        .map(i => )

      print(num1)
    }

    val res = twoSum(Array(3,2,4), 6)

  }
}
