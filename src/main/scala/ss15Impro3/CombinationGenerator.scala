package ss15Impro3

import java.util
import java.util.Arrays

class CombinationGenerator {

  var BLANK_COMBINATION: Array[Int] = Array[Int](Integer.MIN_VALUE)
  var k: Int = 0
  var n: Int = 0
  var result: Array[Int] = null
  var stack: util.Stack[Integer] = new util.Stack[Integer]()
  var source: Array[Int] = null
  var nextCombination: Array[Int] = null

  def CombinationGeneratorScala(stack: util.Stack[Integer]  = new util.Stack[Integer]()) {

  }

  /**
   * Reset the generator generate combinations of size k, with the given source.
   *
   * @param k Size of the combinations to generate.
   * @param source Source of the combinations to be generated.
   */
  def reset(k: Int, source: Array[Int]) {

    this.k = k
    n = source.length
    this.source = source
    result = new Array[Int](k)
    stack.removeAllElements()
    stack.push(0)


    this.nextCombination = getNextCombination

  }

  /**
   * Checks if there is another combination which could be generated
   *
   * @return
   */
  def hasMoreCombinations: Boolean = {

    if (k == 0 || (BLANK_COMBINATION eq nextCombination) || !hasNextCombination) {

      return false
    }
    true
  }

  /**
   * Return the next combination.
   *
   * @return int array representing the next combination.
   */
  def next: Array[Int] = {


    var result = this.nextCombination
    this.nextCombination = getNextCombination
    Arrays.sort(nextCombination)

    result
  }

  private def hasNextCombination: Boolean = {

    stack.size > 0
  }

  private def getNextCombination: Array[Int] = {

    while (k != 0 && hasNextCombination) {

      var index = stack.size - 1
      var value = stack.pop


      while (value < n) {

        result(index) = value

        index = index + 1
        value = value + 1

        stack.push(value)

        if (index == k) {

          var combination: Array[Int] =  new Array[Int](k)

          for(  i <- 0 to (k-1) ){ // workaround what I need is for (int i = 0; i < k; i++) but for(i <- 0 to k) includes k too. That's why (k-1)

            combination(i) = source(result(i))
          }
          return combination
        }
      }
    }

    BLANK_COMBINATION
  }
}
