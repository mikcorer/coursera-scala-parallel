package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {

    def newBalance(b: (Int, Int), ch: Char): (Int, Int) = ch match {
      case '(' => (b._1 + 1, b._2)
      case ')' => (b._1 - 1, b._2.min(b._1 - 1))
      case _ => b
    }

    chars.foldLeft((0, 0))(newBalance) == (0, 0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int): (Int, Int) = {
      var i = idx
      var balance = 0
      var min = 0
      while (i < until) {
        if (chars(i) == '(') {
          balance += 1
        } else if (chars(i) == ')') {
          balance -= 1
          min = min.min(balance)
        }
        i += 1
      }
      (balance, min)
    }

    def reduce(from: Int, until: Int) : (Int, Int) = {
      if (until - from <= threshold) {
        traverse(from, until)
      }
      else {
        val mid = from + (until - from) / 2
        val (t1, t2) = parallel(
          reduce(from, mid),
          reduce(mid, until)
        )
        (t1._1 + t2._1, t1._2.min(t1._1 + t2._2))
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}
