package test

data class Result(var successes: Int = 0, var fails: Int = 0) {
  operator fun plusAssign(other: Result) {
    successes += other.successes
    fails += other.fails
  }

  fun add(bool: Boolean) {
    if (bool)
      successes++
    else
      fails++
  }
}
