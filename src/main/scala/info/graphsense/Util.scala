package info.graphsense

object Util {

  import java.util.concurrent.TimeUnit

  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block
    val t1 = System.currentTimeMillis()

    val elapsed_time = t1 - t0
    val hour = TimeUnit.MILLISECONDS.toHours(elapsed_time)
    val min = TimeUnit.MILLISECONDS.toMinutes(elapsed_time) % 60
    val sec = TimeUnit.MILLISECONDS.toSeconds(elapsed_time) % 60
    println("Time: %02d:%02d:%02d".format(hour, min, sec))
    result
  }
}
