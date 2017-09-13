

object AppDemo {
  def main(args: Array[String]): Unit = {
    val result = new Test().getNum(1, 2);
    println(result)
    val str = "2017-08-29 13:59:59\tfd72e662\t1503986399\tPOST\tAndroid\t49.168.237.115"
    val p = ("2017-08-29 13:59:59","fd72e662\t1503986399\tPOST\tAndroid\t49.168.237.115","aa")
    print(p._2)
  }

}
