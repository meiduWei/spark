package sparkday01

/**
  * * @Author: Wmd
  * * @Date: 2019/7/6 21:13
  */
object AbstrctConctrl {
  def main(args: Array[String]): Unit = {

    def myac(f: ()=> Unit){
      new Thread(){
        f()
      }.start()
    }

    myac{
      () =>
      println("开始干活了,需要2秒")
      Thread.sleep(2000)
      println("活干完了")
    }


  }
}
