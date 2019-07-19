package scaladay05

/**
  * * @Author: Wmd
  * * @Date: 2019/7/11 11:14
  */
object sortWith {
  def main(args: Array[String]): Unit = {
    val users = List(new User("zs",10),new User("ls",20),new User("a",20))
   /* val list2 = users.sortWith((user1,user2) => {
       user1.age < user2.age
      if (user1.age  == user2.age) {
        user1.name > user2.name
      }else{
        user1.age < user2.age
      }
    })*/
    //println(list2)

    val users2 = users.sortWith((user1,user2) => user1 < user2)
    println(users2)
  }

 /* class User(val name:String,val age:Int){

    override def toString = s"User($name, $age)"
  }*/
}

class User (val name:String,val age:Int) extends Ordered[User]{

  override def toString = s"User($name, $age)"

  override def compare(that: User): Int = {
    this.age > that.age
    1
  }
}

 object  sortEd{
   def main(args: Array[String]): Unit = {


val list = List(2,4,2,1,30)



   }



 }
object  soertBy{
  def main(args: Array[String]): Unit = {
    val list = List(10,20,5,7,40,80)
   //val list2 =  list.sortBy(x => x % 2)(Ordering.Int.reverse)
    //val list3 = List("aa","helo","shanfang","weimeidu","long")
    //list3.sortBy(x => (x.length,x))(Ordering.Tuple2(Ordering.Int.reverse,Ordering.String))
    val users = List(new User("zs",10),new User("ls",20),new User("a",20))
val list4 = users.sortBy(user => (user.age,user.name))(Ordering.Tuple2(Ordering.Int,Ordering.String.reverse))
    println(list4)
  }
}