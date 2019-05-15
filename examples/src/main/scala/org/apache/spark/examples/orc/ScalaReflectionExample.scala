package org.apache.spark.examples.orc
import org.apache.spark.examples.sql.hive.JavaBase
import org.apache.spark.sql.hive.util.ScalaReflectionUtil
import org.apache.spark.sql.hive.util.ScalaReflectionUtil._
/**
  * create with org.apache.spark.examples.orc
  * USER: husterfox
  */
case class ExampleA(a: String, b: Array[String], c: Array[String]) {
  println("invoke Test constructor method")
  var subTest: String = "hello"

  def this(b: Array[String], c: Array[String]) {
    this("1", b, c)
  }

  def this() = {
    this(null, null, null)
  }

  def doA(): Unit = {
    println("I am doA")
  }

  def doA(a: Int): Unit = {
    println("I am doA(a: Int)")
  }

  def doA(a: String, b: Array[String]): Unit = {
    println("I am doA(a:String, b:Array[String])")
  }

  def doB(a: Array[String]): Unit = {
    println("I am doB(a:Array[String])")
  }

}

class ExampleB(a: String, b: Array[String], c: Array[String], d: String) extends ExampleA(a, b, c) {
  override def doB(a: Array[String]): Unit = {
    println("I am TestB.doB(a: Array[String])")
  }

  def doC() = {
    println("I am TestB.doC")
  }
}

object ScalaReflectionExample {
  //decl 只会寻找本类的成员 member会寻找本类和继承类的成员
  def main(args: Array[String]): Unit = {


    /**
      * 获取ExampleA类中的doB方法， 指定doB方法的参数类型，注意用Seq结构存储类型，不要用Array，否则会出现
      * TypeTag的传递问题
      */
    val testInstance = new ExampleA("helloWord", Array[String]("hello"), Array[String]("world"))
    val doBMethod = ScalaReflectionUtil.reflectMethod("doB", false,
      Seq(getType[Array[String]]): _*)(testInstance)
    doBMethod(Array[String]())


    /**
      * ExampleA类中获取实例方法, 方法参数为空 doA()
      *
      */

    val doAMethod = ScalaReflectionUtil.reflectMethod("doA", false,
      Seq[ru.Type](): _*)(testInstance)
    doAMethod("a", Array[String]("b"))


    /**
      * ExampleA类中获取构造方法，指定参数类型
      */
    val testConstructor = ScalaReflectionUtil.reflectConstructorMethod[ExampleA](
      Seq(getType[String], getType[Array[String]], getType[Array[String]]): _*)
    testConstructor("1", Array("2"), Array("3"))

    /**
      * ExampleA类中获取构造方法，构造方法参数为空
      */
    val emptyTestConstructor = ScalaReflectionUtil.reflectConstructorMethod[ExampleA](
      Seq[ru.Type](): _*)
    emptyTestConstructor()

    /**
      * 获取Java类中的静态方法
      * 注意Java类中的静态方法由于无法用Scala反射获取，因此只能返回Java反射包中的Method
      * 参数类型也由Type变为Class, 需要用classOf 获取
      * 调用方式<returnMethod>.invokd(null,args*)
      */

    val staticBaseMethod = ScalaReflectionUtil.reflectJavaStaticMethod[JavaBase]("staticBase",
      false, Seq(classOf[String]): _*)
    staticBaseMethod.invoke(null, "hello, world")

    /**
      * 获取非静态字段
      */
    val javaBaseInstance = new JavaBase()
    val field = ScalaReflectionUtil.reflectFiled("b", false)(javaBaseInstance)
    println(s"get JavaBase field b: ${field.get}")
    println(s"set JavaBase field b: ${field.get} -> 3")
    field.set(3)
    println(s"after set JavaBase field b: ${field.get}")

    /**
      * 获取Java静态字段
      * 是用Java反射获取，获取的类型为Java反射类中的Field
      */
    val fieldA = ScalaReflectionUtil.reflectJavaStaticField[JavaBase]("a", false)
    println(s"get JavaBase static field a: ${fieldA.get(null)}")
    println(s"set JavaBase static field a ${fieldA.get(null)} -> 100")
    fieldA.set(null, 100)
    println(s"after set JavaBase static field a: ${fieldA.get(null)}")


    /**
      * allScope 设置为true，从本类以及所有父类中寻找非静态方法
      */
    val testBInstance1 = new ExampleB("hello", Array("world"), Array("world"), "world")
    val doAMethod0 = ScalaReflectionUtil.reflectMethod("doA", true,
      Seq(getType[Int]): _*)(testBInstance1)
    doAMethod0(1)

    /**
      * allScope 设置为true 寻找本类以及所有父类中的字段
      */
    val testBInstance2 = new ExampleB("hello", Array("world"), Array("world"), "world")
    val subTestField = ScalaReflectionUtil.reflectFiled("subTest", true)(testBInstance2)
    println(s"get TestB field subTest: ${subTestField.get}")
    println(s"set TestB field subTest ${subTestField.get} -> world")
    subTestField.set("world")
    println(s"after set TestB field subTest: ${subTestField.get}")


    /**
      * 异常测试
      * 当找不到非静态方法时
      */
    //方法名称错误
    val testBInstance3 = new ExampleB("hello", Array("world"), Array("world"), "world")
    try {
      ScalaReflectionUtil.reflectMethod("doD", false, Seq[ru.Type](): _*)(testBInstance3)
    } catch {
      case e: NoSuchMethodException =>
        println(e.getMessage)
      case o: Exception =>
        throw o
    }

    //参数类型错误
    try {
      ScalaReflectionUtil.reflectMethod("doC", false, Seq(getType[Int]): _*)(testBInstance3)
    } catch {
      case e: NoSuchMethodException =>
        println(e.getMessage)
      case o: Exception =>
        throw o
    }

    /**
      * 异常测试
      * 当找不到构造方法时
      */
    //参数类型错误
    try {
      ScalaReflectionUtil.reflectConstructorMethod[ExampleB](Seq(getType[Int]): _*)
    } catch {
      case e: NoSuchMethodException =>
        println(e.getMessage)
      case o: Exception =>
        throw o
    }

    /**
      * 异常测试
      * 找不到静态方法
      */
    try {
      val staticBase0Method = ScalaReflectionUtil.reflectJavaStaticMethod[JavaBase]("staticBase0",
        false, Seq(classOf[String]): _*)
      staticBase0Method.invoke(null, "hello, world")
    } catch {
      case e: NoSuchMethodException =>
        println(e.getMessage)
      case o: Exception =>
        throw o
    }

    try {
      val base0Method = ScalaReflectionUtil.reflectJavaStaticMethod[JavaBase]("base",
        false, Seq[Class[_]](): _*)
      base0Method.invoke(null, "hello, world")
    } catch {
      case e: NoSuchMethodException =>
        println(e.getMessage)
      case o: Exception =>
        throw o
    }


    /**
      * 异常测试，找不到字段
      */

    try {
      val javaBaseInstance1 = new JavaBase()
      val field1 = ScalaReflectionUtil.reflectFiled("e", false)(javaBaseInstance1)
      println(s"get JavaBase field b: ${field1.get}")
      println(s"set JavaBase field b: ${field1.get} -> 3")
      field.set(3)
      println(s"after set JavaBase field b: ${field1.get}")
    } catch {
      case e: NoSuchFieldException =>
        println(e.getMessage)
      case o: Exception =>
        throw o
    }

    try {
      val fieldB = ScalaReflectionUtil.reflectJavaStaticField[JavaBase]("b", false)
      println(s"get JavaBase static field a: ${fieldB.get(null)}")
      println(s"set JavaBase static field a ${fieldB.get(null)} -> 100")
      fieldB.set(null, 100)
      println(s"after set JavaBase static field a: ${fieldB.get(null)}")
    } catch {
      case e: NoSuchFieldException =>
        println(e.getMessage)
      case o: Exception =>
        throw o
    }
  }
}
