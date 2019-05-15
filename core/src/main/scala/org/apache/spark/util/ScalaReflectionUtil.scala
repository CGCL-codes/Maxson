package org.apache.spark.util

/**
  * create with org.apache.spark.util
  * USER: husterfox
  */
import java.lang.reflect.{Field, Method, Modifier}

import scala.reflect.ClassTag

/**
  * create with org.apache.spark.sql.hive.util
  * USER: husterfox
  */
object ScalaReflectionUtil {

  val ru: scala.reflect.runtime.universe.type = scala.reflect.runtime.universe

  def m: ru.Mirror = {
    ru.runtimeMirror(Thread.currentThread().getContextClassLoader)
  }

  /**
    * 反射非静态(非static)方法
    * @param methodName: 方法名字
    * @param allScope： 是否从父类中寻找，true: 先从本类找，本类找不到再从父类找 false: 只寻找当前类
    * @param types: 方法的参数类型, 可以使用本类中提供的 ru.typeOf[T] 或者ru.typeOf(x) 获得
    * @param x: 实例对象
    * @tparam T: TypeTag[T] 用于记录实例类型
    * @return 返回Scala MethodMirror,可以直接使用 `()` 调用
    */
  def reflectMethod[T: ru.TypeTag : ClassTag](methodName: String, allScope: Boolean, types: ru.Type*)(x: T): ru.MethodMirror = {
    val instanceMirror = m.reflect(x)
    val methodSymbols = if (allScope) {
      val members = getTypeTag(x).tpe.member(ru.TermName(methodName))
      if (members.equals(ru.NoSymbol)) {
        throw new NoSuchMethodException(noSuchMethodException(methodName, allScope, types: _*)(x))
      }
      members
        .asTerm
        .alternatives
        .map(_.asMethod)
    } else {
      val decls = getTypeTag(x).tpe.decl(ru.TermName(methodName))
      if (decls.equals(ru.NoSymbol)) {
        throw new NoSuchMethodException(noSuchMethodException(methodName, allScope, types: _*)(x))
      }
      decls
        .asTerm
        .alternatives
        .map(_.asMethod)
    }
    methodSymbols.foreach(item => assert(item.paramLists.size < 2, "we don't support curry method yet"))
    val methodSymbol = methodSymbols.find(item =>
      if (item.paramLists.head.isEmpty) {
        types.isEmpty
      } else {
        if (types.isEmpty) {
          item.paramLists.head.isEmpty
        } else {
          //空集forall 为 true
          item.paramLists.head.zip(types).forall(pair => pair._1.info =:= pair._2)
        }
      }).getOrElse(throw new NoSuchMethodException(noSuchMethodException(methodName, allScope, types: _*)(x)))

    val methodMirror = instanceMirror.reflectMethod(methodSymbol)
    methodMirror
  }

  /**
    * 反射构造方法
    * @param types： 方法的参数类型, 可以使用本类中提供的 ru.typeOf[T] 或者ru.typeOf(x) 获得
    * @tparam T: TypeTag[T] 用于记录实例类型
    * @return 返回构造方法 Scala Method Mirror 可以直接使用 `()` 调用
    */
  def reflectConstructorMethod[T: ru.TypeTag](types: ru.Type*): ru.MethodMirror = {
    // 也可以这样 val cm = ru.typeOf[T].typeSymbol.asClass
    val classSymbol = getTypeTag[T].tpe.typeSymbol.asClass
    val classMirror = m.reflectClass(classSymbol)
    val methodSymbols = ru.typeOf[T].decl(ru.termNames.CONSTRUCTOR).asTerm.alternatives.map(_.asMethod)
    val methodSymbol = methodSymbols.find(item =>
      if (item.paramLists.head.isEmpty) {
        types.isEmpty
      } else {
        if (types.isEmpty) {
          item.paramLists.head.isEmpty
        } else {
          //空集forall 为 true
          item.paramLists.head.zip(types).forall(pair => pair._1.info =:= pair._2)
        }
      }
    ).getOrElse(throw new NoSuchMethodException(noSuchConstructorException[T](types: _*)))
    val constructorMethod = classMirror.reflectConstructor(methodSymbol)
    constructorMethod
  }

  /**
    * 反射Java类中的静态方法，由于Scala的反射API无法反射Java的静态方法，所以需要使用Java的API
    * @param methodName：方法名字
    * @param allScope: 是否从父类中寻找，true: 先从本类找，本类找不到再从父类找 false: 只寻找当前类
    * @param types: 方法的参数类型, 可以使用本类中提供的 ru.typeOf[T] 或者ru.typeOf(x) 获得
    * @tparam T: TypeTag[T] 用于记录实例类型
    * @return 返回java.reflect.Method, 需要使用method.invoke来调用
    */
  def reflectJavaStaticMethod[T: ru.TypeTag](methodName: String, allScope: Boolean, types: Class[_]*): Method = {
    val clazz = getRuntimeClass[T]
    if (allScope) {
      def _parents: Seq[Class[_]] = Seq(clazz) ++ _parents.map(_.getSuperclass)

      val parents = _parents.takeWhile(_ != null).toList

      def methods = parents.flatMap(_.getDeclaredMethods)

      val staticMethod = methods.find(item =>
        Modifier.isStatic(item.getModifiers)
          && item.getName == methodName
          && item.getParameterTypes.toSeq.equals(types))
        .getOrElse(throw new NoSuchMethodException(noSuchStaticMethodException[T](methodName, allScope, types: _*)))
      staticMethod.setAccessible(true)
      staticMethod
    } else {
      val staticMethod = clazz.getDeclaredMethod(methodName, types: _*)
      if (!Modifier.isStatic(staticMethod.getModifiers)) {
        throw new NoSuchMethodException(noSuchStaticMethodException[T](methodName, allScope, types: _*))
      }
      staticMethod.setAccessible(true)
      staticMethod
    }
  }

  /**
    *
    * @param fieldName: 字段名称
    * @param allScope： 是否从父类中寻找，true: 先从本类找，本类找不到再从父类找 false: 只寻找当前类
    * @param x： 实例对象
    * @tparam T： TypeTag[T] 用于记录实例类型
    * @return 返回Scala FiledMirror，可以直接调用 `get` `set`方法赋值，无需考虑是否私有
    */
  def reflectFiled[T: ru.TypeTag : ClassTag](fieldName: String, allScope: Boolean = false)(x: T): ru.FieldMirror = {
    val instanceMirror = m.reflect(x)
    val fieldSymbol = if (allScope) {
      val tmp =
        getTypeTag(x).tpe.member(ru.TermName(fieldName))
      if (tmp.equals(ru.NoSymbol)) {
        throw new NoSuchFieldException(noSuchFieldException[T](fieldName, allScope))
      }
      tmp.asTerm
    } else {
      val tmp =
        getTypeTag(x).tpe.decl(ru.TermName(fieldName))
      if (tmp.equals(ru.NoSymbol)) {
        throw new NoSuchFieldException(noSuchFieldException[T](fieldName, allScope))
      }
      tmp.asTerm
    }
    val filedMirror = instanceMirror.reflectField(fieldSymbol)
    filedMirror
  }

  /**
    * 反射Java 类中静态字段
    * 因为Scala反射API不支持反射Java类静态字段，因此该部分用Java反射API实现
    * @param fieldName： 字段名
    * @param allScope：是否从父类中寻找，true: 先从本类找，本类找不到再从父类找 false: 只寻找当前类
    * @tparam T： TypeTag[T] 用于记录实例类型
    * @return 返回java.reflect.Field  field.set(null, val) field.get(null, val)
    */
  def reflectJavaStaticField[T: ru.TypeTag](fieldName: String, allScope: Boolean = false): Field = {
    val clazz = getRuntimeClass[T]
    if (allScope) {
      def _parents: Seq[Class[_]] = Seq(clazz) ++ _parents.map(_.getSuperclass)

      val parents = _parents.takeWhile(_ != null).toList
      val fields = parents.flatMap(_.getDeclaredFields)
      val staticField = fields.find(item =>
        item.getName == fieldName && Modifier.isStatic(item.getModifiers)).getOrElse(throw new NoSuchFieldException(noSuchFieldException[T](fieldName, allScope)))
      staticField.setAccessible(true)
      staticField
    } else {
      val staticField = clazz.getDeclaredField(fieldName)
      if (!Modifier.isStatic(staticField.getModifiers)) {
        throw new NoSuchFieldException(noSuchStaticFieldException[T](fieldName, allScope))
      }
      staticField.setAccessible(true)
      staticField
    }
  }

  private def noSuchMethodException[T: ru.TypeTag : ClassTag](methodName: String, allScope: Boolean, types: ru.Type*)(x: T): String = {
    s"no such method: $methodName, allScope: $allScope type: $types in ${getRuntimeClass(x)}"
  }

  private def noSuchMethodException[T: ru.TypeTag](methodName: String, allScope: Boolean, types: Class[_]*): String = {
    s"no such method: $methodName, allScope: $allScope type: $types in ${getRuntimeClass[T]}"
  }
  private def noSuchStaticMethodException[T: ru.TypeTag](methodName: String, allScope: Boolean, types: Class[_]*): String = {
    s"no such static method: $methodName, allScope: $allScope type: $types in ${getRuntimeClass[T]}"
  }

  private def noSuchConstructorException[T: ru.TypeTag](types: ru.Type*): String = {
    s"no such constructor method types: $types in ${getRuntimeClass[T]}"
  }

  private def noSuchFieldException[T: ru.TypeTag](fieldName: String, allScope: Boolean = false): String = {
    s"no such field $fieldName, allScopes: $allScope in ${getRuntimeClass[T]}"
  }
  private def noSuchStaticFieldException[T: ru.TypeTag](fieldName: String, allScope: Boolean = false): String = {
    s"no such static field $fieldName, allScopes: $allScope in ${getRuntimeClass[T]}"
  }

  def getTypeTag[T: ru.TypeTag]: ru.TypeTag[T] = {
    ru.typeTag[T]
  }

  def getType[T: ru.TypeTag]: ru.Type = {
    getTypeTag[T].tpe
  }

  def getType[T: ru.TypeTag](obj: T): ru.Type = {
    getTypeTag(obj).tpe
  }

  def getTypeTag[T: ru.TypeTag](obj: T): ru.TypeTag[T] = {
    ru.typeTag[T]
  }

  def getRuntimeClass[T: ru.TypeTag]: ru.RuntimeClass = {
    val tg = ru.typeTag[T]
    tg.mirror.runtimeClass(tg.tpe)
  }

  def getRuntimeClass[T: ru.TypeTag](obj: T): ru.RuntimeClass = {
    val tg = ru.typeTag[T]
    tg.mirror.runtimeClass(tg.tpe)
  }

  def typeToClassTag[T: ru.TypeTag]: ClassTag[T] = {
    ClassTag[T](getTypeTag[T].mirror.runtimeClass(getTypeTag[T].tpe))
  }


}

