package org.apache.spark.sql.hive.util

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

  //reflect method, method can't be curry
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

  def reflectJavaStaticMethod[T: ru.TypeTag](methodName: String, allScope: Boolean, types: Class[_]*): Method = {
    val clazz = getRuntimeClass[T]
    if (allScope) {
      def _parents: Stream[Class[_]] = Stream(clazz) #::: _parents.map(_.getSuperclass)

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

  def reflectJavaStaticField[T: ru.TypeTag](fieldName: String, allScope: Boolean = false): Field = {
    val clazz = getRuntimeClass[T]
    if (allScope) {
      def _parents: Stream[Class[_]] = Stream(clazz) #::: _parents.map(_.getSuperclass)

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
