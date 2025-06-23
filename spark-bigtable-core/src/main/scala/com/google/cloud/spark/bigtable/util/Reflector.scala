package com.google.cloud.spark.bigtable.util

import com.google.cloud.spark.bigtable.Logging

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}
import scala.reflect.runtime.universe.{TypeTag, typeOf}

object Reflector extends Logging {

  def createVerifiedInstance[T](
      fullyQualifiedClassName: String,
      constructorArgs: Object*
  )(implicit classTag: ClassTag[T]): Try[T] = {
    // We can't use .getDeclaredConstructor since it only works for exact types,
    // while we want to be able to use compatible types as well. Instead we loop
    // through the list of constructors to check for assignability. The order
    // matters since you can have multiple constructor signatures that change
    // only the order of each type. If multiple constructors match we will
    // use an arbitrary one.
    Try(
      Class.forName(fullyQualifiedClassName)
        .getDeclaredConstructors
        .filter(constructor => constructor.getParameterCount == constructorArgs.size)
        .find(constructor =>
          constructor.getParameterTypes.zip(constructorArgs).forall {
            case (constructorArgType, constructorArg) => constructorArgType.isAssignableFrom(constructorArg.getClass)
          })
        .getOrElse(throw new NoSuchMethodException())
        .newInstance(constructorArgs: _*)
    )
    match {
      case Success(result) =>
        if (classTag.runtimeClass.isInstance(result)) {
          Try(result.asInstanceOf[T])
        } else {
          Failure(new ClassCastException(s"$fullyQualifiedClassName is of an unexpected type"))
        }
      case Failure(e) =>
        Failure(e)
    }
  }
}
