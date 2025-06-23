package com.google.cloud.spark.bigtable.util

import com.google.api.gax.core.CredentialsProvider
import com.google.auth.Credentials
import com.google.cloud.spark.bigtable.Logging
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite

import scala.util.{Failure, Success}

class ClassWithOnlyEmptyConstructor {
}

class ClassWithoutEmptyConstructor(val parameter: String) {
}

class ClassWithEmptyAndNonEmptyConstructor(val parameter: String) {
  def this() = this("default")
}

class ClassWithMultipleParameters(val param1: String, val param2: String) {
}

class SuperType {
}

class SubType(val param1: String) extends SuperType {
}

class ReflectorTest
  extends AnyFunSuite
    with Logging {
  test("Can instantiate class with only empty constructor") {
    val fqcn = new ClassWithOnlyEmptyConstructor().getClass.getName

    Reflector.createVerifiedInstance[ClassWithOnlyEmptyConstructor](fqcn) match {
      case Success(instance) => assert(instance.isInstanceOf[ClassWithOnlyEmptyConstructor])
      case _ => fail("Unexpected type")
    }
  }

  test("Can instantiate class with only non empty constructor") {
    val fqcn = new ClassWithoutEmptyConstructor("a").getClass.getName

    Reflector.createVerifiedInstance[ClassWithoutEmptyConstructor](fqcn, "expected parameter") match {
      case Success(instance) =>
        assert(instance.parameter == "expected parameter")
      case _ => fail("Unexpected type")
    }
  }

  test("Can instantiate class with empty and non empty constructor without passing any argument") {
    val fqcn = new ClassWithEmptyAndNonEmptyConstructor().getClass.getName

    Reflector.createVerifiedInstance[ClassWithEmptyAndNonEmptyConstructor](fqcn) match {
      case Success(instance) =>
        assert(instance.parameter == "default")
      case _ => fail("Unexpected type")
    }
  }

  test("Can instantiate class with empty and non empty constructor passing arguments") {
    val fqcn = new ClassWithEmptyAndNonEmptyConstructor().getClass.getName

    Reflector.createVerifiedInstance[ClassWithEmptyAndNonEmptyConstructor](fqcn, "expected parameter") match {
      case Success(instance) =>
        assert(instance.parameter == "expected parameter")
      case _ => fail("Unexpected type")
    }
  }

  test("Returns failure if class does not exist") {
    val fqcn = "a.class.with.this.name.shouldnt.exist.right"

    Reflector.createVerifiedInstance(fqcn) match {
      case Failure(exception) => assert(exception.isInstanceOf[ClassNotFoundException])
      case _ => fail("Unexpected type")
    }
  }

  test("Returns failure if constructor does not exist") {
    val onlyEmptyConstructorFqcn = new ClassWithOnlyEmptyConstructor().getClass.getName

    Reflector.createVerifiedInstance(onlyEmptyConstructorFqcn, "parameter") match {
      case Failure(exception) => assert(exception.isInstanceOf[NoSuchMethodException])
      case _ => fail("Unexpected type")
    }

    val onlyNonEmptyConstructor = new ClassWithoutEmptyConstructor("a").getClass.getName

    Reflector.createVerifiedInstance(onlyNonEmptyConstructor) match {
      case Failure(exception) => assert(exception.isInstanceOf[NoSuchMethodException])
      case _ => fail("Unexpected type")
    }

    Reflector.createVerifiedInstance(onlyNonEmptyConstructor, "a", "b") match {
      case Failure(exception) => assert(exception.isInstanceOf[NoSuchMethodException])
      case _ => fail("Unexpected type")
    }
  }

  test("Can instantiate class with constructor with multiple parameters") {
    val fqcn = new ClassWithMultipleParameters("a", "a").getClass.getName

    Reflector.createVerifiedInstance[ClassWithMultipleParameters](fqcn, "param1", "param2") match {
      case Success(instance) =>
        assert(instance.param1 == "param1")
        assert(instance.param2 == "param2")
      case _ => fail("Unexpected type")
    }
  }

  test("Returns failure if class is not the right type") {
    val fqcn = new ClassWithOnlyEmptyConstructor().getClass.getName

    Reflector.createVerifiedInstance[ClassWithEmptyAndNonEmptyConstructor](fqcn) match {
      case Failure(exception) => assert(exception.isInstanceOf[ClassCastException])
      case _ => fail("Unexpected type")
    }
  }

  test("Can instantiate subtypes") {
    val fqcn = new SubType("a").getClass.getName

    Reflector.createVerifiedInstance[SuperType](fqcn, "expected") match {
      case Success(result) => assert(result.asInstanceOf[SubType].param1 == "expected")
      case _ => fail("Unexpected type")
    }
  }
}
