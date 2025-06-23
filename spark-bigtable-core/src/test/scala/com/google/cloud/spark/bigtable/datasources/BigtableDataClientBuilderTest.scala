package com.google.cloud.spark.bigtable.datasources

import com.google.api.gax.core.{CredentialsProvider, GoogleCredentialsProvider}
import com.google.auth.Credentials
import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.spark.bigtable.Logging
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.funsuite.AnyFunSuite

object ConstructorCallTracker {
  var constructorCalls: scala.collection.mutable.Map[Option[Map[String, String]], Int] = scala.collection.mutable.Map()

  def addConstructorCall(params: Option[Map[String, String]]): Unit = {
    val previousValue = constructorCalls.getOrElse(params, 0)
    constructorCalls = constructorCalls.clone() += params -> (previousValue + 1)
  }
}

class DummyCredentials extends GoogleCredentials {
}

class CustomParameterlessAuthProvider extends CredentialsProvider {
  ConstructorCallTracker.addConstructorCall(None)

  override def getCredentials: Credentials = new DummyCredentials
}

class CustomMapParamConstructorAuthProvider(val params: Map[String, String]) extends CredentialsProvider {
  ConstructorCallTracker.addConstructorCall(Some(params))

  override def getCredentials: Credentials = new DummyCredentials
}

class TwoConstructorsAuthProvider(val params: Map[String, String]) extends CredentialsProvider {
  ConstructorCallTracker.addConstructorCall(Some(params))

  def this() = this(Map("param1" -> "default"))

  override def getCredentials: Credentials = new DummyCredentials
}

class BigtableDataClientBuilderTest extends AnyFunSuite
  with BeforeAndAfter
  with BeforeAndAfterAll
  with Logging {

  var customParameterlessAuthProviderFqcn: String = ""
  var customMapParamConstructorAuthProviderFqcn: String = ""
  var twoConstructorsAuthProviderFqcn: String = ""

  override def beforeAll(): Unit = {
    customParameterlessAuthProviderFqcn = new CustomParameterlessAuthProvider().getClass.getName
    customMapParamConstructorAuthProviderFqcn = new CustomMapParamConstructorAuthProvider(Map()).getClass.getName
    twoConstructorsAuthProviderFqcn = new TwoConstructorsAuthProvider().getClass.getName
  }

  before {
    ConstructorCallTracker.constructorCalls.clear()
  }

  // The credentials provider is hidden from the client's interface, so the
  // best we can do is check if our custom provider is instantiated,
  test("Custom authenticator without parameters is instantiated") {
    val sparkConfig = BigtableSparkConfBuilder()
      .setProjectId("some-project")
      .setInstanceId("some-instance")
      .setCustomAuthenticationProvider(customParameterlessAuthProviderFqcn)
      .build()

    val dataClient = BigtableDataClientBuilder.getHandle(sparkConfig.bigtableClientConfig)

    assert(ConstructorCallTracker.constructorCalls.size == 1)
    assert(ConstructorCallTracker.constructorCalls.get(None).contains(1))
  }

  // The credentials provider is hidden from the client's interface, so the
  // best we can do is check if our custom provider is instantiated,
  test("Custom authenticator with parameters is instantiated") {
    val sparkConfig = BigtableSparkConfBuilder()
      .setProjectId("some-project")
      .setInstanceId("some-instance")
      .setCustomAuthenticationProvider(customMapParamConstructorAuthProviderFqcn, Map(("key", "val")))
      .build()

    val dataClient = BigtableDataClientBuilder.getHandle(sparkConfig.bigtableClientConfig)

    assert(ConstructorCallTracker.constructorCalls.size == 1)
    assert(ConstructorCallTracker.constructorCalls.get(Some(Map("key" -> "val"))).contains(1))
  }

  // The credentials provider is hidden from the client's interface, so the
  // best we can do is check if our custom provider is instantiated,
  test("Custom authenticator with parameterless and Map constructor is called with Map constructor") {
    val sparkConfig = BigtableSparkConfBuilder()
      .setProjectId("some-project")
      .setInstanceId("some-instance")
      .setCustomAuthenticationProvider(twoConstructorsAuthProviderFqcn, Map("key" -> "val"))
      .build()

    val dataClient = BigtableDataClientBuilder.getHandle(sparkConfig.bigtableClientConfig)

    assert(ConstructorCallTracker.constructorCalls.size == 1)
    assert(ConstructorCallTracker.constructorCalls.get(Some(Map("key" -> "val"))).contains(1))
  }
}
