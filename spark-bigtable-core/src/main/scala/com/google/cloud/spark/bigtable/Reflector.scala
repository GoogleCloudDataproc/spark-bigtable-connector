package com.google.cloud.spark.bigtable

import com.google.cloud.spark.bigtable.auth.{
  AccessTokenProvider,
  AccessTokenProviderCredentials,
  SparkBigtableCredentialsProvider
}
import com.google.cloud.spark.bigtable.datasources.BigtableClientKey

import java.lang.reflect.InvocationTargetException

object Reflector extends Logging {

  def getCredentialsProvider(
      clientKey: BigtableClientKey
  ): Option[SparkBigtableCredentialsProvider] = {
    clientKey.customAccessTokenProviderFQCN.map { accessTokenProviderFQCN =>
      logInfo(s"Using access token provider: $accessTokenProviderFQCN")
      val accessTokenProviderInstance =
        createVerifiedInstance(accessTokenProviderFQCN, classOf[AccessTokenProvider])
      new SparkBigtableCredentialsProvider(
        new AccessTokenProviderCredentials(accessTokenProviderInstance)
      )
    }
  }

  def createVerifiedInstance[T](
      fullyQualifiedClassName: String,
      requiredClass: Class[T],
      constructorArgs: Object*
  ): T = {
    try {
      val clazz = Class.forName(fullyQualifiedClassName)

      // Create an array of Class objects with the proper type
      val parameterTypes: Array[Class[_]] = constructorArgs.map(_.getClass).toArray

      val result = clazz
        .getDeclaredConstructor(parameterTypes: _*)
        .newInstance(constructorArgs.toArray: _*)

      if (!requiredClass.isInstance(result)) {
        throw new IllegalArgumentException(
          s"${clazz.getCanonicalName} does not implement ${requiredClass.getCanonicalName}"
        )
      }
      result.asInstanceOf[T]
    } catch {
      case e @ (_: ClassNotFoundException | _: InstantiationException | _: IllegalAccessException |
          _: InvocationTargetException | _: NoSuchMethodException) =>
        throw new IllegalArgumentException(
          s"Could not instantiate class [$fullyQualifiedClassName], implementing ${requiredClass.getCanonicalName}",
          e
        )
    }
  }
}
