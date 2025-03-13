package com.google.cloud.spark.bigtable.customauth

import com.google.api.gax.core.CredentialsProvider
import com.google.auth.Credentials

class BigtableCredentialsProvider(private val credentials: Credentials)
    extends CredentialsProvider {
  override def getCredentials: Credentials = credentials
}
