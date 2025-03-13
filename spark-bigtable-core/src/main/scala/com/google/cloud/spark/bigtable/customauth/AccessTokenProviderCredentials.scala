package com.google.cloud.spark.bigtable.customauth

import com.google.auth.oauth2.{AccessToken, GoogleCredentials}
import com.google.common.annotations.VisibleForTesting

import java.io.IOException

class AccessTokenProviderCredentials(private val accessTokenProvider: AccessTokenProvider)
    extends GoogleCredentials {
  @throws[IOException]
  override def refreshAccessToken: AccessToken = {
    accessTokenProvider.refresh()
    accessTokenProvider.getAccessToken()
  }
}
