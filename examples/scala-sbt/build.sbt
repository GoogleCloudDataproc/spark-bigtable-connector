/*
 * Copyright 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/** build settings for scala 2.12 */

name := "spark-bigtable-example-scala2.12"
version := "0.1"
scalaVersion := "2.12.18"
val sparkBigtable = "spark-bigtable_2.12"
val sparkBigtableVersion = "0.7.0" /* ${NEXT_VERSION_FLAG} */


/** build settings for scala 2.13 */

/*
name := "spark-bigtable-example-scala2.13"
version := "0.1"
scalaVersion := "2.13.14"
val sparkBigtable = "spark-bigtable_2.13"
*/

useCoursier := false

val sparkVersion = "3.5.1"

resolvers += Resolver.mavenLocal

libraryDependencies ++= Seq(
  ("com.google.cloud.spark.bigtable" % sparkBigtable % sparkBigtableVersion)
    .exclude("io.opencensus", "opencensus-contrib-http-util"),
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.slf4j" % "slf4j-reload4j" % "1.7.36",
  "commons-codec" % "commons-codec" % "1.11",
  "com.google.api" % "gax" % "2.51.0",
  "com.google.auth" % "google-auth-library-credentials" % "1.24.0"
)

val scalatestVersion = "3.2.6"
assembly / test := {}

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) =>
    (xs map {_.toLowerCase}) match {
      case ("manifest.mf" :: Nil) | ("index.list" :: Nil) | ("dependencies" :: Nil) =>
        MergeStrategy.discard
      case ps @ (x :: xs) if ps.last.endsWith(".sf") || ps.last.endsWith(".dsa") || ps.last.endsWith(".rsa") =>
        MergeStrategy.discard
      case _ => MergeStrategy.first
    }
  case _ => MergeStrategy.first
}
