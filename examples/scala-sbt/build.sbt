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

name := "spark-bigtable-example"

version := "0.1"

scalaVersion := "2.12.19"
val sparkVersion = "3.0.1"

libraryDependencies += "com.google.cloud.spark.bigtable" % "spark-bigtable_2.12" % "0.1.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.slf4j" % "slf4j-reload4j" % "1.7.36",
)

val scalatestVersion = "3.2.6"
test in assembly := {}

ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", "io.netty.versions.properties") => MergeStrategy.first
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", "native", xs @ _*)         => MergeStrategy.first
  case PathList("META-INF", "native-image", xs @ _*)         => MergeStrategy.first
  case PathList("mozilla", "public-suffix-list.txt")         => MergeStrategy.first
  case PathList("google", xs @ _*) => xs match {
    case ps @ (x :: xs) if ps.last.endsWith(".proto") => MergeStrategy.first
    case _ => MergeStrategy.deduplicate
  }
  case PathList("javax", xs @ _*)         => MergeStrategy.first
  case PathList("io", "netty", xs @ _*)         => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith ".proto" => MergeStrategy.first
  case PathList(ps @ _*) if ps.last endsWith "module-info.class" => MergeStrategy.discard
  case x =>
    val oldStrategy = (ThisBuild / assemblyMergeStrategy).value
    oldStrategy(x)
}
