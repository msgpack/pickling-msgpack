import SonatypeKeys._

releaseSettings

sonatypeSettings

organization := "org.msgpack"

organizationName := "msgpack.org"

scalaVersion := "2.10.3"

description := "pickling-based object serialization/deserialization in msgpack format"

version := "0.1-SNAPSHOT"

pomExtra := {
  <url>https://github.com/msgpack/pickling-msgpack</url>
  <licenses>
    <license>
      <name>Apache 2</name>
      <url>http://www.apache.org/licenses/LICENSE-2.0.txt</url>
    </license>
  </licenses>
  <scm>
    <connection>scm:git:github.com/msgpack/pickling-msgpack.git</connection>
    <developerConnection>scm:git:git@github.com:msgpack/pickling-msgpack.git</developerConnection>
    <url>github.com/msgpack/pickling-msgpack.git</url>
  </scm>
  <developers>
    <developer>
      <id>xerial</id>
      <name>Taro L. Saito</name>
      <url>http://xerial.org/leo</url>
    </developer>
  </developers>
}

resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies ++= Seq(
  "org.scalatest"  % "scalatest_2.10" % "2.0" % "test",
  "org.scala-lang" %% "scala-pickling" % "0.8.0-SNAPSHOT",
  "org.xerial" % "xerial-lens" % "3.2.3"
)

scalacOptions ++= Seq("-encoding", "UTF-8", "-deprecation", "-unchecked", "-target:jvm-1.6", "-feature")

logBuffered in Test := false

// Since sbt-0.13.2
incOptions := incOptions.value.withNameHashing(true)