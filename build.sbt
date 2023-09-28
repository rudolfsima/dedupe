lazy val commonSettings = Seq(
  version := "0.0.1",
  scalaVersion := "2.12.17",
  scalacOptions := "-Xasync" :: "-encoding" :: "utf8" :: "-Xfatal-warnings" :: "-deprecation" :: "-unchecked" :: "-language:higherKinds" :: "-feature" :: Nil,
  publishMavenStyle := true
)

lazy val dedupe = project
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      "commons-io" % "commons-io" % "2.11.0", // TODO: remove
      "org.scalatest" %% "scalatest" % "3.2.2" % Test
    )
  )

lazy val benchmark = project
  .settings(
    commonSettings,
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio" % "2.0.9",
      "dev.zio" %% "zio-streams" % "2.0.9",
      "dev.zio" %% "zio-json" % "0.5.0"
    )
  )
  .dependsOn(dedupe)