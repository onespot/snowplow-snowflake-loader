lazy val root = project.in(file("."))
  .settings(BuildSettings.buildSettings)
  .aggregate(core, loader, transformer)

lazy val core = project
  .settings(moduleName := "snowplow-snowflake-core")
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.scalifySettings)
  .settings(libraryDependencies ++= commonDependencies)

lazy val loader = project
  .settings(moduleName := "snowplow-snowflake-loader")
  .settings(BuildSettings.assemblySettings)
  .settings(BuildSettings.buildSettings)
  .settings(
    libraryDependencies ++= Seq(
      Dependencies.snowflakeJdbc
    ) ++ commonDependencies
  )
  .dependsOn(core)

lazy val transformer = project
  .settings(moduleName := "snowplow-snowflake-transformer")
  .settings(BuildSettings.assemblySettings)
  .settings(BuildSettings.buildSettings)
  .settings(
    resolvers ++= Seq(
      "Sonatype OSS Snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"
    ),
    libraryDependencies ++= Seq(
      Dependencies.hadoop,
      Dependencies.spark
    ) ++ commonDependencies
  )
  .dependsOn(core)

lazy val commonDependencies = Seq(
  // Scala
  Dependencies.cats,
  Dependencies.analyticsSdk,
  Dependencies.scopt,
  Dependencies.json4sJackson,
  // Scala (test-only)
  Dependencies.specs2,
  Dependencies.scalazSpecs2,
  Dependencies.scalaCheck
)

