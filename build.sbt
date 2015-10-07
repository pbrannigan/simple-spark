name := "simple-spark"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark"   %% "spark-core"                 % "1.3.1",
  "org.apache.spark"   %% "spark-sql"                  % "1.3.1",
  "com.oracle"          % "ojdbc6"                     % "11.2.0.3.0-1"
  )

resolvers += "Terradatum Nexus" at "https://nexus.terradatum.com/content/groups/public"

resolvers += "Terradatum Snapshots Nexus" at "https://nexus.terradatum.com/content/repositories/snapshots"