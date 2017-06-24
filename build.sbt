lazy val `kafka-connect-mssql` =
  (project in file("kafka-connect-mssql"))
    .settings(
      name := "kafka-connect-mssql",
      organization := "com.agoda",
      version := "1.0.0",
      scalaVersion := "2.11.8",
      libraryDependencies ++= Dependencies.Compile.kafkaConnect
    )
    .enablePlugins(UniversalPlugin, JavaServerAppPackaging)
    .settings(
      mainClass in Compile := Option("com.agoda.kafka.connect.Boot"),
      mappings in Universal ++= {
        val resourcesDirectory = (sourceDirectory in Compile).value / "resources"
        for {
          (file, relativePath) <- (resourcesDirectory.*** --- resourcesDirectory) pair relativeTo(resourcesDirectory)
        } yield file -> s"conf/$relativePath"
      }
    )
    .enablePlugins(sbtdocker.DockerPlugin)
    .settings(
      dockerfile in docker := {
        val appDir: File = stage.value
        val targetDir = "/app"

        new Dockerfile {
          from("java")
          expose(8083)
          copy(appDir, targetDir)
          env(("CONFIG_FILE", "application.conf"))
          entryPointRaw(s"$targetDir/bin/${executableScriptName.value} -Dconfig.file=$targetDir/conf/$${CONFIG_FILE}")
        }
      },
      imageNames in docker := Seq(
        ImageName(s"${organization.value}/${name.value}:latest"),
        ImageName(Some(organization.value), Some(name.value), version.value)
      ),
      buildOptions in docker := BuildOptions(
        cache = false,
        removeIntermediateContainers = BuildOptions.Remove.Always
      )
    )

lazy val `functional` =
  (project in file("functional"))
    .settings(
      name := "functional",
      scalaVersion := "2.11.8",
      libraryDependencies ++= Dependencies.Test.functional,
      fork in Test := true
    )
