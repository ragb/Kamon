publishMavenStyle := true

if (System.getenv().containsKey("NEXUS_USER") && System.getenv().containsKey("NEXUS_PASS")) {
  val username: String = System.getenv().get("NEXUS_USER")
  val password: String = System.getenv().get("NEXUS_PASS")
  println("Using system env credentials.")
  credentials += Credentials(
    "Sonatype Nexus Repository Manager",
    "nexus.eqeris.org",
    username,
    password
  )
} else {
  println("Using credentials from /.ivy2/.eqeris_credentials")
  credentials += Credentials(Path.userHome / ".ivy2" / ".eqeris_credentials")
}

publishTo <<= version.apply {
  v =>
    val nexus = "https://nexus.eqeris.org/nexus/content/repositories/"
    if (v.trim.endsWith("SNAPSHOT"))
      Some("snapshots" at nexus + "eqeris-snapshot-local/")
    else
      Some("releases" at nexus + "eqeris-releases-local/")
}

publishArtifact in Test := true
