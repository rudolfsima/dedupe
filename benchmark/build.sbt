enablePlugins(JmhPlugin)

run in Jmh := (run in Jmh).dependsOn(Keys.compile in Jmh).evaluated