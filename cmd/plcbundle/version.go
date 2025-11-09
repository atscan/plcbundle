package main

import "runtime/debug"

var (
	version   = "dev"
	gitCommit = "unknown"
	buildDate = "unknown"
)

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		if info.Main.Version != "" && info.Main.Version != "(devel)" {
			version = info.Main.Version
		}

		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				if setting.Value != "" {
					gitCommit = setting.Value
					if len(gitCommit) > 7 {
						gitCommit = gitCommit[:7]
					}
				}
			case "vcs.time":
				if setting.Value != "" {
					buildDate = setting.Value
				}
			}
		}
	}
}

// GetVersion returns the version string
func GetVersion() string {
	return version
}

func getGitCommit() string {
	return gitCommit
}

func getBuildDate() string {
	return buildDate
}
