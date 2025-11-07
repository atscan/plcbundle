package commands

import (
	"fmt"
	"runtime/debug"
)

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

// VersionCommand handles the version subcommand
func VersionCommand(args []string) error {
	fmt.Printf("plcbundle version %s\n", version)
	fmt.Printf("  commit: %s\n", gitCommit)
	fmt.Printf("  built:  %s\n", buildDate)
	return nil
}

// GetVersion returns the version string
func GetVersion() string {
	return version
}
