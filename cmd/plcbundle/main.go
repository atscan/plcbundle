package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"tangled.org/atscan.net/plcbundle/cmd/plcbundle/commands"
)

func main() {
	//debug.SetGCPercent(400)

	rootCmd := newRootCommand()

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "plcbundle",
		Short: "DID PLC Bundle Management Tool",
		Long: `plcbundle - DID PLC Bundle Management Tool

Tool for archiving AT Protocol's DID PLC Directory operations
into immutable, cryptographically-chained bundles of 10,000
operations each.

Documentation: https://tangled.org/@atscan.net/plcbundle`,

		Version: GetVersion(),

		Run: func(cmd *cobra.Command, args []string) {
			printRootHelp()
		},
	}

	// GLOBAL FLAGS (available to all commands)
	cmd.PersistentFlags().StringP("dir", "C", "", "Repository directory (default: current directory)")
	cmd.PersistentFlags().BoolP("verbose", "v", false, "Show detailed output and progress")
	cmd.PersistentFlags().BoolP("quiet", "q", false, "Suppress non-error output")
	//cmd.PersistentFlags().Bool("json", false, "Output as JSON (where applicable)")

	// Bundle operations (root level - most common)
	cmd.AddCommand(commands.NewSyncCommand())
	cmd.AddCommand(commands.NewCloneCommand())
	/*cmd.AddCommand(commands.NewPullCommand())
	cmd.AddCommand(commands.NewExportCommand())*/
	cmd.AddCommand(commands.NewStreamCommand())
	//cmd.AddCommand(commands.NewGetCommand())
	cmd.AddCommand(commands.NewRollbackCommand())

	// Status & info (root level)
	cmd.AddCommand(commands.NewStatusCommand())
	cmd.AddCommand(commands.NewLogCommand())
	cmd.AddCommand(commands.NewLsCommand())
	//cmd.AddCommand(commands.NewGapsCommand())
	cmd.AddCommand(commands.NewVerifyCommand())
	cmd.AddCommand(commands.NewDiffCommand())
	/*cmd.AddCommand(commands.NewStatsCommand())
	cmd.AddCommand(commands.NewInspectCommand())*/

	// Namespaced commands
	cmd.AddCommand(commands.NewDIDCommand())
	cmd.AddCommand(commands.NewIndexCommand())
	cmd.AddCommand(commands.NewMempoolCommand())
	/*cmd.AddCommand(commands.NewDetectorCommand())

	// Monitoring & maintenance
	cmd.AddCommand(commands.NewWatchCommand())
	cmd.AddCommand(commands.NewHealCommand())
	cmd.AddCommand(commands.NewCleanCommand())*/

	// Server
	cmd.AddCommand(commands.NewServerCommand())

	// Utilities
	cmd.AddCommand(newVersionCommand())
	cmd.AddCommand(newCompletionCommand())

	return cmd
}

func newVersionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Show version information",
		Run: func(cmd *cobra.Command, args []string) {
			cmd.Printf("plcbundle version %s\n", GetVersion())
			cmd.Printf("  commit: %s\n", getGitCommit())
			cmd.Printf("  built:  %s\n", getBuildDate())
		},
	}
}

func newCompletionCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "completion [bash|zsh|fish|powershell]",
		Short: "Generate shell completion script",
		Long: `Generate shell completion script for your shell.

To load completions:

Bash:
  $ source <(plcbundle completion bash)
  
  # To load automatically:
  $ plcbundle completion bash > /etc/bash_completion.d/plcbundle

Zsh:
  $ plcbundle completion zsh > ~/.zsh/completion/_plcbundle
  
  # Add to ~/.zshrc:
  fpath=(~/.zsh/completion $fpath)

Fish:
  $ plcbundle completion fish > ~/.config/fish/completions/plcbundle.fish`,

		Args:                  cobra.ExactArgs(1),
		ValidArgs:             []string{"bash", "zsh", "fish", "powershell"},
		DisableFlagsInUseLine: true,

		RunE: func(cmd *cobra.Command, args []string) error {
			switch args[0] {
			case "bash":
				return cmd.Root().GenBashCompletion(os.Stdout)
			case "zsh":
				return cmd.Root().GenZshCompletion(os.Stdout)
			case "fish":
				return cmd.Root().GenFishCompletion(os.Stdout, true)
			case "powershell":
				return cmd.Root().GenPowerShellCompletion(os.Stdout)
			}
			return nil
		},
	}
}

func printRootHelp() {
	fmt.Print(`plcbundle ` + GetVersion() + ` - DID PLC Bundle Management

Usage: plcbundle <command> [options]

Main Commands:
  sync                 Fetch new bundles from PLC
  clone <url>          Clone from remote repository
  status               Show repository status
  did resolve <did>    Resolve DID document
  server               Start HTTP server

Command Groups:
  Bundle:   clone, sync, pull, export, stream, get, rollback
  Status:   status, log, gaps, verify, diff, stats, inspect
  DID:      did <lookup|resolve|history|batch|search|stats>
  Index:    index <build|repair|stats|verify>
  Tools:    watch, heal, clean, mempool, detector

Getting Started:
  plcbundle clone https://plc.example.com
  plcbundle sync
  plcbundle status

Run 'plcbundle help' for full documentation
Run 'plcbundle <command> --help' for command help
`)
}
