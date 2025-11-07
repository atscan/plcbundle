package main

import (
	"fmt"
	"os"
	"runtime/debug"

	"tangled.org/atscan.net/plcbundle/cmd/plcbundle/commands"
)

func main() {
	debug.SetGCPercent(400)

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	var err error
	switch command {
	case "fetch":
		err = commands.FetchCommand(os.Args[2:])
	case "clone":
		err = commands.CloneCommand(os.Args[2:])
	case "rebuild":
		err = commands.RebuildCommand(os.Args[2:])
	case "verify":
		err = commands.VerifyCommand(os.Args[2:])
	case "info":
		err = commands.InfoCommand(os.Args[2:])
	case "export":
		err = commands.ExportCommand(os.Args[2:])
	case "backfill":
		err = commands.BackfillCommand(os.Args[2:])
	case "mempool":
		err = commands.MempoolCommand(os.Args[2:])
	case "serve":
		err = commands.ServerCommand(os.Args[2:])
	case "compare":
		err = commands.CompareCommand(os.Args[2:])
	case "detector":
		err = commands.DetectorCommand(os.Args[2:])
	case "index":
		err = commands.IndexCommand(os.Args[2:])
	case "get-op":
		err = commands.GetOpCommand(os.Args[2:])
	case "version":
		err = commands.VersionCommand(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Printf(`plcbundle %s - DID PLC Bundle Management Tool

Usage:
  plcbundle <command> [options]

Commands:
  fetch      Fetch next bundle from PLC directory
  clone      Clone bundles from remote HTTP endpoint
  rebuild    Rebuild index from existing bundle files
  verify     Verify bundle integrity
  info       Show bundle information
  export     Export operations from bundles
  backfill   Fetch/load all bundles and stream to stdout
  mempool    Show mempool status and operations
  serve      Start HTTP server to serve bundle data
  compare    Compare local index with target index
  detector   Run spam detectors
  index      Manage DID position index
  get-op     Get specific operation by bundle and position
  version    Show version

Examples:
  plcbundle fetch
  plcbundle clone https://plc.example.com
  plcbundle info --bundles
  plcbundle serve --sync --websocket
  plcbundle detector run invalid_handle --bundles 1-100

`, commands.GetVersion())
}
