package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/AnkitxRot/ForgeFlow/internal/version"
)

func main() {
	showVersion := flag.Bool("version", false, "Print version information and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("ForgeFlow %s (commit: %s, built: %s)\n", version.Version, version.Commit, version.BuildDate)
		return
	}

	if flag.NArg() == 0 {
		fmt.Println("ForgeFlow - Distributed Durable Execution Platform")
		fmt.Println("Usage: forgeflow [server|worker|scheduler|job|workflow] [flags]")
		fmt.Println("Run 'forgeflow --help' for options.")
		os.Exit(0)
	}

	subcommand := flag.Arg(0)
	switch subcommand {
	case "version":
		fmt.Printf("ForgeFlow %s\n", version.Version)
	default:
		fmt.Fprintf(os.Stderr, "Unknown subcommand: %s\n", subcommand)
		os.Exit(1)
	}
}
