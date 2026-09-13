package main

import (
	"os"

	"github.com/AnkitxRot/ForgeFlow/internal/cli"
)

func main() {
	app := cli.NewApp(os.Stdout, os.Stderr)
	exitCode := app.Run(os.Args[1:])
	os.Exit(exitCode)
}
