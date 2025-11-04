package main

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/goccy/go-json"
)

func cmdGetOp() {
	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: plcbundle get-op <bundle> <position>\n")
		fmt.Fprintf(os.Stderr, "Example: plcbundle get-op 42 1337\n")
		os.Exit(1)
	}

	bundleNum, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid bundle number\n")
		os.Exit(1)
	}

	position, err := strconv.Atoi(os.Args[3])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: invalid position\n")
		os.Exit(1)
	}

	mgr, _, err := getManager("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer mgr.Close()

	ctx := context.Background()
	op, err := mgr.LoadOperation(ctx, bundleNum, position)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	// Output JSON
	if len(op.RawJSON) > 0 {
		fmt.Println(string(op.RawJSON))
	} else {
		data, _ := json.Marshal(op)
		fmt.Println(string(data))
	}
}
