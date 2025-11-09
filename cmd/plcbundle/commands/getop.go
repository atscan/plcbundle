package commands

import (
	"context"
	"fmt"
	"strconv"

	"github.com/goccy/go-json"
)

// GetOpCommand handles the get-op subcommand
func GetOpCommand(args []string) error {
	if len(args) < 2 {
		return fmt.Errorf("usage: plcbundle get-op <bundle> <position>\n" +
			"Example: plcbundle get-op 42 1337")
	}

	bundleNum, err := strconv.Atoi(args[0])
	if err != nil {
		return fmt.Errorf("invalid bundle number")
	}

	position, err := strconv.Atoi(args[1])
	if err != nil {
		return fmt.Errorf("invalid position")
	}

	mgr, _, err := getManager(nil)
	if err != nil {
		return err
	}
	defer mgr.Close()

	ctx := context.Background()
	op, err := mgr.LoadOperation(ctx, bundleNum, position)
	if err != nil {
		return err
	}

	// Output JSON
	if len(op.RawJSON) > 0 {
		fmt.Println(string(op.RawJSON))
	} else {
		data, _ := json.Marshal(op)
		fmt.Println(string(data))
	}

	return nil
}
