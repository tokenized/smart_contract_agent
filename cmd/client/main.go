package main

import (
	"github.com/tokenized/smart_contract_agent/cmd/client/commands"

	"github.com/spf13/cobra"
)

var scCmd = &cobra.Command{
	Use:   "sca_client",
	Short: "Smart Contract Agent CLI",
}

func main() {
	scCmd.AddCommand(commands.SendRequest)
	scCmd.AddCommand(commands.ExpandTx)
	scCmd.Execute()
}
