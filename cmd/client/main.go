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
	scCmd.AddCommand(commands.SendRequestTx)
	scCmd.AddCommand(commands.ExpandTx)
	scCmd.AddCommand(commands.CreateAgent)
	scCmd.AddCommand(commands.CreateContractOffer)
	scCmd.Execute()
}
