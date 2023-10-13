package main

import (
	"github.com/tokenized/smart_contract_agent/cmd/diagnostics/commands"

	"github.com/spf13/cobra"
)

var scCmd = &cobra.Command{
	Use:   "diagnostics",
	Short: "Smart Contract Agent Diagnostics CLI",
}

func main() {
	scCmd.AddCommand(commands.Tx)
	scCmd.AddCommand(commands.BalanceAudit)
	scCmd.AddCommand(commands.RepairPending)
	scCmd.Execute()
}
