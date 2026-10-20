package cli

import (
	"io"

	"github.com/spf13/cobra"

	"github.com/stellar-optics/stellar-prism/pkg/stream"
)

func newStorageCmd(g *globalFlags, stdout, stderr io.Writer) *cobra.Command {
	var (
		sf          streamFlags
		contractIDs []string
		changeTypes []string
		entryTypes  []string
	)

	cmd := &cobra.Command{
		Use:   "storage",
		Short: "Stream contract storage changes",
		Long: `Stream contract data, contract code and TTL mutations extracted from
transaction metadata.

Every record names the change type, the ledger entry type and, for contract
data, the contract and durability. State-only entries are omitted: they record
what a transaction read, not what it changed.

Contract code and TTL entries are not tied to a contract address, so --contract
omits them and shows only that contract's data.`,
		Example: `  # Follow every contract storage mutation
  prism storage --follow

  # Only one contract's persistent data, created or updated
  prism storage --contract CBGSB... --change created --change updated --follow

  # Contract code and TTL changes, NDJSON into jq
  prism storage --entry contract_code --entry ttl --json | jq -r '.wasmHash'`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			client, closeClient, err := openClient(g, stderr)
			if err != nil {
				return err
			}
			defer closeClient()

			from := sf.from
			if from == 0 {
				latest, err := latestLedger(cmd.Context(), client)
				if err != nil {
					return err
				}
				from = latest
			}

			src, err := stream.NewStorageChangeSource(client, stream.StorageChangeQuery{
				StartLedger: from,
				PageLimit:   sf.pageSize,
				ContractIDs: contractIDs,
				ChangeTypes: changeTypes,
				EntryTypes:  entryTypes,
			})
			if err != nil {
				return err
			}
			return runStream(cmd.Context(), src, &sf, g, stdout, stderr)
		},
	}

	cmd.Flags().StringSliceVar(&contractIDs, "contract", nil,
		"contract ID to watch; repeatable")
	cmd.Flags().StringSliceVar(&changeTypes, "change", nil,
		"change type to include: created, updated, removed or restored (default: all)")
	cmd.Flags().StringSliceVar(&entryTypes, "entry", nil,
		"entry type to include: contract_data, contract_code or ttl (default: all)")
	sf.register(cmd, stream.DefaultStorageChangePageLimit)

	return cmd
}
