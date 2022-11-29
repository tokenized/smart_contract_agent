package agents

import (
	"context"

	"github.com/pkg/errors"
	"github.com/tokenized/logger"
	"github.com/tokenized/pkg/bitcoin"
	"github.com/tokenized/specification/dist/golang/actions"
)

type IdentityOracle struct {
	Index     int
	PublicKey bitcoin.PublicKey
}

func (a *Agent) GetIdentityOracles(ctx context.Context) ([]*IdentityOracle, error) {
	a.contract.Lock()
	defer a.contract.Unlock()

	if a.contract.Formation == nil {
		logger.WarnWithFields(ctx, []logger.Field{
			logger.Stringer("contract_locking_script", a.contract.LockingScript),
		}, "Missing contract formation")
		return nil, errors.New("Missing contract formation")
	}

	var result []*IdentityOracle
	for i, oracle := range a.contract.Formation.Oracles {
		if len(oracle.OracleTypes) != 0 {
			isIdentity := false
			for _, typ := range oracle.OracleTypes {
				if typ == actions.ServiceTypeIdentityOracle {
					isIdentity = true
					break
				}
			}

			if !isIdentity {
				continue
			}
		}

		ra, err := bitcoin.DecodeRawAddress(oracle.EntityContract)
		if err != nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("contract_locking_script", a.contract.LockingScript),
			}, "Invalid oracle entity contract address %d : %s", i, err)
			continue
		}

		lockingScript, err := ra.LockingScript()
		if err != nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("contract_locking_script", a.contract.LockingScript),
			}, "Failed to create oracle entity contract locking script %d : %s", i, err)
			continue
		}

		services, err := a.caches.Services.Get(ctx, lockingScript)
		if err != nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("contract_locking_script", a.contract.LockingScript),
				logger.Stringer("service_locking_script", lockingScript),
			}, "Failed to get oracle entity contract service %d : %s", i, err)
			continue
		}

		if services == nil {
			logger.WarnWithFields(ctx, []logger.Field{
				logger.Stringer("contract_locking_script", a.contract.LockingScript),
				logger.Stringer("service_locking_script", lockingScript),
			}, "Oracle entity contract service not found %d : %s", i, err)
			continue
		}

		for _, service := range services.Services {
			if service.Type != actions.ServiceTypeIdentityOracle {
				continue
			}

			result = append(result, &IdentityOracle{
				Index:     i,
				PublicKey: service.PublicKey,
			})
			break
		}

		a.caches.Services.Release(ctx, lockingScript)
	}

	return result, nil
}
