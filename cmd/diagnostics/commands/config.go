package commands

import "github.com/tokenized/pkg/storage"

type Config struct {
	IsTest  bool           `json:"is_test" envconfig:"IS_TEST"`
	Storage storage.Config `json:"storage" envconfig:"STORAGE"`
}
