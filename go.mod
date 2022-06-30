module github.com/tokenized/smart_contract_agent

go 1.14

require (
	github.com/FactomProject/basen v0.0.0-20150613233007-fe3947df716e // indirect
	github.com/FactomProject/btcutilecc v0.0.0-20130527213604-d3a63a5752ec // indirect
	github.com/pkg/errors v0.9.1
	github.com/tokenized/channels v0.0.0-20220628035147-ced190d0f0cb
	github.com/tokenized/config v0.2.1
	github.com/tokenized/pkg v0.4.1-0.20220628034826-ab5f0badf2f0
	github.com/tokenized/specification v1.1.2-0.20220624212115-8ff08a078967
	github.com/tokenized/spynode v0.2.2-0.20220628035011-9815320ab778
)

replace github.com/tokenized/pkg => ../pkg

replace github.com/tokenized/specification => ../specification
