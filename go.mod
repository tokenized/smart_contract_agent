module github.com/tokenized/smart_contract_agent

go 1.14

require (
	github.com/pkg/errors v0.9.1
	github.com/tokenized/cacher v0.0.0-20220902194227-f29703eb52bb
	github.com/tokenized/channels v0.0.0-20220902192544-9fd058a84b32
	github.com/tokenized/config v0.2.2-0.20220902160347-43a4340c357e
	github.com/tokenized/logger v0.1.1
	github.com/tokenized/pkg v0.4.1-0.20220902192427-a00dc57bbeb3
	github.com/tokenized/specification v1.1.2-0.20220902163651-058d13f0a70f
	github.com/tokenized/spynode v0.2.2-0.20220902163745-06e7a19f49ee
	github.com/tokenized/threads v0.1.1-0.20220908162622-5e406dccfad8
)

replace github.com/tokenized/cacher => ../cacher
