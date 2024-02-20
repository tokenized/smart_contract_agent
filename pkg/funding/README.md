# Smart Contract Agent Response Funding

A smart contract agent receives requests in the form of a Bitcoin transaction. The agent then spends an output of that request transaction to provide a response. The combination of the two transactions represents an action. Actions include forming contracts, creating instruments, transferring tokens, and more.

The output of the request transaction that the agent spends must include enough bitcoin to cover all fees including the contract action fee, any transfer fees, and the Bitcoin transaction mining fee. Since the Bitcoin transaction mining fee is based on the size of the response transaction the creator of the request transaction must be able to estimate the size of the response transaction. This document contains some guidance on performing that calculation.

If the request transaction doesn't provide enough bitcoin in the contract output then the request will be rejected by the smart contract agent. If there is enough funding the smart contract agent will post a transaction containing a Rejection payload on chain, but otherwise will just reject it via any available off chain methods which include peer channels if the request was submitted to the smart contract afent via peer channels.

## Notes

### Variable Size Integer Encoding

Since protobuf and bitcoin transactions use variable size integer encoding (though different methods) we can't predict the exact size without knowing the exact value being encoded, so for simplicity larger encoding sizes are assumed. If increased accuracy is required then a much more complex algorithm can take that into account, though it is difficult to know the exact settlement quantities the smart contract agent will provide in all scenarios.

## Transfers

Token transfers are one of the most dynamic actions that can be performed which makes estimating the size of the response transaction more difficult. There can be many senders and receivers, multiple instruments can be involved, and even Bitcoin can be transferred atomically with the tokens. When a transfer request is approved the response transaction will be a settlement that signals the approval of the transfer and containing information about the new token balances. To calculate the size of this response transaction requires the locking scripts of all senders and receivers (participants) because there will be a dust output for each in the settlement transaction that is used to identify each participant, notify them of their involvement when using address scanning, and can also be used to authorize later actions involving those tokens.

### Multi-Contract Transfers

Multi-contract transfers require the smart contract agents to communicate with each other. This involves the first agent wrapping its settlement payload data in a message transaction to the next agent and so on until the last agent and then the last agent builds the settlement transaction, signs it and wraps it in a message transaction to the previous agent. Then each agent adds their signature and wraps it in a message transaction to the previous contract. When the first agent receives the settlement transaction with the other agent's signatures it can sign it and broadcast it on chain.

Each of these message transactions needs to pay the Bitcoin transaction mining fee. This is called "boomerang" funding because the first agent starts it and it comes back to the first agent.

### Settlement Transaction Structure

The settlement transaction is the transaction that is the approval response to the transfer transaction. It is created and signed by the smart contract agent.

* Input for the smart contract agent that authorizes the response. This ties the response to the request on chain and also authenticates that the smart contract agent is authorizing it.
* Output containing settlement payload.
* Outputs with dust values for each of the participants locking scripts.

### Settlement Payload

The settlement payload is non-executable "op return" data in one output of the settlement transaction. It is [Protobuf](https://protobuf.dev/) encoded and then wrapped in an [Envelope](https://github.com/tokenized/envelope/blob/master/README.md).

* List of instruments
* 8 byte timestamp used to specify order of responses

Each instrument in the settlement payload contains this:

* Integer index to input for the instrument's smart contract agent
* Instrument type (3 bytes)
* Instrument code (20 bytes)
* List of instrument settlements

Each instrument settlement contains this:

* Integer index of output for the participant's locking script
* Integer quantity of new token balance

### Settlement Transaction Size Estimation

Sender locking scripts can be obtained from the ancestor transactions of the transfer transaction.
Receiver locking scripts are encoded in the transfer payload.

Dust limit for each participant's locking script is needed because the settlement transaction will have to provide dust in each participant's output. It can be calculated based on the size of the locking script.

#### Single Contract, No Bitcoin Transfer

Here is a simple algorithm for estimating the size of a transfer that only uses one contract and does not include a bitcoin transfer.

```
BaseTxSize          = 16  // version, lock time, input count, output count
P2PKHOutputSize     = 34  // P2PKH locking script and value
P2PKHInputMaxSize   = 149 // previous outpoint, sequence, and script including signature and public key
OutputSize          = 12  // 64 bit value encoded in transaction output + encoded script length
EnvelopeSize        = 37  // OP_0 OP_RETURN 445 OP_1 0x746573742e544b4e OP_3 0 "T2" + output value
InstrumentSize      = 30  // type and code with some encoding overhead
SettlementEntrySize = 10  // settlement entry, 2 byte index, max 8 byte quantity

// Start with smart contract agent input, contract fee output, and settlement envelope.
responseSize = BaseTxSize + P2PKHInputMaxSize + P2PKHOutputSize + EnvelopeSize
dust = 0

for instrument in transfer.instruments loop
	if new contract or instrument is bitcoin then
		return failure // this algorithm removes this complexity
	end if

	responseSize += InstrumentSize

	for sender in instrument.senders loop
		// Dust output containing sender's locking script and entry in settlement payload
		senderLockingScript = lookupLockingScriptForInput(transferTx, sender.index)
		senderLockingScriptSize = length(senderLockingScript)

		responseSize += OutputSize + senderLockingScriptSize + SettlementEntrySize

		dust += dustLimit(senderLockingScriptSize)
	end loop

	for receiver in instrument.receivers loop
		// Dust output containing receiver's locking script and entry in settlement payload
		receiverLockingScript = decodeRawAddressToLockingScript(receiver.address)
		receiverLockingScriptSize = length(receiverLockingScript)

		responseSize += OutputSize + receiverLockingScriptSize + SettlementEntrySize

		dust += dustLimit(receiverLockingScriptSize)
	end loop
end loop

responseTxFee = roundUpToInteger(responseSize * miningFeeRate)

transferTx.Outputs[contractIndex].Value = contractFee+transferFees+responseTxFee+dust
```

#### Full Algorithm

Here is the full algorithm supporting multi-contract boomerang funding and bitcoin transfers.

```
BaseTxSize                         = 16  // version, lock time, input count, output count
P2PKHOutputSize                    = 34  // P2PKH locking script and value
P2PKHInputMaxSize                  = 149 // previous outpoint, sequence, and script including signature and public key
P2PKHBitcoinAgentTransferInputSize = 320 // input to unlock bitcoin agent transfer outputs
OutputSize                         = 12  // 64 bit value encoded in transaction output + encoded script length
EnvelopeSize                       = 37  // OP_0 OP_RETURN 445 OP_1 0x746573742e544b4e OP_3 0 "T2" + output value
InstrumentSize                     = 30  // type and code with some encoding overhead
SettlementEntrySize                = 10  // settlement entry, 2 byte index, max 8 byte quantity

// Start with smart contract agent input, contract fee output, and settlement envelope.
responseSize = BaseTxSize + P2PKHInputMaxSize + P2PKHOutputSize + EnvelopeSize
payloadSize = EnvelopeSize
dust = 0

contracts = empty contract list

for instrument in transfer.instruments loop
	if instrument is bitcoin then
		for receiver in instrument.receivers loop
			// Unlock bitcoin agent transfer locking script to pay bitcoin receiver.
			responseSize += P2PKHBitcoinAgentTransferInputSize

			// Payment output containing receiver's locking script
			receiverLockingScript = decodeRawAddressToLockingScript(receiver.address)
			receiverLockingScriptSize = length(receiverLockingScript)

			responseSize += OutputSize + receiverLockingScriptSize + SettlementEntrySize
		end loop

		goto start of instrument loop
	end if

	contract = contracts.find(instrument.contractIndex)
	if instrument.contractIndex not found in contracts then
		// New contract
		contract = newContract()
		contracts.add(contract)
		responseSize += P2PKHInputMaxSize // new contract input
	end if

	responseSize += InstrumentSize
	contract.payloadSize += InstrumentSize

	for sender in instrument.senders loop
		// Dust output containing sender's locking script and entry in settlement payload
		senderLockingScript = lookupLockingScriptForInput(transferTx, sender.index)
		senderLockingScriptSize = length(senderLockingScript)

		responseSize += OutputSize + senderLockingScriptSize + SettlementEntrySize

		dust += dustLimit(senderLockingScriptSize)

		contract.payloadSize += SettlementEntrySize
	end loop

	for receiver in instrument.receivers loop
		// Dust output containing receiver's locking script and entry in settlement payload
		receiverLockingScript = decodeRawAddressToLockingScript(receiver.address)
		receiverLockingScriptSize = length(receiverLockingScript)

		responseSize += OutputSize + receiverLockingScriptSize + SettlementEntrySize

		dust += dustLimit(receiverLockingScriptSize)

		contract.payloadSize += SettlementEntrySize
	end loop
end loop

boomerangSize = 0
if size(contracts) > 1 then
	accumulatedPayloadSize = 0
	for contract in contracts[:size(contracts)-1] loop // stop loop before last contract
		accumulatedPayloadSize += contract.payloadSize

		// Settlement request messages from one contract to the next containing accumulated
		// settlement payload.
		// Input from contract, output to next contract and an envelope to wrap the payload
		boomerangSize += BaseTxSize + P2PKHInputMaxSize + P2PKHOutputSize + EnvelopeSize
		boomerangSize += accumulatedPayloadSize

		// Signature request messages from one contract to the previous containing full settlement
		// transaction without all the signatures.
		// Input from contract, output to previous contract and an envelope to wrap the payload
		boomerangSize += BaseTxSize + P2PKHInputMaxSize + P2PKHOutputSize + EnvelopeSize
		boomerangSize += responseSize
	end loop
end if

responseTxFee = roundUpToInteger(responseSize * miningFeeRate)

transferTx.Outputs[contracts[0].index].Value = contractNFee+transferNFees+responseTxFee+dust

for contract in contracts[1:] loop // start loop after first contract
	transferTx.Outputs[contract.index].Value = contract.fee+contract.transferFees
end loop

// Boomerang funding must be added to a second output to the first contract in the transfer tx.
// The first output to that contract must be spent into the settlement tx so the second is needed to
// spend into the first settlement request message tx.
transferTx.Outputs[contracts[0].secondaryIndex].Value = roundUpToInteger(boomerangSize * miningFeeRate)
```

#### Examples

Here are some examples for single contract instrument transfers.

5 P2PKH senders and 5 P2PKH receivers:
* less than 700 bytes for the response transaction
* 35 satoshis to fund the response tx at a mining fee rate of 50 satoshis per 1000 bytes
* 10 satoshis for dust outputs (assuming P2PKH locking scripts have a dust limit of 1 satoshi)
* So the contract output should have the contract fee + any transfer fees + 45 satoshis.

1 P2PKH senders and 2 P2PKH receivers:
* less than 400 bytes for the response transaction
* 20 satoshis to fund the response tx at a mining fee rate of 50 satoshis per 1000 bytes
* 3 satoshis for dust outputs (assuming P2PKH locking scripts have a dust limit of 1 satoshi)
* So the contract output should have the contract fee + any transfer fees + 23 satoshis.

## Other Request Types

Most other request types just include the new full relevant state so you simply estimate the size of the response payload and add the size of the transaction that it will be contained in.