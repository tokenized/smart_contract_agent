# Agents

A smart contract agent is appointed by an issuer to respond to requests in an automated manner.

## Submitting Requests

Requests can be submitted to the blockchain network or directly to the smart contract agent via their peer channel. If they are submitted directly to the smart contract agent then a reply to peer channel can be provided to receive a direct response.

The URL of the peer channel to submit requests to can be found in the `RequestPeerChannel` field of the contract formation.

### Peer Channel Requests

Requests and responses are binary bitcoin scripts.

The structures of the requests are as follows. They are encoded using the [Envelope](https://github.com/tokenized/envelope) protocol to identify the sub-protocols used and [BSOR](https://github.com/tokenized/pkg/tree/master/bsor) (Bitcoin Script Object Representation Format) protocol to encode the structured data.

The base protocol of the envelope is [ETX](https://github.com/tokenized/channels/blob/master/expanded_tx/expanded_tx.go) (Expanded TX) Envelope protocol containing the Bitcoin transaction containing the request. To be valid it must contain the immediate ancestors, the transactions containing outputs being spent by the request transaaction.

Using the Envelope protocol the ETX is then wrapped in a [RT](https://github.com/tokenized/channels/blob/master/reply_to.go) (Reply To) Envelope protocol to specify where the response should be sent.

### BSOR Definition Files

Here are BSOR definition files that can be used in languages other than golang to read and write BSOR data. In golang you can just define a type as a struct with decorators like `bsor:"1"` on each field with the correct field ids.

* [Expanded Tx](https://github.com/tokenized/pkg/blob/master/expanded_tx/expanded_tx.bsor)
* [Ancestor Tx](https://github.com/tokenized/pkg/blob/master/expanded_tx/expanded_tx.bsor)
* [Output](https://github.com/tokenized/pkg/blob/master/expanded_tx/expanded_tx.bsor)
* [Reply To](https://github.com/tokenized/channels/blob/master/bsor/channels.bsor)
* [Response](https://github.com/tokenized/channels/blob/master/bsor/channels.bsor)
* [Signature](https://github.com/tokenized/channels/blob/master/bsor/channels.bsor)

#### Examples

##### Transfer Request

Script:
```
OP_0 OP_RETURN 445 OP_2 "RT" "ETX" 32 OP_0 OP_1 OP_1 0x6d6f636b3a2f2f6d6f636b5f706565725f6368616e6e656c732f6170692f76312f6368616e6e656c2f66643232653161642d373965332d346135342d626432642d3664306238343739616661313f746f6b656e3d65303362653733372d633265352d343764322d396338332d303438323265353337313536 OP_0 OP_3 OP_1 0x01000000028289fdeb705e4da446141ca816369895d54ba9daee612e36ccd93a86b171ba90010000006a4730440220106ca4559de5e20ed1f128edbc15a796c14c91505d8337a33a7cb6103df8ee7702203fb995150971492d95bd385090334ad586222d20bc5513e29153fc777dcbaa3b4121029879e96fdf4bd804109c62f51216453732bc672239cc2ee08c7642c3b1b4eaedffffffff5e9af716c533090bf209874718e8472f4ba09d03a9314bbe178d35d277eb77b6000000006b483045022100f58bd54f65cb135a1db901960f87343f81f4cba2e6b05e0132fc248d764eaec902203d954da09bf3eb0baaf661b64d0b12c5c33562b67d3f072ee74ac5a1f7e3d5d541210238768652f204c5d9d51302293c57edca8e6efdcf0310e1099a31edcb1bc61648ffffffff03c8000000000000001976a9141aaa4695ad08e55c12108d98ade1e473f2f984c188ac000000000000000054006a02bd015108746573742e544b4e5301000254313e0a3c12034343591a1469b7bf699ff972336bf7f5a951860ba2e4e1e9e7220310c0062a1a0a1520875b640a586d14d203fdabe6ea77203949983eab10c0064d000000000000001976a9141ec17f6f422d8624615c35fd69e05606de5ba71e88ac00000000 OP_2 OP_2 OP_1 OP_1 OP_1 0x0100000000035e020000000000000001000000000000001976a9149752a5fabb40426ee63e8d290d5254cb95261f0688ac55030000000000000000000000 OP_1 OP_1 OP_1 0x0100000000022c010000000000001976a914f09594972b89d1d7a8e48cc9c93a7805bb9d1e9f88accb030000000000000000000000 OP_3 OP_2 OP_1 OP_2 OP_1 OP_1 OP_2 0x76a9149752a5fabb40426ee63e8d290d5254cb95261f0688ac OP_1 OP_2 OP_1 300 OP_2 0x76a914f09594972b89d1d7a8e48cc9c93a7805bb9d1e9f88ac
```

Hex Script:
```
006a02bd01520252540345545801200051514c786d6f636b3a2f2f6d6f636b5f706565725f6368616e6e656c732f6170692f76312f6368616e6e656c2f66643232653161642d373965332d346135342d626432642d3664306238343739616661313f746f6b656e3d65303362653733372d633265352d343764322d396338332d3034383232653533373135360053514dd20101000000028289fdeb705e4da446141ca816369895d54ba9daee612e36ccd93a86b171ba90010000006a4730440220106ca4559de5e20ed1f128edbc15a796c14c91505d8337a33a7cb6103df8ee7702203fb995150971492d95bd385090334ad586222d20bc5513e29153fc777dcbaa3b4121029879e96fdf4bd804109c62f51216453732bc672239cc2ee08c7642c3b1b4eaedffffffff5e9af716c533090bf209874718e8472f4ba09d03a9314bbe178d35d277eb77b6000000006b483045022100f58bd54f65cb135a1db901960f87343f81f4cba2e6b05e0132fc248d764eaec902203d954da09bf3eb0baaf661b64d0b12c5c33562b67d3f072ee74ac5a1f7e3d5d541210238768652f204c5d9d51302293c57edca8e6efdcf0310e1099a31edcb1bc61648ffffffff03c8000000000000001976a9141aaa4695ad08e55c12108d98ade1e473f2f984c188ac000000000000000054006a02bd015108746573742e544b4e5301000254313e0a3c12034343591a1469b7bf699ff972336bf7f5a951860ba2e4e1e9e7220310c0062a1a0a1520875b640a586d14d203fdabe6ea77203949983eab10c0064d000000000000001976a9141ec17f6f422d8624615c35fd69e05606de5ba71e88ac0000000052525151513e0100000000035e020000000000000001000000000000001976a9149752a5fabb40426ee63e8d290d5254cb95261f0688ac55030000000000000000000000515151350100000000022c010000000000001976a914f09594972b89d1d7a8e48cc9c93a7805bb9d1e9f88accb030000000000000000000000535251525151521976a9149752a5fabb40426ee63e8d290d5254cb95261f0688ac515251022c01521976a914f09594972b89d1d7a8e48cc9c93a7805bb9d1e9f88ac
```

###### OP_RETURN

It starts with OP_0 `0x00` (same as OP_FALSE) and then OP_RETURN `0x6a` to ensure it is known as non-executable script containing data.

###### Envelope Header

Then the Envelope protocol starts with the 445 `0x02bd01` which is a 2 byte push data containing the hex bytes 0xbd01 representing Envelope version 1.

There is then an OP_2 `0x52` specifying there are 2 Envelope protocols included. The protocols are "RT" `0x025254` (Reply To) and "ETX" `0x03455458` (Expanded Tx).

32 `0x0120` specifies that 32 op codes/push datas follow that are included in the specified protocols.

###### Reply To

OP_0 `0x00` Reply To version 0.

Reply To BSOR fields are defined [here](https://github.com/tokenized/channels/blob/master/bsor/channels.bsor).

OP_1 `0x51` There is one field included.

OP_1 `0x51` Field ID 1

`0x4c78` `0x6d6f636b3a2f2f6d6f636b5f706565725f6368616e6e656c732f6170692f76312f6368616e6e656c2f66643232653161642d373965332d346135342d626432642d3664306238343739616661313f746f6b656e3d65303362653733372d633265352d343764322d396338332d303438323265353337313536` push data containing 120 bytes. This is the peer channel URL. "mock://mock_peer_channels/api/v1/channel/fd22e1ad-79e3-4a54-bd2d-6d0b8479afa1?token=e03be737-c2e5-47d2-9c83-04822e537156". The "mock://" URL protocol specifier is only for internal testing. It will normally be an HTTP URL.

###### Expanded Tx

OP_0 `0x00` Expanded Tx version 0.

Expanded Tx BSOR fields are defined [here](https://github.com/tokenized/pkg/blob/master/expanded_tx/expanded_tx.bsor).

OP_3 `0x53` There are 3 fields included.

OP_1 `0x51` Field ID 1 of Expanded Tx

`0x4dd201` `0x01000000028289fdeb705e4da446141ca816369895d54ba9daee612e36ccd93a86b171ba90010000006a4730440220106ca4559de5e20ed1f128edbc15a796c14c91505d8337a33a7cb6103df8ee7702203fb995150971492d95bd385090334ad586222d20bc5513e29153fc777dcbaa3b4121029879e96fdf4bd804109c62f51216453732bc672239cc2ee08c7642c3b1b4eaedffffffff5e9af716c533090bf209874718e8472f4ba09d03a9314bbe178d35d277eb77b6000000006b483045022100f58bd54f65cb135a1db901960f87343f81f4cba2e6b05e0132fc248d764eaec902203d954da09bf3eb0baaf661b64d0b12c5c33562b67d3f072ee74ac5a1f7e3d5d541210238768652f204c5d9d51302293c57edca8e6efdcf0310e1099a31edcb1bc61648ffffffff03c8000000000000001976a9141aaa4695ad08e55c12108d98ade1e473f2f984c188ac000000000000000054006a02bd015108746573742e544b4e5301000254313e0a3c12034343591a1469b7bf699ff972336bf7f5a951860ba2e4e1e9e7220310c0062a1a0a1520875b640a586d14d203fdabe6ea77203949983eab10c0064d000000000000001976a9141ec17f6f422d8624615c35fd69e05606de5ba71e88ac00000000` push data containing 466 bytes. Field 1 as defined by `bsor:"1"` is a bitcoin p2p network encoded transaction.

OP_2 `0x52` Field ID 2 of Expanded Tx

OP_2 `0x52` Field 2 is a list of ancestors so this specifies 2 items in the list.

OP_1 `0x51` Field 2 is a list of ancestor "pointers" so OP_1 specifies that this item in the list is not null.

This starts an Expanded Tx Ancestor Tx with BSOR fields defined [here](https://github.com/tokenized/pkg/blob/master/expanded_tx/expanded_tx.bsor).

OP_1 `0x51` There is 1 field included.

OP_1 `0x51` Field ID 1 of Ancestor Tx

`0x3e` `0x0100000000035e020000000000000001000000000000001976a9149752a5fabb40426ee63e8d290d5254cb95261f0688ac55030000000000000000000000` push data containing 62 bytes. Field 1 as defined by `bsor:"1"` in Ancestor Tx is a bitcoin p2p network encoded transaction.

OP_1 `0x51` Field 2 of the Expanded Tx is a list of ancestor "pointers" so OP_1 specifies that this item in the list is not null.

OP_1 `0x51` There is 1 field included.

OP_1 `0x51` Field ID 1 of Ancestor Tx

`0x35` `0x0100000000022c010000000000001976a914f09594972b89d1d7a8e48cc9c93a7805bb9d1e9f88accb030000000000000000000000` push data containing 53 bytes. Field 1 as defined by `bsor:"1"` in Ancestor Tx is a bitcoin p2p network encoded transaction.

OP_3 `0x53` Field ID 3 of Expanded Tx

OP_2 `0x52` Field 3 of Expanded Tx is a list of outputs. This specifies there are two items in the list.

OP_1 `0x51` Field 3 of Expanded Tx is a list of "pointers" so OP_1 specifies that this item in the list is not a null.

OP_2 `0x52` There are 2 fields included in the output.

Output BSOR fields are defined [here](https://github.com/tokenized/pkg/blob/master/expanded_tx/expanded_tx.bsor).

OP_1 `0x51` Field ID 1

OP_1 `0x51` The value of the output is 1.

OP_2 `0x52` Field ID 2

`0x19` `0x76a9149752a5fabb40426ee63e8d290d5254cb95261f0688ac` The locking script of the output.

OP_1 `0x51` Field 3 of Expanded Tx is a list of "pointers" so OP_1 specifies that this item in the list is not a null.

OP_2 `0x52` There are 2 fields included.

OP_1 `0x51` Field ID 1

`0x02` `0x2c01` The value of the output is 300. (Little Endian)

OP_2 `0x52` Field ID 2

`0x19` `0x76a914f09594972b89d1d7a8e48cc9c93a7805bb9d1e9f88ac` The locking script of the output.

The SpentOutputs do not need to be provided when the ancestors are included, but this example just happened to have them.

Here is a text representation of the above data.

```
TxId: f25ec40fce99319f51d87b6a06bef702ba66e094ca0f41209ea2e6f6c0d0435a (466 bytes)
  Version: 1
  Inputs:

    Outpoint: 1 - 90ba71b1863ad9cc362e61eedaa94bd595983616a81c1446a44d5e70ebfd8982
    Script: 0x30440220106ca4559de5e20ed1f128edbc15a796c14c91505d8337a33a7cb6103df8ee7702203fb995150971492d95bd385090334ad586222d20bc5513e29153fc777dcbaa3b41 0x029879e96fdf4bd804109c62f51216453732bc672239cc2ee08c7642c3b1b4eaed
    Sequence: ffffffff

    Outpoint: 0 - b677eb77d2358d17be4b31a9039da04b2f47e818478709f20b0933c516f79a5e
    Script: 0x3045022100f58bd54f65cb135a1db901960f87343f81f4cba2e6b05e0132fc248d764eaec902203d954da09bf3eb0baaf661b64d0b12c5c33562b67d3f072ee74ac5a1f7e3d5d541 0x0238768652f204c5d9d51302293c57edca8e6efdcf0310e1099a31edcb1bc61648
    Sequence: ffffffff

  Outputs:

    Value: 0.00000200
    Script: OP_DUP OP_HASH160 0x1aaa4695ad08e55c12108d98ade1e473f2f984c1 OP_EQUALVERIFY OP_CHECKSIG

    Value: 0.00000000
    Script: OP_0 OP_RETURN 445 OP_1 0x746573742e544b4e OP_3 0 "T1" 0x0a3c12034343591a1469b7bf699ff972336bf7f5a951860ba2e4e1e9e7220310c0062a1a0a1520875b640a586d14d203fdabe6ea77203949983eab10c006

    Value: 0.00000077
    Script: OP_DUP OP_HASH160 0x1ec17f6f422d8624615c35fd69e05606de5ba71e OP_EQUALVERIFY OP_CHECKSIG

  LockTime: 0

Fee: 24 (0.051502 sat/byte)
Ancestors: 2
  TxId: 90ba71b1863ad9cc362e61eedaa94bd595983616a81c1446a44d5e70ebfd8982 (62 bytes)
  Version: 1
  Inputs:

  Outputs:

    Value: 0.00000606
    Script:

    Value: 0.00000001
    Script: OP_DUP OP_HASH160 0x9752a5fabb40426ee63e8d290d5254cb95261f06 OP_EQUALVERIFY OP_CHECKSIG

    Value: 0.00000853
    Script:

  LockTime: 0
  0 Miner Responses

  TxId: b677eb77d2358d17be4b31a9039da04b2f47e818478709f20b0933c516f79a5e (53 bytes)
  Version: 1
  Inputs:

  Outputs:

    Value: 0.00000300
    Script: OP_DUP OP_HASH160 0xf09594972b89d1d7a8e48cc9c93a7805bb9d1e9f OP_EQUALVERIFY OP_CHECKSIG

    Value: 0.00000971
    Script:

  LockTime: 0
  0 Miner Responses

Spent Outputs:
  1: OP_DUP OP_HASH160 0x9752a5fabb40426ee63e8d290d5254cb95261f06 OP_EQUALVERIFY OP_CHECKSIG
  300: OP_DUP OP_HASH160 0xf09594972b89d1d7a8e48cc9c93a7805bb9d1e9f OP_EQUALVERIFY OP_CHECKSIG

```

### Peer Channel Responses

Responses can either be in the form of an ETX (Expanded TX) Envelope protocol, if a full transaction response is possible, or a TxID (transaction hash) Envelope protocol or the request transaction, if a response transaction can't be created.

TxID responses will be wrapped using the RE (Response) Envelope protocol containing information about the rejection and the S (Signature) Envelope protocol with a signature from the smart contract agent's key.

ETX responses will contain a response transaction with inputs signed by the smart contract agent's key.

#### Examples

##### Transfer Response Expanded Tx

Script :
```
OP_0 OP_RETURN 445 OP_1 "ETX" OP_10 OP_0 OP_2 OP_1 0x01000000015a43d0c0f6e6a29e20410fca94e066ba02f7be066a7bd8519f3199ce0fc45ef2000000006a4730440220134bc9331005319b7524bf28eee95ee105050775814be887cf7070744fb170f9022004aa136b93bccd3e8862b52c465b68d1102d4a9678b267214051081f1b2caea84121036f3143bdd3f185f17da298482d7bbd3c9a8a906c44e874d884c6507bbd28d15fffffffff0401000000000000001976a9149752a5fabb40426ee63e8d290d5254cb95261f0688ac01000000000000001976a914875b640a586d14d203fdabe6ea77203949983eab88ac00000000000000004a006a02bd015108746573742e544b4e530100025432340a2812034343591a1469b7bf699ff972336bf7f5a951860ba2e4e1e9e722041080fe3c2205080110c006108284f294949b90be17b4000000000000001976a9146dcb0a1f0d4f7f8aab7e0259c70904e99bc2db2888ac00000000 OP_2 OP_1 OP_1 OP_1 OP_1 0x01000000028289fdeb705e4da446141ca816369895d54ba9daee612e36ccd93a86b171ba90010000006a4730440220106ca4559de5e20ed1f128edbc15a796c14c91505d8337a33a7cb6103df8ee7702203fb995150971492d95bd385090334ad586222d20bc5513e29153fc777dcbaa3b4121029879e96fdf4bd804109c62f51216453732bc672239cc2ee08c7642c3b1b4eaedffffffff5e9af716c533090bf209874718e8472f4ba09d03a9314bbe178d35d277eb77b6000000006b483045022100f58bd54f65cb135a1db901960f87343f81f4cba2e6b05e0132fc248d764eaec902203d954da09bf3eb0baaf661b64d0b12c5c33562b67d3f072ee74ac5a1f7e3d5d541210238768652f204c5d9d51302293c57edca8e6efdcf0310e1099a31edcb1bc61648ffffffff03c8000000000000001976a9141aaa4695ad08e55c12108d98ade1e473f2f984c188ac000000000000000054006a02bd015108746573742e544b4e5301000254313e0a3c12034343591a1469b7bf699ff972336bf7f5a951860ba2e4e1e9e7220310c0062a1a0a1520875b640a586d14d203fdabe6ea77203949983eab10c0064d000000000000001976a9141ec17f6f422d8624615c35fd69e05606de5ba71e88ac00000000
```

Script Hex:
```
006a02bd0151034554585a0052514d560101000000015a43d0c0f6e6a29e20410fca94e066ba02f7be066a7bd8519f3199ce0fc45ef2000000006a4730440220134bc9331005319b7524bf28eee95ee105050775814be887cf7070744fb170f9022004aa136b93bccd3e8862b52c465b68d1102d4a9678b267214051081f1b2caea84121036f3143bdd3f185f17da298482d7bbd3c9a8a906c44e874d884c6507bbd28d15fffffffff0401000000000000001976a9149752a5fabb40426ee63e8d290d5254cb95261f0688ac01000000000000001976a914875b640a586d14d203fdabe6ea77203949983eab88ac00000000000000004a006a02bd015108746573742e544b4e530100025432340a2812034343591a1469b7bf699ff972336bf7f5a951860ba2e4e1e9e722041080fe3c2205080110c006108284f294949b90be17b4000000000000001976a9146dcb0a1f0d4f7f8aab7e0259c70904e99bc2db2888ac0000000052515151514dd20101000000028289fdeb705e4da446141ca816369895d54ba9daee612e36ccd93a86b171ba90010000006a4730440220106ca4559de5e20ed1f128edbc15a796c14c91505d8337a33a7cb6103df8ee7702203fb995150971492d95bd385090334ad586222d20bc5513e29153fc777dcbaa3b4121029879e96fdf4bd804109c62f51216453732bc672239cc2ee08c7642c3b1b4eaedffffffff5e9af716c533090bf209874718e8472f4ba09d03a9314bbe178d35d277eb77b6000000006b483045022100f58bd54f65cb135a1db901960f87343f81f4cba2e6b05e0132fc248d764eaec902203d954da09bf3eb0baaf661b64d0b12c5c33562b67d3f072ee74ac5a1f7e3d5d541210238768652f204c5d9d51302293c57edca8e6efdcf0310e1099a31edcb1bc61648ffffffff03c8000000000000001976a9141aaa4695ad08e55c12108d98ade1e473f2f984c188ac000000000000000054006a02bd015108746573742e544b4e5301000254313e0a3c12034343591a1469b7bf699ff972336bf7f5a951860ba2e4e1e9e7220310c0062a1a0a1520875b640a586d14d203fdabe6ea77203949983eab10c0064d000000000000001976a9141ec17f6f422d8624615c35fd69e05606de5ba71e88ac00000000
```

###### OP_RETURN

It starts with OP_0 `0x00` (same as OP_FALSE) and then OP_RETURN `0x6a` to ensure it is known as non-executable script containing data.

###### Envelope Header

Then the Envelope protocol starts with the 445 `0x02bd01` which is a 2 byte push data containing the hex bytes 0xbd01 representing Envelope version 1.

There is then an OP_1 `0x51` specifying there is 1 Envelope protocol included. The protocol is "ETX" `0x03455458` (Expanded Tx).

OP_10 `0x5a` specifies that 10 op codes/push datas follow that are included in the specified protocols.

###### Expanded Tx

OP_0 `0x00` Expanded Tx version 0.

Expanded Tx BSOR fields are defined [here](https://github.com/tokenized/pkg/blob/master/expanded_tx/expanded_tx.bsor).

OP_2 `0x52` There are 2 fields included.

OP_1 `0x51` Field ID 1 of Expanded Tx

`0x4d5601` `0x01000000015a43d0c0f6e6a29e20410fca94e066ba02f7be066a7bd8519f3199ce0fc45ef2000000006a4730440220134bc9331005319b7524bf28eee95ee105050775814be887cf7070744fb170f9022004aa136b93bccd3e8862b52c465b68d1102d4a9678b267214051081f1b2caea84121036f3143bdd3f185f17da298482d7bbd3c9a8a906c44e874d884c6507bbd28d15fffffffff0401000000000000001976a9149752a5fabb40426ee63e8d290d5254cb95261f0688ac01000000000000001976a914875b640a586d14d203fdabe6ea77203949983eab88ac00000000000000004a006a02bd015108746573742e544b4e530100025432340a2812034343591a1469b7bf699ff972336bf7f5a951860ba2e4e1e9e722041080fe3c2205080110c006108284f294949b90be17b4000000000000001976a9146dcb0a1f0d4f7f8aab7e0259c70904e99bc2db2888ac00000000` push data containing 342 bytes. Field 1 as defined by `bsor:"1"` is a bitcoin p2p network encoded transaction.

OP_2 `0x52` Field ID 2

OP_1 `0x51` Field 2 is a list of ancestors so this specifies 1 item in the list.

OP_1 `0x51` Field 2 is a list of ancestor "pointers" so OP_1 specifies that this item in the list is not null.

This starts an Expanded Tx Ancestor Tx with BSOR fields defined [here](https://github.com/tokenized/pkg/blob/master/expanded_tx/expanded_tx.bsor).

OP_2 `0x51` There are 2 fields included.

OP_2 `0x51` Field ID 1

`0x4dd201` `0x01000000028289fdeb705e4da446141ca816369895d54ba9daee612e36ccd93a86b171ba90010000006a4730440220106ca4559de5e20ed1f128edbc15a796c14c91505d8337a33a7cb6103df8ee7702203fb995150971492d95bd385090334ad586222d20bc5513e29153fc777dcbaa3b4121029879e96fdf4bd804109c62f51216453732bc672239cc2ee08c7642c3b1b4eaedffffffff5e9af716c533090bf209874718e8472f4ba09d03a9314bbe178d35d277eb77b6000000006b483045022100f58bd54f65cb135a1db901960f87343f81f4cba2e6b05e0132fc248d764eaec902203d954da09bf3eb0baaf661b64d0b12c5c33562b67d3f072ee74ac5a1f7e3d5d541210238768652f204c5d9d51302293c57edca8e6efdcf0310e1099a31edcb1bc61648ffffffff03c8000000000000001976a9141aaa4695ad08e55c12108d98ade1e473f2f984c188ac000000000000000054006a02bd015108746573742e544b4e5301000254313e0a3c12034343591a1469b7bf699ff972336bf7f5a951860ba2e4e1e9e7220310c0062a1a0a1520875b640a586d14d203fdabe6ea77203949983eab10c0064d000000000000001976a9141ec17f6f422d8624615c35fd69e05606de5ba71e88ac00000000` push data containing 466 bytes. Field 1 as defined by `bsor:"1"` is a bitcoin p2p network encoded transaction.


Here is a text representation of the above data.

```
TxId: 17cd0d9f5bd1654928dbe64be7f008263e49016f077fd29d3b472d1e211f3206 (342 bytes)
  Version: 1
  Inputs:

    Outpoint: 0 - f25ec40fce99319f51d87b6a06bef702ba66e094ca0f41209ea2e6f6c0d0435a
    Script: 0x30440220134bc9331005319b7524bf28eee95ee105050775814be887cf7070744fb170f9022004aa136b93bccd3e8862b52c465b68d1102d4a9678b267214051081f1b2caea841 0x036f3143bdd3f185f17da298482d7bbd3c9a8a906c44e874d884c6507bbd28d15f
    Sequence: ffffffff

  Outputs:

    Value: 0.00000001
    Script: OP_DUP OP_HASH160 0x9752a5fabb40426ee63e8d290d5254cb95261f06 OP_EQUALVERIFY OP_CHECKSIG

    Value: 0.00000001
    Script: OP_DUP OP_HASH160 0x875b640a586d14d203fdabe6ea77203949983eab OP_EQUALVERIFY OP_CHECKSIG

    Value: 0.00000000
    Script: OP_0 OP_RETURN 445 OP_1 0x746573742e544b4e OP_3 0 "T2" 0x0a2812034343591a1469b7bf699ff972336bf7f5a951860ba2e4e1e9e722041080fe3c2205080110c006108284f294949b90be17

    Value: 0.00000180
    Script: OP_DUP OP_HASH160 0x6dcb0a1f0d4f7f8aab7e0259c70904e99bc2db28 OP_EQUALVERIFY OP_CHECKSIG

  LockTime: 0

Fee: 18 (0.052632 sat/byte)
Ancestors: 1
  TxId: f25ec40fce99319f51d87b6a06bef702ba66e094ca0f41209ea2e6f6c0d0435a (466 bytes)
  Version: 1
  Inputs:

    Outpoint: 1 - 90ba71b1863ad9cc362e61eedaa94bd595983616a81c1446a44d5e70ebfd8982
    Script: 0x30440220106ca4559de5e20ed1f128edbc15a796c14c91505d8337a33a7cb6103df8ee7702203fb995150971492d95bd385090334ad586222d20bc5513e29153fc777dcbaa3b41 0x029879e96fdf4bd804109c62f51216453732bc672239cc2ee08c7642c3b1b4eaed
    Sequence: ffffffff

    Outpoint: 0 - b677eb77d2358d17be4b31a9039da04b2f47e818478709f20b0933c516f79a5e
    Script: 0x3045022100f58bd54f65cb135a1db901960f87343f81f4cba2e6b05e0132fc248d764eaec902203d954da09bf3eb0baaf661b64d0b12c5c33562b67d3f072ee74ac5a1f7e3d5d541 0x0238768652f204c5d9d51302293c57edca8e6efdcf0310e1099a31edcb1bc61648
    Sequence: ffffffff

  Outputs:

    Value: 0.00000200
    Script: OP_DUP OP_HASH160 0x1aaa4695ad08e55c12108d98ade1e473f2f984c1 OP_EQUALVERIFY OP_CHECKSIG

    Value: 0.00000000
    Script: OP_0 OP_RETURN 445 OP_1 0x746573742e544b4e OP_3 0 "T1" 0x0a3c12034343591a1469b7bf699ff972336bf7f5a951860ba2e4e1e9e7220310c0062a1a0a1520875b640a586d14d203fdabe6ea77203949983eab10c006

    Value: 0.00000077
    Script: OP_DUP OP_HASH160 0x1ec17f6f422d8624615c35fd69e05606de5ba71e OP_EQUALVERIFY OP_CHECKSIG

  LockTime: 0
  0 Miner Responses
```

##### Transfer Response TxID (Rejection)

Script :
```
OP_0 OP_RETURN 445 OP_3 83 "RE" "TxID" 18 OP_0 OP_2 OP_1 0x30450221008572371e81c6ad95b4eb5bb319fb14d15e9c5143d48ec76cd8c30b52914802510220176c4a8eab70d5adf4fc219fc17212a033864ee4349b89b974430f7850c9fc39 OP_3 0x147bc82a36e4a7aadc63410142f0250869a1a59ce59b772e33af4d4d39314eee OP_0 OP_4 OP_2 OP_1 OP_3 "TKN" OP_4 61 OP_5 0x32302066756e64696e67203c20313031206e6565646564202b203137207478206665653a20496e73756666696369656e742056616c7565 OP_0 0xd7b1f18ac2b428cfb120550c333b33067c9b5aef4749eaa684284c8b087c477b
```

Script Hex:
```
006a02bd01530153025245045478494401120052514730450221008572371e81c6ad95b4eb5bb319fb14d15e9c5143d48ec76cd8c30b52914802510220176c4a8eab70d5adf4fc219fc17212a033864ee4349b89b974430f7850c9fc395320147bc82a36e4a7aadc63410142f0250869a1a59ce59b772e33af4d4d39314eee005452515303544b4e54013d553732302066756e64696e67203c20313031206e6565646564202b203137207478206665653a20496e73756666696369656e742056616c75650020d7b1f18ac2b428cfb120550c333b33067c9b5aef4749eaa684284c8b087c477b
```

###### OP_RETURN

It starts with OP_0 `0x00` (same as OP_FALSE) and then OP_RETURN `0x6a` to ensure it is known as non-executable script containing data.

###### Envelope Header

Then the Envelope protocol starts with the 445 `0x02bd01` which is a 2 byte push data containing the hex bytes 0xbd01 representing Envelope version 1.

There is then an OP_3 `0x53` specifying there is 3 Envelope protocol2 included. The protocols are 83 "S" `0x53` (Signature), "RE" `0x025245` Response, and "TxID" `0x0454784944` (Transaction ID).

18 `0x0112` specifies that 18 op codes/push datas follow that are included in the specified protocols.

This starts a Signature with BSOR fields defined [here](https://github.com/tokenized/channels/blob/master/bsor/channels.bsor).

OP_0 `0x00` Signature version 0

OP_2 `0x52` There are 2 fields included.

OP_1 `0x51` Field ID 1

`0x47` `30450221008572371e81c6ad95b4eb5bb319fb14d15e9c5143d48ec76cd8c30b52914802510220176c4a8eab70d5adf4fc219fc17212a033864ee4349b89b974430f7850c9fc39` push data containing 71 bytes. Field 1 as defined by `bsor:"1"` is a signature (not including a signature hash type byte at the end.

OP_3 `0x53` Field ID 3

`0x20` `0x147bc82a36e4a7aadc63410142f0250869a1a59ce59b772e33af4d4d39314eee` push data containing 32 bytes. Field 3 as defined by `bsor:"3"` is a 32 byte derivation hash.

This starts a Response with BSOR fields defined [here](https://github.com/tokenized/channels/blob/master/bsor/channels.bsor).

OP_0 `0x00` Response version 0

OP_4 `0x54` There are 4 fields included.

OP_2 `0x52` Field ID 2

OP_1 `0x51` Response status is 1, which corresponds with (StatusReject)[https://github.com/tokenized/channels/blob/master/response.go#L22]

OP_3 `0x53` Field ID 3

"TKN" `0x03544b4e` Response code protocol ID. This specifies that the response code corresponds with the Tokenized protocol.

OP_4 `0x54` Field ID 4

61 `0x013d` Response code is 61 which corresponds to [Insufficient Value](https://github.com/tokenized/specification/blob/master/src/resources/develop/Rejections.yaml#L198)

OP_5 `0x55` Field ID 5

`0x37` `0x32302066756e64696e67203c20313031206e6565646564202b203137207478206665653a20496e73756666696369656e742056616c7565` push data containing 55 bytes. Field 5 as defined by `bsor:"5"` is a text note. "20 funding < 101 needed + 17 tx fee: Insufficient Value"

This starts a TxID as defined [here](https://github.com/tokenized/channels/blob/master/txid.go). It is just an op code for the version and a 32 byte push data for the little endian tx hash.

OP_0 `0x00` TxID version 0

`0x20` `0xd7b1f18ac2b428cfb120550c333b33067c9b5aef4749eaa684284c8b087c477b` a push data containing 32 bytes that represent a little endian transaction hash. It would be displayed in text as big endian "7b477c088b4c2884a6ea4947ef5a9b7c06333b330c5520b1cf28b4c28af1b1d7".