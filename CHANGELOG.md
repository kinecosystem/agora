# Changelog

Note: after v0.3.0, the client SDK can be found in https://github.com/kinecosystem/kin-go-internal.

## [v0.3.0](https://github.com/kinecosystem/agora/releases/tag/v0.3.0)
- Add `Dedupe` support on payments (`Client.SubmitPayment`) and earn batches (`Client.SubmitEarnBatch`)
- `Client.SubmitEarnBatch` now supports submitting only a single transaction and up to 15 earns
- `EarnBatchResult` has been modified and now contains `TxID`, `TxError` and `EarnErrors` instead of `Succeeded` and `Failed` due to the above changes
- Add `PaymentErrors` to `TransactionErrors`

## [v0.2.7](https://github.com/kinecosystem/agora/releases/tag/v0.2.7)
- Fix Kin 2 prod issuer

## [v0.2.6](https://github.com/kinecosystem/agora/releases/tag/v0.2.6)
- Fix Kin 4 account creation bug

## [v0.2.5](https://github.com/kinecosystem/agora/releases/tag/v0.2.5)
- Rename 'WithSenderResolution' to 'WithAccountResolution'
- Add account resolution support to `Client.GetBalance`

## [v0.2.4](https://github.com/kinecosystem/agora/releases/tag/v0.2.4)
- Create new accounts with different token account address

## [v0.2.3](https://github.com/kinecosystem/agora/releases/tag/v0.2.3)
- Add Kin 4 support
- Rename `TxHash` to `TxID` in `Client.GetTransaction`, `TransactionData`, and `EarnResult`
- Add Solana options to `Client` methods
- Mark `SignTransactionRequest.TxHash()` as deprecated in favour of `SignTransactionRequest.TxID()`.
- Check for duplicate signers for Stellar transactions
- Use more accurate kin to quark conversions
- Fixed issue in Solana transaction header calculations

## [v0.2.2](https://github.com/kinecosystem/agora/releases/tag/v0.2.2)
- Add Kin 2 support

## [v0.2.1](https://github.com/kinecosystem/agora/releases/tag/v0.2.1)
- Add user-agent metadata to Agora requests

## [v0.2.0](https://github.com/kinecosystem/agora/releases/tag/v0.2.0)
- Rename `Source` in `Payment` and `EarnBatch` to `Channel` for clarity
- Adjust `ErrBadNonce` handling

## [v0.1.0](https://github.com/kinecosystem/agora/releases/tag/v0.1.0)
- Initial release with Kin 3 support
