# Changelog

## Unreleased

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
