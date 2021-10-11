# Agora [![CircleCI](https://circleci.com/gh/kinecosystem/agora.svg?style=svg&circle-token=cef79e0ea851bd883f200f0c2e529ce50c79e237)](https://circleci.com/gh/kinecosystem/agora) [![go.dev client](https://img.shields.io/badge/go.dev-client-007d9c?logo=go&logoColor=white&style=flat-square)](https://pkg.go.dev/github.com/kinecosystem/agora/client)

Agora is a blockchain-agnostic service that acts as the primary endpoint for Kin SDKs (and the apps that use them) to communicate with the Kin blockchain. For an overview of Kin and how Agora fits into a Kin-integrated app, please refer to the [website documentation](https://docs.kin.org).

## Client SDK Usage

See: [here](./docs/sdk.md)

## Using local versions of dependencies

If you are working on other Kin projects, such as [agora-api](https://github.com/kinecosystem/agora-api) or [agora-common](https://github.com/kinecosystem/agora-common), you can use the versions of these dependencies you have locally. This allows you to test agora without deploying it to the test net. This is possible using the "require" keyword in the go.mod file.

To use your locally checked out modules, uncomment the following lines in your go.mod file:

```
//replace github.com/kinecosystem/agora-api => ../agora-api
//replace github.com/kinecosystem/agora-common => ../agora-common
```

Then, use go get to fetch these dependencies.

```
go get -u github.com/kinecosystem/agora-api
go get -u github.com/kinecosystem/agora-common
```

You should be able to use these locally checked-out dependencies. Best of all, your code's imports
will remain unchanged. Once you are done, be sure to not commit changes to your go.mod or go.sum file.