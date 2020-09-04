# Kin Go SDK Internal

The Kin Go SDK enables developers use Kin inside their backend servers. It contains support for blockchain actions
such as creating accounts, sending payments, as well a webhook handlers to assist with implementing Agora webhooks.

## Requirements
* Go

## Installation
```
go get -u github.com/kinecosystem/agora
```

## Overview
The SDK contains two main components, a `Client` and web hook handlers. The `Client` is used for blockchain actions, such as creating accounts sending payments, while the web hook handlers are meant for developers who wish to make
use of Agora Webhooks. For a high-level overview of using Agora, please refer to the [website documentation](https://docs.kin.org).

## Client
The main component of this library is the `Client`, which facilitates access to the Kin blockchain.

### Initialization
At a minimum, the client needs to be instantiated with an `Environment`.

```go
package main

import (
    github.com/kinecosystem/agora/client
)

func main() {
	client, err := client.New(client.EnvironmentTest)
}
```

Apps with [registered](https://docs.kin.org/app-registration) app indexes should initialize the client with their index:

```go
package main

import (
    github.com/kinecosystem/agora/client
)

func main() {
	client, err := client.New(client.EnvironmentTest, client.WithAppIndex(1))
}
```

Some additional options include:
- `WithWhitelister(key)`: The private key of an account that will be used to co-sign all transactions.
- `WithEndpoint(endpoint)`: Specify an endpoint to use in the client. This will be inferred by default from the Environment.

### Usage

#### Create an Account
The createAccount method creates an account with the provided private key.
```go
account, err := client.NewPrivateKey()
err = client.CreateAccount(context.Background(), account)
```

#### Get a Transaction
The `GetTransaction` method gets transaction data by transaction hash.
```go
txHash, err := hex.DecodeString("")
transactionData, err := client.GetTransaction(context.Background(txHash))
```

#### Get an Account Balance
The `GetBalance` method gets the balance of the provided account, in [quarks](https://docs.kin.org/terms-and-concepts#quark)
```go
account, err := client.PublicKeyFromString("")
quarks, err = client.GetBalance(context.Background(), account)
```

#### Submit a Payment
The `SubmitPayment` method submits the provided payment to Agora.
```typescript
var sender client.PrivateKey
var dest client.PublicKey

txHash, err := client.SubmitPayment(context.Background(), client.Payment{
    Sender: sender,
    Destination: dest,
    Type: kin.TransactionTypeEarn,
    // Note: should use client.KinToQuarks when using non-constants.
    Quarks: client.MustKinToQuarks("1.0"),
});
```

A `Payment` has the following required properties:
- `Sender`: The private key of the account from which the payment will be sent.
- `Destination`: The public key of the account to which the payment will be sent.
- `Type`: The transaction type of the payment.
- `Quarks`: The amount of the payment, in [quarks](https://docs.kin.org/terms-and-concepts#quark).

Additionally, it has some optional properties:
- `Channel`: The private key of a channel account to use as the source of the transaction. If unset, `sender` will be used as the transaction source.
- `Invoice`: An [Invoice](https://docs.kin.org/how-it-works#invoices) to associate with this payment. Cannot be set if `memo` is set.
- `Memo` A text memo to include in the transaction. Cannot be set if `invoice` is set.

#### Submit an Earn Batch
The `SubmitEarnBatch` method submits a batch of earns to Agora from a single account. It batches the earns into fewer
transactions where possible and submits as many transactions as necessary to submit all the earns.
```go
var sender client.PrivateKey
var dest1, dest2 client.PublicKey

result, err := client.SubmitEarnBatch(context.Background(), client.EarnBatch{
    Sender: sender,
    Earns: []client.Earn{
        {
            Destination: dest1,
            // Note: should use client.KinToQuarks when using non-constants.
            Quarks:      client.MustKinToQuarks("1.0"),
        },
        {
            Destination: dest2,
            // Note: should use client.KinToQuarks when using non-constants.
            Quarks:      client.MustKinToQuarks("1.0"),
        },
    },
})
```


A single `Earn` has the following properties:
- `Destination`: The public key of the account to which the earn will be sent.
- `Quarks`: The amount of the earn, in [quarks](https://docs.kin.org/terms-and-concepts#quark).
- `Invoice`: (optional) An [Invoice](https://docs.kin.org/how-it-works#invoices) to associate with this earn.

The `submitEarnBatch` method has the following parameters:
- `Sender`:  The private key of the account from which the earns will be sent.
- `Earns`: The list of earns to send.
- `Channel`: (optional): The private key of a channel account to use as the transaction source. If not set, `sender` will be used as the source.
- `Memo`: (optional) A text memo to include in the transaction. Cannot be used if the earns have invoices associated with them.

### Examples
A few examples for creating an account and different ways of submitting payments and batched earns can be found in `examples/client`.

## Webhook Handlers

The SDK offers handler functions to assist  developers with implementing the [Agora webhooks](ttps://docs.kin.org/how-it-works#webhooks)

Only apps that have been assigned an [app index](https://docs.kin.org/app-registration) can make use of Agora webhooks.

### Usage

There are currently two handlers:

- [Events](https://docs.kin.org/how-it-works#events) with `EventsHandler`
- [Sign Transaction](https://docs.kin.org/how-it-works#sign-transaction) with `SignTransactionHandler`

When configuring a webhook, a [webhook secret](https://docs.kin.org/agora/webhook#authentication) can be specified.

#### Events Webhook

To consume events from Agora:

```go
import (
    "http",

    "github.com/kinecosystem/agora/client"
    "github.com/kinecosystem/agora/pkg/webhook/events"
)

const webhookSecret = ""

func eventsHandler(events []events.Event) error {
    // process events
}

// Note: If an empty secret is provided to the handler, all events will be processed.
//       Otherwise, the request signature will be validated to ensure it came from agora.
http.HandleFunc("/events", client.EventsHandler(webhookSecret, eventsHandler))
```

#### Sign Transaction Webhook

To verify and sign transactions related to your app:

```go
import (
    "http",

    "github.com/kinecosystem/agora/client"
    "github.com/kinecosystem/agora/pkg/webhook/events"
)

const webhookSecret = ""

func signHandler(req client.SignTransactionRequest, resp* client.SignTransactionResponse) error {
    // decide whether or not to sign() or reject() the request.
}

// Note: If an empty secret is provided to the handler, all events will be processed.
//       Otherwise, the request signature will be validated to ensure it came from agora.
http.HandleFunc("/sign_transaction", client.SignTransactionHandler(webhookSecret, signHandler))
```

### Example Code

A simple example server implementing both the Events and Sign Transaction webhooks can be found in `examples/webhook/main.go`.
