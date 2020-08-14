package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/kinecosystem/agora-common/kin"

	commonpb "github.com/kinecosystem/agora-api/genproto/common/v3"

	"github.com/kinecosystem/agora/client"
)

var (
	senderSeed  = flag.String("sender", "", "Sender seed")
	destAddress = flag.String("dest", "", "Destination address")
)

func main() {
	flag.Parse()

	sender, err := client.PrivateKeyFromString(*senderSeed)
	if err != nil {
		log.Fatal(err)
	}
	dest, err := client.PublicKeyFromString(*destAddress)
	if err != nil {
		log.Fatal(err)
	}

	// Initialize the SDK using AppIndex 2, the test app.
	c, err := client.New(client.EnvironmentTest, client.WithAppIndex(2))
	if err != nil {
		log.Fatal(err)
	}

	// Create a new account
	priv, err := client.NewPrivateKey()
	if err != nil {
		log.Fatal(err)
	}
	err = c.CreateAccount(context.Background(), client.PrivateKey(priv))
	if err != nil {
		log.Fatal(err)
	}

	// Payment with no invoicing.
	txHash, err := c.SubmitPayment(context.Background(), client.Payment{
		Sender:      sender,
		Destination: dest,
		Type:        kin.TransactionTypeP2P,
		Quarks:      client.KinToQuarks(1.0),
	})
	fmt.Printf("Hash: %x, err: %v\n", txHash, err)

	// Payment with an old style memo
	txHash, err = c.SubmitPayment(context.Background(), client.Payment{
		Sender:      sender,
		Destination: dest,
		Type:        kin.TransactionTypeSpend,
		Quarks:      client.KinToQuarks(1.0),
		Memo:        "1-test",
	})

	// Payment with an invoice
	txHash, err = c.SubmitPayment(context.Background(), client.Payment{
		Sender:      sender,
		Destination: dest,
		Type:        kin.TransactionTypeSpend,
		Quarks:      client.KinToQuarks(1.0),
		Invoice: &commonpb.Invoice{
			Items: []*commonpb.Invoice_LineItem{
				{
					Title:       "TestPayment",
					Description: "Optional desc about the payment",
					Amount:      client.KinToQuarks(1.0),
					Sku:         []byte("some opaque sky"),
				},
			},
		},
	})
	fmt.Printf("Hash: %x, err: %v\n", txHash, err)

	// Earn batch with an old style memo
	result, err := c.SubmitEarnBatch(context.Background(), client.EarnBatch{
		Sender: sender,
		Memo:   "1-test",
		Earns: []client.Earn{
			{
				Destination: dest,
				Quarks:      client.KinToQuarks(1.0),
			},
			{
				Destination: dest,
				Quarks:      client.KinToQuarks(1.0),
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Succeeded:")
	for _, p := range result.Succeeded {
		fmt.Printf("\tHash: %x, Receiver: %s\n", p.TxHash, p.Earn.Destination.StellarAddress())
	}
	fmt.Println("Failed:")
	for _, p := range result.Failed {
		fmt.Printf("\tHash: %x, Receiver: %s, Error: %v\n", p.TxHash, p.Earn.Destination.StellarAddress(), p.Error)
	}
}
