package main

import (
	"bytes"
	"log"
	"net/http"
	"os"

	"github.com/kinecosystem/agora/client"
	"github.com/kinecosystem/agora/pkg/webhook/events"
)

var whitelistKey client.PrivateKey

// eventsHandler is used to listen to completed transactions related to our app
// (as configured by the AppIndex, or legacy memo field).
func eventsHandler(events []events.Event) error {
	for _, e := range events {
		if e.TransactionEvent == nil {
			log.Println("received event:", e)
			continue
		}

		log.Printf("transaction completed: %x", e.TransactionEvent.TxID)
	}

	return nil
}

// signHandler allows us to sign or reject transactions related to our app.
// (as configured by the AppIndex, or legacy memo field).
func signHandler(req client.SignTransactionRequest, resp *client.SignTransactionResponse) error {
	log.Printf("SignRequest for <'%s','%s'>", req.UserID, req.UserPasskey)
	txHash, err := req.TxHash()
	if err != nil {
		return err
	}

	for i, p := range req.Payments {
		// Double check that the transaction crafter is not trying to impersonate us.
		if bytes.Equal(p.Sender, whitelistKey.Public()) {
			log.Printf("rejecting: source is whitelist")
			resp.Reject()
			return nil
		}

		// In this example, we don't want to sign transactions that are not sending
		// Kin to ourselves. Other application use cases may not have this restriction.
		if !bytes.Equal(p.Destination, whitelistKey.Public()) {
			log.Printf("rejecting: bad dest(%x), expected %x", p.Sender, whitelistKey.Public())
			resp.MarkWrongDestination(i)
		}

		// If the transaction crafter submitted an invoice, make sure the SKU is set.
		//
		// Note: the SKU is optional, but we simulate a rejection here for testing.
		// Applications may wish to cross-check their own databases for the item
		// being purchased. If the user has already purchased said 'item', they
		// may wish to use MarkAlreadyPaid().
		if p.Invoice != nil {
			for _, item := range p.Invoice.Items {
				if len(item.Sku) == 0 {
					log.Println("rejecting: empty sku")
					resp.MarkSKUNotFound(i)
				}
			}
		}
	}

	// Note: Agora will _not_ forward a rejected transaction to the blockchain,
	//       but it's safer to check that here as well.
	if resp.IsRejected() {
		log.Printf("transaction rejected: %x (%d payments)\n", txHash, len(req.Payments))
		return nil
	}

	log.Printf("transaction approved: %x (%d payments)\n", txHash, len(req.Payments))

	// Note: This allows agora to forward the transaction to the blockchain. However,
	// it does not indicate that it will be submitted successfully, or that the transaction
	// will be successful. For example, if sender has insufficient funds.
	//
	// Backends may keep track of the transaction themselves via the req.TxHash(), and rely
	// on either the Events handler or polling to get the status.
	return resp.Sign(whitelistKey)
}

func main() {
	webhookSecret := os.Getenv("WEBHOOK_SECRET")
	if webhookSecret == "" {
		log.Fatal("missing webhook secret")
	}

	var err error
	whitelistSeed := os.Getenv("WHITELIST_SEED")
	whitelistKey, err = client.PrivateKeyFromString(whitelistSeed)
	if err != nil {
		log.Fatal("invalid whitelist seed")
	}

	env := client.Environment(os.Getenv("ENVIRONMENT"))
	switch env {
	case client.EnvironmentTest, client.EnvironmentProd:
	default:
		log.Fatalf("unknown environment: %s", env)
	}

	http.HandleFunc("/events", client.EventsHandler(webhookSecret, eventsHandler))
	http.HandleFunc("/sign_transaction", client.SignTransactionHandler(env, webhookSecret, signHandler))
	log.Fatal(http.ListenAndServe(":8080", nil))
}
