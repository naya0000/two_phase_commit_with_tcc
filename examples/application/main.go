package main

import (
	"github.com/if-nil/tcc-toy/pb"
	"github.com/if-nil/tcc-toy/transaction_manager"
	"log"
)

func main() {
	err := transaction_manager.TCCCall(func(tx *transaction_manager.Transaction) transaction_manager.Operation {
		_, err := tx.CallTry("localhost:9210", &pb.TryRequest{Param: "tx1"})
		if err != nil {
			log.Printf("tx1 try failed: %v", err)
			return tx.Cancel()
		}
		_, err = tx.CallTry("localhost:9211", &pb.TryRequest{Param: "tx2"})
		if err != nil {
			log.Printf("tx2 try failed: %v", err)
			return tx.Cancel()
		}
		log.Printf("tx1 and tx2 try success")
		return tx.Commit()
	})
	if err != nil {
		log.Fatalf("transaction failed: %v", err)
	}
}
