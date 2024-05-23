package main

import (
	"fmt"

	"github.com/if-nil/tcc-toy/pb"
	"github.com/if-nil/tcc-toy/transaction_manager"
)

func main() {
	// Transaction Manager is like a Coordinator in Two Phase Commit (2PC) protocol
	err := transaction_manager.TCCCall(func(tx *transaction_manager.Transaction) transaction_manager.Operation {
		// CallTry is like the first phase in 2PC: Request To Prepare
		_, err := tx.CallTry("localhost:9210", &pb.TryRequest{Param: "tx1"})
		// If any error occurs, the transaction will be cancelled
		if err != nil {
			fmt.Printf("tx1 try failed: %v", err)
			return tx.Cancel()
		}
		_, err = tx.CallTry("localhost:9211", &pb.TryRequest{Param: "tx2"})
		if err != nil {
			fmt.Printf("tx2 try failed: %v", err)
			return tx.Cancel()
		}
		fmt.Printf("tx1 and tx2 try success")
		return tx.Commit()
	})
	if err != nil {
		fmt.Errorf("transaction failed: %v", err)
	}
}
