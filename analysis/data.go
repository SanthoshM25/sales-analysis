package analysis

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
)

func RefreshData(db *sql.DB) error {
	file, err := os.Open("./data/data.csv")
	if err != nil {
		return fmt.Errorf("error opening file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)

	_, err = reader.Read()
	if err != nil {
		return fmt.Errorf("error reading header: %v", err)
	}

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("error starting transaction: %v", err)
	}

	const batchSize = 1000
	customers := make([][]any, 0, batchSize)
	products := make([][]any, 0, batchSize)
	orders := make([][]any, 0, batchSize)
	count := 0
	batchNum := 0
	totalRecords := 0

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("error reading CSV: %v", err)
		}

		customers = append(customers, []any{
			record[2], record[12], record[13], record[14],
		})
		products = append(products, []any{
			record[1], record[3], record[4],
		})
		orders = append(orders, []any{
			record[0], record[2], record[1], record[5], record[6],
			record[7], strings.TrimPrefix(record[8], "$"), record[9],
			strings.TrimPrefix(record[10], "$"), record[11],
		})
		count++
		totalRecords++

		if count >= batchSize {
			batchNum++
			log.Printf("Processing batch %d (%d records)...", batchNum, count)
			if err := executeBatch(tx, customers, products, orders); err != nil {
				tx.Rollback()
				return fmt.Errorf("error in batch %d: %v", batchNum, err)
			}
			log.Printf("Batch %d completed. Total records processed: %d", batchNum, totalRecords)

			customers = customers[:0]
			products = products[:0]
			orders = orders[:0]
			count = 0
		}
	}

	if count > 0 {
		batchNum++
		log.Printf("Processing final batch %d (%d records)...", batchNum, count)
		if err := executeBatch(tx, customers, products, orders); err != nil {
			tx.Rollback()
			return fmt.Errorf("error in final batch: %v", err)
		}
		log.Printf("Final batch completed. Total records processed: %d", totalRecords)
	}

	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return fmt.Errorf("error committing transaction: %v", err)
	}

	log.Printf("Data refresh completed. Processed %d records in %d batches", totalRecords, batchNum)
	return nil
}

func executeBatch(tx *sql.Tx, customers, products, orders [][]interface{}) error {
	{
		customersQuery := strings.Builder{}
		customersQuery.WriteString(`
		INSERT INTO customers (id, name, email, address)
		VALUES `)
		for i := range customers {
			if i > 0 {
				customersQuery.WriteString(",")
			}
			customersQuery.WriteString("(?,?,?,?)")
		}
		customersQuery.WriteString(`
		ON DUPLICATE KEY UPDATE 
		name = VALUES(name),
		email = VALUES(email),
		address = VALUES(address)`)

		customerVals := make([]any, 0, len(customers)*4)
		for _, c := range customers {
			customerVals = append(customerVals, c...)
		}

		if _, err := tx.Exec(customersQuery.String(), customerVals...); err != nil {
			return fmt.Errorf("error batch inserting customers: %v", err)
		}
	}

	{
		productsQuery := strings.Builder{}
		productsQuery.WriteString(`
		INSERT INTO products (id, name, category)
		VALUES `)
		for i := range products {
			if i > 0 {
				productsQuery.WriteString(",")
			}
			productsQuery.WriteString("(?,?,?)")
		}
		productsQuery.WriteString(`
		ON DUPLICATE KEY UPDATE 
		name = VALUES(name),
		category = VALUES(category)`)

		productVals := make([]any, 0, len(products)*3)
		for _, p := range products {
			productVals = append(productVals, p...)
		}

		if _, err := tx.Exec(productsQuery.String(), productVals...); err != nil {
			return fmt.Errorf("error batch inserting products: %v", err)
		}
	}

	{
		ordersQuery := strings.Builder{}
		ordersQuery.WriteString(`
		INSERT INTO orders (id, customer_id, product_id, region, sale_date, 
			quantity, unit_price, discount, shipping_cost, payment_method)
		VALUES `)
		for i := range orders {
			if i > 0 {
				ordersQuery.WriteString(",")
			}
			ordersQuery.WriteString("(?,?,?,?,?,?,?,?,?,?)")
		}
		ordersQuery.WriteString(`
		ON DUPLICATE KEY UPDATE
		customer_id = VALUES(customer_id),
		product_id = VALUES(product_id),
		region = VALUES(region),
		sale_date = VALUES(sale_date),
		quantity = VALUES(quantity),
		unit_price = VALUES(unit_price),
		discount = VALUES(discount),
		shipping_cost = VALUES(shipping_cost),
		payment_method = VALUES(payment_method)`)

		orderVals := make([]any, 0, len(orders)*10)
		for _, o := range orders {
			orderVals = append(orderVals, o...)
		}

		if _, err := tx.Exec(ordersQuery.String(), orderVals...); err != nil {
			return fmt.Errorf("error batch inserting orders: %v", err)
		}
	}
	return nil
}
