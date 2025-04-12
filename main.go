package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strconv"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
	"github.com/santhosh/sales-analysis/analysis"
)

var db *sql.DB

func main() {
	if err := godotenv.Load(); err != nil {
		log.Fatal("Error loading .env file")
	}

	dbURL := os.Getenv("DATABASE_URL")
	var err error
	db, err = sql.Open("mysql", dbURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	go func() {
		if err := analysis.RefreshData(db); err != nil {
			log.Printf("Error refreshing data: %v\n", err)
		}
	}()

	http.HandleFunc("/api/data/refresh", handleDataRefresh)
	http.HandleFunc("/api/analysis/revenue", handleRevenueAnalysis)
	http.HandleFunc("/api/analysis/products", handleTopProducts)

	log.Println("Server starting on :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handleDataRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := analysis.RefreshData(db); err != nil {
		log.Printf("Error refreshing data: %v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := json.NewEncoder(w).Encode(map[string]string{"status": "success"}); err != nil {
		log.Printf("Error encoding response: %v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func handleRevenueAnalysis(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	startDate := r.URL.Query().Get("start_date")
	endDate := r.URL.Query().Get("end_date")
	region := r.URL.Query().Get("region")

	revenue, err := analysis.CalculateRevenue(db, startDate, endDate, region)
	if err != nil {
		log.Printf("Error calculating revenue: %v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := json.NewEncoder(w).Encode(revenue); err != nil {
		log.Printf("Error encoding response: %v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func handleTopProducts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	startDate := r.URL.Query().Get("start_date")
	endDate := r.URL.Query().Get("end_date")

	log.Printf("Received request for top products between %s and %s\n", startDate, endDate)

	if startDate == "" || endDate == "" {
		http.Error(w, "start_date and end_date are required", http.StatusBadRequest)
		return
	}

	limit := 10
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if l, err := strconv.Atoi(limitStr); err == nil && l > 0 {
			limit = l
		}
	}

	analysis, err := analysis.GetTopProducts(db, startDate, endDate, limit)
	if err != nil {
		log.Printf("Error analyzing products: %v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(analysis); err != nil {
		log.Printf("Error encoding response: %v\n", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
