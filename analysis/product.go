package analysis

import (
	"database/sql"
	"fmt"
	"log"
	"time"
)

type ProductAnalysis struct {
	TopOverall    []ProductStat            `json:"top_overall,omitempty"`
	TopByCategory map[string][]ProductStat `json:"top_by_category,omitempty"`
	TopByRegion   map[string][]ProductStat `json:"top_by_region,omitempty"`
}

type ProductStat struct {
	ProductID    string  `json:"product_id"`
	Name         string  `json:"name"`
	Category     string  `json:"category"`
	TotalSold    int     `json:"total_sold"`
	TotalRevenue float64 `json:"total_revenue"`
}

func GetTopProducts(db *sql.DB, startDate, endDate string, limit int) (*ProductAnalysis, error) {
	// Parse and format dates to match MySQL format
	start, err := time.Parse("2006-01-02", startDate)
	if err != nil {
		return nil, fmt.Errorf("invalid start_date format: %v", err)
	}
	end, err := time.Parse("2006-01-02", endDate)
	if err != nil {
		return nil, fmt.Errorf("invalid end_date format: %v", err)
	}

	// Debug log
	log.Printf("Analyzing products between %s and %s", start.Format("2006-01-02"), end.Format("2006-01-02"))

	result := &ProductAnalysis{
		TopByCategory: make(map[string][]ProductStat),
		TopByRegion:   make(map[string][]ProductStat),
	}

	// Debug query
	verifyQuery := `
		SELECT COUNT(*) 
		FROM orders 
		WHERE sale_date BETWEEN ? AND ?`

	var count int
	if err := db.QueryRow(verifyQuery, start.Format("2006-01-02"), end.Format("2006-01-02")).Scan(&count); err != nil {
		return nil, fmt.Errorf("error verifying data: %v", err)
	}
	log.Printf("Found %d orders in date range", count)

	// Get top products overall
	overall, err := getTopProductsOverall(db, start.Format("2006-01-02"), end.Format("2006-01-02"), limit)
	if err != nil {
		return nil, fmt.Errorf("error getting top products overall: %v", err)
	}
	result.TopOverall = overall

	// Get top products by category
	byCategory, err := getTopProductsByCategory(db, start.Format("2006-01-02"), end.Format("2006-01-02"), limit)
	if err != nil {
		return nil, fmt.Errorf("error getting top products by category: %v", err)
	}
	result.TopByCategory = byCategory

	// Get top products by region
	byRegion, err := getTopProductsByRegion(db, start.Format("2006-01-02"), end.Format("2006-01-02"), limit)
	if err != nil {
		return nil, fmt.Errorf("error getting top products by region: %v", err)
	}
	result.TopByRegion = byRegion

	return result, nil
}

func getTopProductsOverall(db *sql.DB, startDate, endDate string, limit int) ([]ProductStat, error) {
	query := `
		SELECT 
			p.id,
			p.name,
			p.category,
			SUM(o.quantity) as total_sold,
			SUM(o.quantity * o.unit_price * (1 - o.discount)) as total_revenue
		FROM orders o
		JOIN products p ON o.product_id = p.id
		WHERE DATE(o.sale_date) BETWEEN ? AND ?
		GROUP BY p.id, p.name, p.category
		ORDER BY total_sold DESC
		LIMIT ?`

	return executeProductQuery(db, query, startDate, endDate, limit)
}

func getTopProductsByCategory(db *sql.DB, startDate, endDate string, limit int) (map[string][]ProductStat, error) {
	query := `
		WITH RankedProducts AS (
			SELECT 
				p.id,
				p.name,
				p.category,
				SUM(o.quantity) as total_sold,
				SUM(o.quantity * o.unit_price * (1 - o.discount)) as total_revenue,
				ROW_NUMBER() OVER (PARTITION BY p.category ORDER BY SUM(o.quantity) DESC) as rn
			FROM orders o
			JOIN products p ON o.product_id = p.id
			WHERE DATE(o.sale_date) BETWEEN ? AND ?
			GROUP BY p.id, p.name, p.category
		)
		SELECT id, name, category, total_sold, total_revenue
		FROM RankedProducts
		WHERE rn <= ?`

	rows, err := db.Query(query, startDate, endDate, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string][]ProductStat)
	for rows.Next() {
		var stat ProductStat
		if err := rows.Scan(&stat.ProductID, &stat.Name, &stat.Category, &stat.TotalSold, &stat.TotalRevenue); err != nil {
			return nil, err
		}
		result[stat.Category] = append(result[stat.Category], stat)
	}
	return result, nil
}

func getTopProductsByRegion(db *sql.DB, startDate, endDate string, limit int) (map[string][]ProductStat, error) {
	query := `
		WITH RankedProducts AS (
			SELECT 
				p.id,
				p.name,
				p.category,
				o.region,
				SUM(o.quantity) as total_sold,
				SUM(o.quantity * o.unit_price * (1 - o.discount)) as total_revenue,
				ROW_NUMBER() OVER (PARTITION BY o.region ORDER BY SUM(o.quantity) DESC) as rn
			FROM orders o
			JOIN products p ON o.product_id = p.id
			WHERE DATE(o.sale_date) BETWEEN ? AND ?
			GROUP BY p.id, p.name, p.category, o.region
		)
		SELECT id, name, category, region, total_sold, total_revenue
		FROM RankedProducts
		WHERE rn <= ?`

	rows, err := db.Query(query, startDate, endDate, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string][]ProductStat)
	for rows.Next() {
		var stat ProductStat
		var region string
		if err := rows.Scan(&stat.ProductID, &stat.Name, &stat.Category, &region, &stat.TotalSold, &stat.TotalRevenue); err != nil {
			return nil, err
		}
		result[region] = append(result[region], stat)
	}
	return result, nil
}

func executeProductQuery(db *sql.DB, query string, startDate, endDate string, limit int) ([]ProductStat, error) {
	rows, err := db.Query(query, startDate, endDate, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var results []ProductStat
	for rows.Next() {
		var stat ProductStat
		if err := rows.Scan(&stat.ProductID, &stat.Name, &stat.Category, &stat.TotalSold, &stat.TotalRevenue); err != nil {
			return nil, err
		}
		results = append(results, stat)
	}
	return results, nil
}
