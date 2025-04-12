package analysis

import "database/sql"

type RevenueAnalysis struct {
	TotalRevenue float64            `json:"total_revenue"`
	ByProduct    map[string]float64 `json:"by_product,omitempty"`
	ByCategory   map[string]float64 `json:"by_category,omitempty"`
	ByRegion     map[string]float64 `json:"by_region,omitempty"`
}

func CalculateRevenue(db *sql.DB, startDate, endDate, region string) (*RevenueAnalysis, error) {
	query := `
		SELECT 
			SUM((o.unit_price * o.quantity) * (1 - o.discount)) as total_revenue,
			p.name as product_name,
			p.category,
			o.region
		FROM orders o
		JOIN products p ON o.product_id = p.id
		WHERE sale_date BETWEEN ? AND ?
	`
	args := []interface{}{startDate, endDate}

	if region != "" {
		query += " AND region = ?"
		args = append(args, region)
	}

	query += " GROUP BY p.name, p.category, o.region"

	rows, err := db.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := &RevenueAnalysis{
		ByProduct:  make(map[string]float64),
		ByCategory: make(map[string]float64),
		ByRegion:   make(map[string]float64),
	}

	for rows.Next() {
		var revenue float64
		var product, category, region string
		if err := rows.Scan(&revenue, &product, &category, &region); err != nil {
			return nil, err
		}

		result.TotalRevenue += revenue
		result.ByProduct[product] += revenue
		result.ByCategory[category] += revenue
		result.ByRegion[region] += revenue
	}

	return result, nil
}
