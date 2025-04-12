module github.com/santhosh/sales-analysis

go 1.23.4

replace github.com/santhoshm25/sales-analysis/analysis => ./analysis

require (
	github.com/go-sql-driver/mysql v1.9.2
	github.com/joho/godotenv v1.5.1
)

require filippo.io/edwards25519 v1.1.0 // indirect
