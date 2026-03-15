#!/bin/sh
set -e

ES_URL="http://${ES_HOST}:${ES_PORT}"

echo "==> Waiting for Elasticsearch at ${ES_URL}..."
until curl -sf "${ES_URL}/_cluster/health?wait_for_status=green&timeout=1s" > /dev/null 2>&1; do
  sleep 2
done
echo "==> Elasticsearch is ready."

# Check if index already exists
if curl -sf "${ES_URL}/ecommerce" > /dev/null 2>&1; then
  echo "==> Index 'ecommerce' already exists, skipping data load."
  exit 0
fi

echo "==> Creating 'ecommerce' index with mappings..."
curl -sf -X PUT "${ES_URL}/ecommerce" -H 'Content-Type: application/json' -d '{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "properties": {
      "order_id":       { "type": "keyword" },
      "order_date":     { "type": "date" },
      "customer_name":  { "type": "keyword" },
      "customer_email": { "type": "keyword" },
      "country":        { "type": "keyword" },
      "city":           { "type": "keyword" },
      "category":       { "type": "keyword" },
      "product_name":   { "type": "text", "fields": { "keyword": { "type": "keyword" } } },
      "quantity":       { "type": "integer" },
      "unit_price":     { "type": "double" },
      "total_price":    { "type": "double" },
      "payment_method": { "type": "keyword" },
      "status":         { "type": "keyword" }
    }
  }
}'
echo ""

echo "==> Loading e-commerce sample data..."
curl -sf -X POST "${ES_URL}/_bulk" -H 'Content-Type: application/x-ndjson' -d '
{"index":{"_index":"ecommerce","_id":"1"}}
{"order_id":"ORD-001","order_date":"2025-01-15T10:30:00Z","customer_name":"Alice Martin","customer_email":"alice@example.com","country":"France","city":"Paris","category":"Electronics","product_name":"Wireless Headphones","quantity":2,"unit_price":79.99,"total_price":159.98,"payment_method":"Credit Card","status":"delivered"}
{"index":{"_index":"ecommerce","_id":"2"}}
{"order_id":"ORD-002","order_date":"2025-01-15T14:20:00Z","customer_name":"Bob Smith","customer_email":"bob@example.com","country":"United States","city":"New York","category":"Books","product_name":"The Art of SQL","quantity":1,"unit_price":45.00,"total_price":45.00,"payment_method":"PayPal","status":"delivered"}
{"index":{"_index":"ecommerce","_id":"3"}}
{"order_id":"ORD-003","order_date":"2025-01-16T09:15:00Z","customer_name":"Claire Dubois","customer_email":"claire@example.com","country":"France","city":"Lyon","category":"Clothing","product_name":"Winter Jacket","quantity":1,"unit_price":129.50,"total_price":129.50,"payment_method":"Credit Card","status":"shipped"}
{"index":{"_index":"ecommerce","_id":"4"}}
{"order_id":"ORD-004","order_date":"2025-01-16T11:45:00Z","customer_name":"David Chen","customer_email":"david@example.com","country":"Germany","city":"Berlin","category":"Electronics","product_name":"USB-C Hub","quantity":3,"unit_price":35.99,"total_price":107.97,"payment_method":"Credit Card","status":"delivered"}
{"index":{"_index":"ecommerce","_id":"5"}}
{"order_id":"ORD-005","order_date":"2025-01-17T08:00:00Z","customer_name":"Eva Rossi","customer_email":"eva@example.com","country":"Italy","city":"Rome","category":"Home","product_name":"Ceramic Vase","quantity":2,"unit_price":55.00,"total_price":110.00,"payment_method":"Bank Transfer","status":"processing"}
{"index":{"_index":"ecommerce","_id":"6"}}
{"order_id":"ORD-006","order_date":"2025-01-17T16:30:00Z","customer_name":"Frank Mueller","customer_email":"frank@example.com","country":"Germany","city":"Munich","category":"Electronics","product_name":"Mechanical Keyboard","quantity":1,"unit_price":149.99,"total_price":149.99,"payment_method":"PayPal","status":"delivered"}
{"index":{"_index":"ecommerce","_id":"7"}}
{"order_id":"ORD-007","order_date":"2025-01-18T12:00:00Z","customer_name":"Grace Kim","customer_email":"grace@example.com","country":"South Korea","city":"Seoul","category":"Books","product_name":"Elasticsearch in Action","quantity":2,"unit_price":52.00,"total_price":104.00,"payment_method":"Credit Card","status":"shipped"}
{"index":{"_index":"ecommerce","_id":"8"}}
{"order_id":"ORD-008","order_date":"2025-01-18T15:45:00Z","customer_name":"Henri Dupont","customer_email":"henri@example.com","country":"France","city":"Marseille","category":"Clothing","product_name":"Running Shoes","quantity":1,"unit_price":89.99,"total_price":89.99,"payment_method":"Credit Card","status":"delivered"}
{"index":{"_index":"ecommerce","_id":"9"}}
{"order_id":"ORD-009","order_date":"2025-01-19T10:20:00Z","customer_name":"Isabella Garcia","customer_email":"isabella@example.com","country":"Spain","city":"Madrid","category":"Home","product_name":"LED Desk Lamp","quantity":2,"unit_price":42.50,"total_price":85.00,"payment_method":"PayPal","status":"delivered"}
{"index":{"_index":"ecommerce","_id":"10"}}
{"order_id":"ORD-010","order_date":"2025-01-19T13:10:00Z","customer_name":"James Wilson","customer_email":"james@example.com","country":"United Kingdom","city":"London","category":"Electronics","product_name":"Portable SSD 1TB","quantity":1,"unit_price":99.99,"total_price":99.99,"payment_method":"Credit Card","status":"processing"}
{"index":{"_index":"ecommerce","_id":"11"}}
{"order_id":"ORD-011","order_date":"2025-01-20T09:00:00Z","customer_name":"Keiko Tanaka","customer_email":"keiko@example.com","country":"Japan","city":"Tokyo","category":"Books","product_name":"Data Engineering with Apache Arrow","quantity":1,"unit_price":59.99,"total_price":59.99,"payment_method":"Credit Card","status":"delivered"}
{"index":{"_index":"ecommerce","_id":"12"}}
{"order_id":"ORD-012","order_date":"2025-01-20T14:30:00Z","customer_name":"Luca Bianchi","customer_email":"luca@example.com","country":"Italy","city":"Milan","category":"Clothing","product_name":"Cashmere Scarf","quantity":3,"unit_price":65.00,"total_price":195.00,"payment_method":"Bank Transfer","status":"shipped"}
{"index":{"_index":"ecommerce","_id":"13"}}
{"order_id":"ORD-013","order_date":"2025-01-21T11:15:00Z","customer_name":"Maria Santos","customer_email":"maria@example.com","country":"Brazil","city":"Sao Paulo","category":"Electronics","product_name":"Wireless Mouse","quantity":4,"unit_price":29.99,"total_price":119.96,"payment_method":"Credit Card","status":"delivered"}
{"index":{"_index":"ecommerce","_id":"14"}}
{"order_id":"ORD-014","order_date":"2025-01-21T17:00:00Z","customer_name":"Nils Johansson","customer_email":"nils@example.com","country":"Sweden","city":"Stockholm","category":"Home","product_name":"Minimalist Wall Clock","quantity":1,"unit_price":75.00,"total_price":75.00,"payment_method":"PayPal","status":"delivered"}
{"index":{"_index":"ecommerce","_id":"15"}}
{"order_id":"ORD-015","order_date":"2025-01-22T08:45:00Z","customer_name":"Olivia Brown","customer_email":"olivia@example.com","country":"United States","city":"San Francisco","category":"Electronics","product_name":"4K Monitor","quantity":1,"unit_price":349.99,"total_price":349.99,"payment_method":"Credit Card","status":"processing"}
{"index":{"_index":"ecommerce","_id":"16"}}
{"order_id":"ORD-016","order_date":"2025-01-22T12:30:00Z","customer_name":"Pierre Moreau","customer_email":"pierre@example.com","country":"France","city":"Toulouse","category":"Books","product_name":"Distributed Systems Design","quantity":1,"unit_price":48.50,"total_price":48.50,"payment_method":"Credit Card","status":"delivered"}
{"index":{"_index":"ecommerce","_id":"17"}}
{"order_id":"ORD-017","order_date":"2025-01-23T10:00:00Z","customer_name":"Qin Wei","customer_email":"qin@example.com","country":"China","city":"Shanghai","category":"Clothing","product_name":"Silk Tie","quantity":2,"unit_price":38.00,"total_price":76.00,"payment_method":"Bank Transfer","status":"shipped"}
{"index":{"_index":"ecommerce","_id":"18"}}
{"order_id":"ORD-018","order_date":"2025-01-23T15:20:00Z","customer_name":"Rachel Green","customer_email":"rachel@example.com","country":"United States","city":"Chicago","category":"Home","product_name":"French Press Coffee Maker","quantity":1,"unit_price":34.99,"total_price":34.99,"payment_method":"PayPal","status":"delivered"}
{"index":{"_index":"ecommerce","_id":"19"}}
{"order_id":"ORD-019","order_date":"2025-01-24T09:30:00Z","customer_name":"Stefan Braun","customer_email":"stefan@example.com","country":"Germany","city":"Hamburg","category":"Electronics","product_name":"Noise Cancelling Earbuds","quantity":1,"unit_price":199.99,"total_price":199.99,"payment_method":"Credit Card","status":"delivered"}
{"index":{"_index":"ecommerce","_id":"20"}}
{"order_id":"ORD-020","order_date":"2025-01-24T14:00:00Z","customer_name":"Tanya Petrova","customer_email":"tanya@example.com","country":"Russia","city":"Moscow","category":"Books","product_name":"Apache Arrow Cookbook","quantity":2,"unit_price":41.00,"total_price":82.00,"payment_method":"Credit Card","status":"processing"}
'
echo ""

echo "==> Refreshing index..."
curl -sf -X POST "${ES_URL}/ecommerce/_refresh"
echo ""

echo "==> Verifying data load..."
COUNT=$(curl -sf "${ES_URL}/ecommerce/_count" | sed 's/.*"count":\([0-9]*\).*/\1/')
echo "==> Loaded ${COUNT} documents into 'ecommerce' index."
echo "==> E-commerce sample data ready."
