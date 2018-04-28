curl -XDELETE http://localhost:9200/messageid
curl -H "Content-Type: application/json" -XPUT http://localhost:9200/messageid -d @elasticsearch-index.json