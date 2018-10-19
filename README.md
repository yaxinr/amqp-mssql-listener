# Subscribtion - publication by amqp from mssql to anywere
* golang realization of [ServiceBrokerListener](https://github.com/dyatchenko/ServiceBrokerListener)
* crossplatform and other pros of golang
## Instructions
* Run 
```bash
go run main.go -amqp-uri="amqp://guest:guest@localhost:5672/"
```
* Create and subscribe to queue "REPLYTO-QUEUE"
* Publish message to queue "mssql-subscribe"
```go
{
  reply_to: "REPLYTO-QUEUE"
  body: JSON
}
```
JSON =
```json
{
  "ConnectionString": "sqlserver://sqlserver://username:password@host:port/instance?database=<Database>",
  "TableName"       : "<Table>",
  "Identity": "a_eq_1",
  "Select": "a",
  "IfUpdate": "update(a) or update(b)",
  "Where": "a='1'",
  "DetailsIncluded": true
}
```
* Change data in Database Table
* Wait for messages in queue "REPLYTO-QUEUE"