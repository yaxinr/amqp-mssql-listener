# Subscribtion - publication by amqp from mssql to anywere
* golang realization of [ServiceBrokerListener](https://github.com/dyatchenko/ServiceBrokerListener)
* crossplatform and other pros of golang
## Instructions
* Run 
```bash
go run main.go -amqp-uri=amqp://guest:guest@localhost:5672/ -queue=mssql-subscribe
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
  "ConnectionString": "sqlserver://username:password@host:port/instance?database=<Database>",
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

or
create file mssql-listeners.json
```json
{
    "test": {
        "ConnectionString": "sqlserver://username:password@host:port/instance?database=<Database>",
        "SchemaName": "dbo",
        "TableName"       : "<Table>",
        "Identity": "field1_eq_A",
        "Select": "field1",
        "IfUpdate": "update(field1) or update(field2)",
        "Where": "field1='A'",
        "DetailsIncluded": true
    }
}
```
and bind to exchange "URL.Host-URL.Port-URL.Path-database" with routingKey "TableName-Identity"
(URL=ConnectionString)