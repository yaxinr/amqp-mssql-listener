package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"strings"
	"text/template"
	"time"

	// "net/http"
	// _ "net/http/pprof"

	xj "github.com/basgys/goxml2json"
	_ "github.com/denisenkom/go-mssqldb"
	"github.com/streadway/amqp"
)

var (
	devPtr   = flag.Bool("dev", false, "a bool")
	amqpURI  = flag.String("amqp-uri", "amqp://guest:guest@localhost:5672/", "amqp://login:password@host:port/vhost")
	queuePtr = flag.String("queue", "mssql-subscribe-dev", "queue for subscribe requests")

	rabbitConn       *amqp.Connection
	rabbitCloseError chan *amqp.Error

	listenersFabric = ListenersFabric{
		listeners: make(map[string]*Listener, 0),
	}
)

func main() {
	flag.Parse()
	// go http.ListenAndServe("0.0.0.0:9088", nil)

	defer rabbitConn.Close()
	rabbitCloseError = make(chan *amqp.Error)
	forever := make(chan bool)
	go rabbitConnector(rabbitCloseError, rabbitConn, setup)
	rabbitCloseError <- amqp.ErrClosed
	<-forever
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func connectToRabbitMQ() *amqp.Connection {
	for {
		conn, err := amqp.Dial(*amqpURI)

		if err == nil {
			return conn
		}

		fmt.Println(err)
		fmt.Printf("Trying to reconnect to RabbitMQ at %s\n", *amqpURI)
		time.Sleep(2 * time.Second)
	}
}

func rabbitConnector(rabbitCloseError chan *amqp.Error, rabbitConn *amqp.Connection, fn func(conn *amqp.Connection, ch *amqp.Channel)) {
	var rabbitErr *amqp.Error

	for {
		rabbitErr = <-rabbitCloseError
		if rabbitErr != nil {
			fmt.Println(rabbitErr)
			fmt.Printf("\nConnecting to %s\n", *amqpURI)

			rabbitConn = connectToRabbitMQ()
			rabbitCloseError = make(chan *amqp.Error)
			rabbitConn.NotifyClose(rabbitCloseError)

			var err error
			ch, err := rabbitConn.Channel()
			failOnError(err, "Failed to open a channel")
			ch.NotifyClose(rabbitCloseError)

			err = ch.Qos(
				1,     // prefetch count
				0,     // prefetch size
				false, // global
			)
			failOnError(err, "Failed to set QoS")

			// run your setup process here
			// setup(rabbitConn, ch)
			fn(rabbitConn, ch)
		}
	}
}

func consumeQueue(ch *amqp.Channel, queue string) (<-chan amqp.Delivery, error) {
	q, err := ch.QueueDeclare(
		queue, // name
		true,  // durable
		false, // delete when usused
		false, // exclusive
		false, // noWait
		nil,   //
	)
	failOnError(err, "Failed to QueueDeclare")
	fmt.Println("RPC requests queue: ", q.Name)

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // orderIDs
	)
	failOnError(err, "Failed to register a consumer")
	return msgs, err
}

func setup(conn *amqp.Connection, ch *amqp.Channel) {
	rabbitConn = conn

	listenersFabric.load(ch)

	queue := *queuePtr
	msgs, _ := consumeQueue(ch, queue)
	go func() {
		for d := range msgs {
			err := listenersFabric.subscribe(d.Body, d.ReplyTo, ch)
			if err == nil {
				d.Ack(false)
			} else {
				d.Nack(false, false)
			}
		}
	}()
}

// ListenersFabric -
type ListenersFabric struct {
	listeners map[string]*Listener
}

func (listenersFabric *ListenersFabric) add(key string, listener *Listener) error {
	listenersFabric.listeners[key] = listener
	return nil
}

func (listenersFabric *ListenersFabric) save() error {
	listenersJSON, _ := json.MarshalIndent(listenersFabric.listeners, "", "\t")
	return ioutil.WriteFile("mssql-listeners.json", listenersJSON, os.ModePerm)
}

func (listenersFabric *ListenersFabric) load(ch *amqp.Channel) error {
	listenersJSON, err := ioutil.ReadFile("mssql-listeners.json")
	if err == nil {
		// var listeners map[string]*Listener
		err := json.Unmarshal(listenersJSON, &listenersFabric.listeners)
		if err != nil {
			return err
		}
		for _, listener := range listenersFabric.listeners {
			// err = listenersFabric.add(k, l)
			// if err == nil {
			if err := listener.start(ch); err != nil {
				fmt.Println(err)
			}
			// }
		}
	} else {
		return err
	}
	return nil
}

func urlParse(rawurl string) (exchangeName string, databaseName string) {
	URL, err := url.Parse(rawurl)
	failOnError(err, "url.Parse(*connectionString)")

	database := URL.Query().Get("database")
	return fmt.Sprintf("%s-%s-%s-%s", URL.Host, URL.Port(), URL.Path, database), database
}

func (listenersFabric *ListenersFabric) subscribe(body []byte, replyTo string, ch *amqp.Channel) error {
	var req Listener
	err := json.Unmarshal(body, &req)
	if err != nil {
		return err
	}
	// log.Printf(string(d.Body))
	if len(req.ConnectionString) == 0 {
		return errors.New("ConnectionString is empty")
	}
	exchangeName, _ := urlParse(req.ConnectionString)
	key := fmt.Sprintf("%s-%s-%s", exchangeName, req.TableName, req.Identity)
	l, ok := listenersFabric.listeners[key]
	l = &req
	l.replyTo = replyTo
	if !ok {
		if err := listenersFabric.add(key, l); err != nil {
			return err
		}
	}
	listenersFabric.listeners[key] = l
	listenersFabric.save()
	if err := l.start(ch); err != nil {
		fmt.Println(err)
	}
	return nil
}

// Listener ...
type Listener struct {
	db                                       *sql.DB
	running                                  bool
	replyTo                                  string
	ConnectionString                         string
	exchangeName                             string
	DatabaseName                             string `json:"-"`
	SchemaName                               string
	TableName                                string
	TriggerType                              string
	Select                                   string
	SelectScript                             string
	SelectIsEmpty                            int
	IfUpdate                                 string
	Fields                                   []string
	Join                                     string
	Where                                    string
	DetailsIncluded                          bool
	Detailed                                 string `json:"-"`
	Identity                                 string
	ConversationQueueName                    string `json:"-"`
	QuotedJoin                               string `json:"-"`
	QuotedWhere                              string `json:"-"`
	ConversationServiceName                  string `json:"-"`
	ConversationTriggerName                  string `json:"-"`
	InstallListenerProcedureName             string `json:"-"`
	UninstallListenerProcedureName           string `json:"-"`
	InstallServiceBrokerNotificationScript   string `json:"-"`
	InstallNotificationTriggerScript         string `json:"-"`
	UninstallNotificationTriggerScript       string `json:"-"`
	UninstallServiceBrokerNotificationScript string `json:"-"`
}

// ExecuteNonQuery ...
func (listener Listener) ExecuteNonQuery(commandText string) error {
	_, err := listener.db.Exec(commandText)
	// if *devPtr {
	// 	fmt.Println(commandText)
	// }
	if err != nil {
		fmt.Println(err)
		fmt.Println(commandText)
	}
	return err
}

func (listener Listener) routingKey() string {
	return fmt.Sprintf("%s_%s", listener.TableName, listener.Identity)
}

var emptyJSON = []byte("{}")

func x2j(xmlText string, detailed bool) []byte {
	if detailed {
		json, err := xj.Convert(strings.NewReader(xmlText))
		if err != nil {
			// panic("That's embarrassing...")
			return []byte(fmt.Sprintf("{error: '%s'}", err.Error()))
		}
		return json.Bytes()
	}
	return emptyJSON
}

var updatedFieldSQL = func(fieldName string) string {
	return "UPDATE(" + fieldName + ")"
}

func mapStrSlice(vs []string, f func(string) string) []string {
	vsm := make([]string, len(vs))
	for i, v := range vs {
		vsm[i] = f(v)
	}
	return vsm
}

func (listener Listener) start(ch *amqp.Channel) error {
	var err error
	if listener.db, err = sql.Open("sqlserver", listener.ConnectionString); err != nil {
		return err
	}
	if err = listener.db.Ping(); err != nil {
		return err
	}

	listener.exchangeName, listener.DatabaseName = urlParse(listener.ConnectionString)
	if listener.SchemaName == "" {
		listener.SchemaName = "dbo"
	}
	suffix := listener.routingKey()
	if len(listener.IfUpdate) == 0 && len(listener.Fields) > 0 {
		listener.IfUpdate = strings.Join(mapStrSlice(listener.Fields, updatedFieldSQL), " OR ")
	}
	listener.ConversationQueueName = "ListenerQueue_" + suffix
	listener.ConversationServiceName = "ListenerService_" + suffix
	listener.ConversationTriggerName = "tr_Listener_" + suffix
	listener.InstallListenerProcedureName = "sp_InstallListenerNotification_" + suffix
	listener.UninstallListenerProcedureName = "sp_UninstallListenerNotification_" + suffix
	listener.QuotedJoin = strings.Replace(listener.Join, "'", "''''", -1)
	listener.QuotedWhere = strings.Replace(listener.Where, "'", "''''", -1)

	execInstallationProcedureScript := getScript("execInstallationProcedureScript", `
		USE [{{.DatabaseName}}]
		IF OBJECT_ID ('{{.SchemaName}}.{{.InstallListenerProcedureName}}', 'P') IS NOT NULL
			EXEC {{.SchemaName}}.{{.InstallListenerProcedureName}}
		`, listener)
	if err = listener.ExecuteNonQuery(listener.getInstallNotificationProcedureScript()); err != nil {
		return err
	}
	if err = listener.ExecuteNonQuery(listener.getUninstallNotificationProcedureScript()); err != nil {
		return err
	}
	if err = listener.ExecuteNonQuery(execInstallationProcedureScript); err != nil {
		return err
	}
	err = ch.ExchangeDeclare(
		listener.exchangeName, // name
		"topic",               // type
		true,                  // durable
		false,                 // auto-deleted
		false,                 // internal
		false,                 // no-wait
		nil,                   // arguments
	)
	if err != nil {
		return err
	}
	if listener.replyTo != "" {
		tmpChannel, err := rabbitConn.Channel()
		defer tmpChannel.Close()
		failOnError(err, "tmpChannel")
		err = tmpChannel.QueueBind(
			listener.replyTo,      // queue name
			listener.routingKey(), // routing key
			listener.exchangeName, // exchange
			false,
			nil)
		if err != nil {
			fmt.Println("tmpChannel.QueueBind: ", err.Error())
		}
	}
	if !listener.running {
		go func() {
			listener.running = true
			// COMMAND_TIMEOUT := 60000
			var commandText = getScript("", `
			DECLARE @ConvHandle UNIQUEIDENTIFIER
			DECLARE @message VARBINARY(MAX)
			USE [{{.DatabaseName}}]
			WAITFOR (RECEIVE TOP(1) @ConvHandle=Conversation_Handle
						, @message=message_body FROM {{.SchemaName}}.[{{.ConversationQueueName}}]), TIMEOUT 30000;
			BEGIN TRY END CONVERSATION @ConvHandle; END TRY BEGIN CATCH END CATCH
	
			SELECT isnull(CAST(@message AS NVARCHAR(MAX)), '')
	`, listener)
			defer listener.stop()
			var (
				s          string
				routingKey = listener.routingKey()
				msg        = amqp.Publishing{}
			)
			stmt, err := listener.db.Prepare(commandText)
			failOnError(err, "db.Prepare: "+commandText)
			if *devPtr {
				fmt.Println(commandText)
			}
			for {
				err = stmt.QueryRow().Scan(&s)
				if err != nil {
					if strings.Contains(err.Error(), "i/o timeout") {
						continue
					}
					failOnError(err, "db.Query: "+commandText)
				}
				if len(s) > 0 {
					if *devPtr {
						fmt.Println(s)
					}
					msg.Body = x2j(s, listener.DetailsIncluded)
					err = ch.Publish(
						listener.exchangeName, // exchange
						routingKey,            // routing key
						false,                 // mandatory
						false,                 // immediate
						msg)
					if err != nil {
						log.Fatalf("%s: %s", "ch.Publish", err)
						panic(fmt.Sprintf("%s: %s", "ch.Publish", err))
					}
				}
			}
		}()
	}
	fmt.Println(suffix + " started")
	return nil
}

func (listener Listener) stop() {
	listener.running = false
	// UninstallNotification();
	execUninstallationProcedureScript := getScript("execUninstallationProcedureScript", `
		USE [{{.DatabaseName}}]
		IF OBJECT_ID ('{{.SchemaName}}.{{.UninstallListenerProcedureName}}', 'P') IS NOT NULL
			EXEC {{.SchemaName}}.{{.UninstallListenerProcedureName}}
	`, listener)
	listener.ExecuteNonQuery(execUninstallationProcedureScript)
	defer listener.db.Close()

	// lock (ActiveEntities)
	//     if (ActiveEntities.Contains(Identity)) ActiveEntities.Remove(Identity);

	// if ((_threadSource == null) || (_threadSource.Token.IsCancellationRequested))
	// {
	//     return;
	// }

	// if (!_threadSource.Token.CanBeCanceled)
	// {
	//     return;
	// }

	// _threadSource.Cancel();
	// _threadSource.Dispose();
}

func (listener Listener) getTriggerTypeByListenerType() string {
	return "INSERT, UPDATE, DELETE"
	// StringBuilder result = new StringBuilder();
	// if (this.NotificaionTypes.HasFlag(NotificationTypes.Insert))
	// 	result.Append("INSERT");
	// if (this.NotificaionTypes.HasFlag(NotificationTypes.Update))
	// 	result.Append(result.Length == 0 ? "UPDATE" : ", UPDATE");
	// if (this.NotificaionTypes.HasFlag(NotificationTypes.Delete))
	// 	result.Append(result.Length == 0 ? "DELETE" : ", DELETE");
	// if (result.Length == 0) result.Append("INSERT");

	// return result.ToString();
}

func getScript(name string, text string, data interface{}) string {
	tmplt, err := template.New(name).Parse(text)
	failOnError(err, "template.New(name).Parse")
	buf := &bytes.Buffer{}
	err = tmplt.Execute(buf, data)
	failOnError(err, "tmplt.Execute")
	// fmt.Println(buf.String())
	return buf.String()
}

// #region Scripts

// #region Procedures

const sqlPermissionsInfo = `
			DECLARE @msg VARCHAR(MAX)
			DECLARE @crlf CHAR(1)
			SET @crlf = CHAR(10)
			SET @msg = 'Current user must have following permissions: '
			SET @msg = @msg + '[CREATE PROCEDURE, CREATE SERVICE, CREATE QUEUE, SUBSCRIBE QUERY NOTIFICATIONS, CONTROL, REFERENCES] '
			SET @msg = @msg + 'that are required to start query notifications. '
			SET @msg = @msg + 'Grant described permissions with following script: ' + @crlf
			SET @msg = @msg + 'GRANT CREATE PROCEDURE TO [<username>];' + @crlf
			SET @msg = @msg + 'GRANT CREATE SERVICE TO [<username>];' + @crlf
			SET @msg = @msg + 'GRANT CREATE QUEUE  TO [<username>];' + @crlf
			SET @msg = @msg + 'GRANT REFERENCES ON CONTRACT::[DEFAULT] TO [<username>];' + @crlf
			SET @msg = @msg + 'GRANT SUBSCRIBE QUERY NOTIFICATIONS TO [<username>];' + @crlf
			SET @msg = @msg + 'GRANT CONTROL ON SCHEMA::[<schemaname>] TO [<username>];'

			PRINT @msg
`

/// SQL_FORMAT_CREATE_INSTALLATION_PROCEDURE - T-SQL script-template which creates notification setup procedure.
/// {0} - database name.
/// {1} - setup procedure name.
/// {2} - service broker configuration statement.
/// {3} - notification trigger configuration statement.
/// {4} - notification trigger check statement.
/// {5} - table name.
/// {6} - schema name.
// installationProcedureScript := fmt.Sprintf(
// 	this.DatabaseName,
// 	this.InstallListenerProcedureName,
// 	strings.Replace(installServiceBrokerNotificationScript, "'", "''", -1),
// 	strings.Replace(installNotificationTriggerScript, "'", "''''", -1),
// 	strings.Replace(uninstallNotificationTriggerScript, "'", "''", -1),
// 	this.TableName,
// 	this.SchemaName)
const SQL_FORMAT_CREATE_INSTALLATION_PROCEDURE = `
		USE [{{.DatabaseName}}]
		` + sqlPermissionsInfo + `
		IF OBJECT_ID ('{{.SchemaName}}.{{.InstallListenerProcedureName}}', 'P') IS NOT NULL
			DROP PROCEDURE {{.SchemaName}}.{{.InstallListenerProcedureName}}
		BEGIN
			EXEC ('
				CREATE PROCEDURE {{.SchemaName}}.{{.InstallListenerProcedureName}}
				AS
				BEGIN
					-- Service Broker configuration statement
					{{.InstallServiceBrokerNotificationScript}}

					-- Notification Trigger configuration statement
					DECLARE @triggerStatement NVARCHAR(MAX)
					DECLARE @select NVARCHAR(MAX)
					DECLARE @sqlInserted NVARCHAR(MAX)
					DECLARE @sqlDeleted NVARCHAR(MAX)

          EXEC sp_executesql N''DROP FUNCTION [dbo].[spaceLess]''

          EXEC sp_executesql N''CREATE FUNCTION dbo.spaceLess(@p1 varchar(max)) RETURNS varchar(max) AS
          BEGIN
            RETURN upper(replace(replace(replace(replace(@p1,space(1),space(0)),char(9),space(0)),char(10),space(0)),char(13),space(0)))
          END''
          
          declare @triggerID int = OBJECT_ID (''{{.SchemaName}}.{{.ConversationTriggerName}}'', ''TR'')	
          declare @oldTriggerDef nvarchar(max)= (SELECT dbo.spaceLess(OBJECT_DEFINITION (@triggerID)) );  

          SET @triggerStatement = N''{{.InstallNotificationTriggerScript}}''

					IF {{.SelectIsEmpty}}=0
						SET @select = STUFF((SELECT '','' + ''['' + COLUMN_NAME + '']''
										 FROM INFORMATION_SCHEMA.COLUMNS
										 WHERE DATA_TYPE NOT IN  (''text'',''ntext'',''image'',''geometry'',''geography'') 
										 AND TABLE_SCHEMA = ''{{.SchemaName}}'' AND TABLE_NAME = ''{{.TableName}}'' AND TABLE_CATALOG = ''{{.DatabaseName}}''
										 FOR XML PATH ('''')
										 ), 1, 1, '''')
					ELSE SET @select = N''{{.SelectScript}}''

					declare @sqlXML NVARCHAR(MAX) = N''SET @retvalOUT = (SELECT '' + @select + N''
											 FROM @t {{.QuotedJoin}}
											 {{if .Where}}WHERE {{.QuotedWhere}}{{end}} FOR XML PATH(''''row''''), ROOT (''''@t''''))''

					SET @sqlInserted = REPLACE(@sqlXML, N''@t'', N''inserted'')
					SET @sqlDeleted = REPLACE(@sqlXML, N''@t'', N''deleted'')
		
					SET @triggerStatement = REPLACE(@triggerStatement
											 , ''%inserted_select_statement%'', @sqlInserted)
					SET @triggerStatement = REPLACE(@triggerStatement
											 , ''%deleted_select_statement%'', @sqlDeleted)

          IF @triggerID IS NOT NULL
          BEGIN
            if @oldTriggerDef = dbo.spaceLess(@triggerStatement)
              RETURN;
            DROP TRIGGER {{.SchemaName}}.{{.ConversationTriggerName}};
          END
          EXEC sp_executesql @triggerStatement
				END
				')
		END
`

// #endregion

// #region ServiceBroker notification
/// T-SQL script-template which prepares database for ServiceBroker notification.
/// {0} - database name;
/// {1} - conversation queue name.
/// {2} - conversation service name.
/// {3} - schema name.
//-- Setup Service Broker
//IF EXISTS (SELECT * FROM sys.databases
//                    WHERE name = '{0}' AND (is_broker_enabled = 0 OR is_trustworthy_on = 0))
//BEGIN

//    ALTER DATABASE [{0}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
//    ALTER DATABASE [{0}] SET ENABLE_BROKER;
//    ALTER DATABASE [{0}] SET MULTI_USER WITH ROLLBACK IMMEDIATE

//    -- FOR SQL Express
//    ALTER AUTHORIZATION ON DATABASE::[{0}] TO [sa]
//    ALTER DATABASE [{0}] SET TRUSTWORTHY ON;

//END

// #endregion

// #region Notification Trigger

/// T-SQL script-template which creates notification trigger.
/// {0} - monitorable table name.
/// {1} - notification trigger name.
/// {2} - event data (INSERT, DELETE, UPDATE...).
/// {3} - conversation service name.
/// {4} - detailed changes tracking mode.
/// {5} - schema name.
/// %inserted_select_statement% - sql code which sets trigger "inserted" value to @retvalOUT variable.
/// %deleted_select_statement% - sql code which sets trigger "deleted" value to @retvalOUT variable.

// this.TableName,
// this.ConversationTriggerName,
// this.getTriggerTypeByListenerType(),
// this.ConversationServiceName,
// detailed,
// this.SchemaName)
const SQL_FORMAT_CREATE_NOTIFICATION_TRIGGER = `
	CREATE TRIGGER [{{.ConversationTriggerName}}]
	ON {{.SchemaName}}.[{{.TableName}}]
	AFTER {{.TriggerType}}
	AS
	{{if .IfUpdate}}IF {{.IfUpdate}}{{end}}
	BEGIN
		SET NOCOUNT ON;

		--Trigger {{.TableName}} is rising...
		IF EXISTS (SELECT * FROM sys.services WHERE name = '{{.ConversationServiceName}}')
		BEGIN
			DECLARE @message NVARCHAR(MAX) = ''
			{{if .DetailsIncluded}}
			SET @message = N'<root/>'
			DECLARE @retvalOUT NVARCHAR(MAX)

			%inserted_select_statement%

			IF (@retvalOUT IS NOT NULL)
				SET @message = N'<root>' + @retvalOUT

			%deleted_select_statement%

			IF (@retvalOUT IS NOT NULL)
				IF (@message = N'<root/>') SET @message = N'<root>' + @retvalOUT
				ELSE SET @message = @message + @retvalOUT

			IF (@message != N'<root/>')
				SET @message = @message + N'</root>'
			ELSE RETURN
			{{end}}
			--Beginning of dialog...
			DECLARE @ConvHandle UNIQUEIDENTIFIER
			--Determine the Initiator Service, Target Service and the Contract
			BEGIN DIALOG @ConvHandle
				FROM SERVICE [{{.ConversationServiceName}}] TO SERVICE '{{.ConversationServiceName}}' ON CONTRACT [DEFAULT] WITH ENCRYPTION=OFF, LIFETIME = 60;

			SEND ON CONVERSATION @ConvHandle MESSAGE TYPE [DEFAULT] (@message);

			END CONVERSATION @ConvHandle;
		END
	END
`

// #endregion

/// T-SQL script-template which returns all dependency identities in the database.
/// {0} - database name.
const SQL_FORMAT_GET_DEPENDENCY_IDENTITIES = `
		USE [{0}]

		SELECT REPLACE(name , 'ListenerService_' , '')
		FROM sys.services
		WHERE [name] like 'ListenerService_%';
`

// #endregion

// #region Forced cleaning of database

/// <summary>
/// T-SQL script-template which cleans database from notifications.
/// {0} - database name.
/// </summary>
const SQL_FORMAT_FORCED_DATABASE_CLEANING = `
		USE [{0}]

		DECLARE @db_name VARCHAR(MAX)
		SET @db_name = '{0}' -- provide your own db name

		DECLARE @proc_name VARCHAR(MAX)
		DECLARE procedures CURSOR
		FOR
		SELECT   sys.schemas.name + '.' + sys.objects.name
		FROM    sys.objects
		INNER JOIN sys.schemas ON sys.objects.schema_id = sys.schemas.schema_id
		WHERE sys.objects.[type] = 'P' AND sys.objects.[name] like 'sp_UninstallListenerNotification_%'

		OPEN procedures;
		FETCH NEXT FROM procedures INTO @proc_name

		WHILE (@@FETCH_STATUS = 0)
		BEGIN
		EXEC ('USE [' + @db_name + '] EXEC ' + @proc_name + ' IF (OBJECT_ID ('''
						+ @proc_name + ''', ''P'') IS NOT NULL) DROP PROCEDURE '
						+ @proc_name)

		FETCH NEXT FROM procedures INTO @proc_name
		END

		CLOSE procedures;
		DEALLOCATE procedures;

		DECLARE procedures CURSOR
		FOR
		SELECT   sys.schemas.name + '.' + sys.objects.name
		FROM    sys.objects
		INNER JOIN sys.schemas ON sys.objects.schema_id = sys.schemas.schema_id
		WHERE sys.objects.[type] = 'P' AND sys.objects.[name] like 'sp_InstallListenerNotification_%'

		OPEN procedures;
		FETCH NEXT FROM procedures INTO @proc_name

		WHILE (@@FETCH_STATUS = 0)
		BEGIN
		EXEC ('USE [' + @db_name + '] DROP PROCEDURE '
						+ @proc_name)

		FETCH NEXT FROM procedures INTO @proc_name
		END

		CLOSE procedures;
		DEALLOCATE procedures;
`

// #endregion
// #endregion

func (listener Listener) getInstallNotificationProcedureScript() string {
	s := getScript("installServiceBrokerNotificationScript", `
		-- Create a queue which will hold the tracked information
		IF NOT EXISTS (SELECT * FROM sys.service_queues WHERE name = '{{.ConversationQueueName}}')
			CREATE QUEUE {{.SchemaName}}.[{{.ConversationQueueName}}]
		-- Create a service on which tracked information will be sent
		IF NOT EXISTS(SELECT * FROM sys.services WHERE name = '{{.ConversationServiceName}}')
			CREATE SERVICE [{{.ConversationServiceName}}] ON QUEUE {{.SchemaName}}.[{{.ConversationQueueName}}] ([DEFAULT])
	`, listener)
	listener.InstallServiceBrokerNotificationScript = strings.Replace(s, "'", "''", -1)

	listener.Detailed = "NOT"
	if listener.DetailsIncluded {
		listener.Detailed = ""
	}
	listener.TriggerType = "INSERT, UPDATE, DELETE"
	s = getScript("installNotificationTriggerScript", SQL_FORMAT_CREATE_NOTIFICATION_TRIGGER, listener)
	listener.InstallNotificationTriggerScript = strings.Replace(s, "'", "''''", -1)

	s = getScript("uninstallNotificationTriggerScript", `
		IF OBJECT_ID ('{{.SchemaName}}.{{.ConversationTriggerName}}', 'TR') IS NOT NULL
			  DROP TRIGGER {{.SchemaName}}.{{.ConversationTriggerName}};
		--	RETURN;
`, listener)
	listener.UninstallNotificationTriggerScript = strings.Replace(s, "'", "''", -1)
	listener.SelectIsEmpty = len(listener.Select)
	listener.SelectScript = strings.Replace(listener.Select, "'", "''''", -1)

	return getScript("installationProcedureScript", SQL_FORMAT_CREATE_INSTALLATION_PROCEDURE, listener)
}

func (listener Listener) getUninstallNotificationProcedureScript() string {
	listener.UninstallServiceBrokerNotificationScript = getScript("uninstallServiceBrokerNotificationScript", `
DECLARE @serviceId INT
SELECT @serviceId = service_id FROM sys.services
WHERE sys.services.name = '{{.ConversationServiceName}}'

DECLARE @ConvHandle uniqueidentifier
DECLARE Conv CURSOR FOR
SELECT CEP.conversation_handle FROM sys.conversation_endpoints CEP
WHERE CEP.service_id = @serviceId AND ([state] != 'CD' OR [lifetime] > GETDATE() + 1)

OPEN Conv;
FETCH NEXT FROM Conv INTO @ConvHandle;
WHILE (@@FETCH_STATUS = 0) BEGIN
	END CONVERSATION @ConvHandle WITH CLEANUP;
	FETCH NEXT FROM Conv INTO @ConvHandle;
END
CLOSE Conv;
DEALLOCATE Conv;

-- Droping service and queue.
IF  EXISTS (SELECT * FROM sys.services WHERE name = N'{{.ConversationServiceName}}')
	DROP SERVICE [{{.ConversationServiceName}}];
IF OBJECT_ID ('{{.SchemaName}}.{{.ConversationQueueName}}', 'SQ') IS NOT NULL
	DROP QUEUE {{.SchemaName}}.[{{.ConversationQueueName}}];
`, listener)
	listener.UninstallNotificationTriggerScript = getScript("uninstallNotificationTriggerScript", `
				IF OBJECT_ID ('{{.SchemaName}}.{{.ConversationTriggerName}}', 'TR') IS NOT NULL
					DROP TRIGGER {{.SchemaName}}.[{{.ConversationTriggerName}}];
				`, listener)
	listener.UninstallServiceBrokerNotificationScript = strings.Replace(listener.UninstallServiceBrokerNotificationScript, "'", "''", -1)
	listener.UninstallNotificationTriggerScript = strings.Replace(listener.UninstallNotificationTriggerScript, "'", "''", -1)
	return getScript("uninstallationProcedureScript", `
		USE [{{.DatabaseName}}]
		`+sqlPermissionsInfo+`
		IF OBJECT_ID ('{{.SchemaName}}.{{.UninstallListenerProcedureName}}', 'P') IS NULL
		BEGIN
			EXEC ('
				CREATE PROCEDURE {{.SchemaName}}.{{.UninstallListenerProcedureName}}
				AS
				BEGIN
					-- Notification Trigger drop statement.
					{{.UninstallNotificationTriggerScript}}

					-- Service Broker uninstall statement.
					{{.UninstallServiceBrokerNotificationScript}}

					IF OBJECT_ID (''{{.SchemaName}}.{{.InstallListenerProcedureName}}'', ''P'') IS NOT NULL
						DROP PROCEDURE {{.SchemaName}}.{{.InstallListenerProcedureName}}

					DROP PROCEDURE {{.SchemaName}}.{{.UninstallListenerProcedureName}}
				END
				')
		END
`, listener)
}
