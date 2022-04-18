# Airbus client for Golang
This is the go-SDK for airbus for producing and consuming the messages.

### Features
1. Asynchronous Producer and Consumer
2. Add multiple consumers for a single topic
3. Support for user defined overrides for configs
4. Subscribe, unsubscribe producer or consumer topic
5. Pause and Play for consumer events
6. Plain text, JSON and AVRO validated schema
7. Listenable callbacks for user supported operations on success and error events after message is produced
8. Listenable callback for user after event is consumed with a returnable error to that event
9. Retry to event errors and in case of multiple fails send to ErrorQ for later analysis
10. StatsD updation of success and error message sent report

### How to integrate with your app
###### Using with GLIDE
Add this to your `glide.yaml`
```
- package: bitbucket.mynt.myntra.com/plt/airbus-go
  repo: git@bitbucket.mynt.myntra.com/plt:myntra/airbus-go
  version: <version>
  vcs: git
```
then run `glide up`

###### Using with GO get
Run this command on commandline to shift to `ssh` from `https` repo pulling
```
git config --global url."git@bitbucket.mynt.myntra.com/plt:".insteadOf "https://bitbucket.mynt.myntra.com/plt/"
```
then run
```
go get bitbucket.mynt.myntra.com/plt/airbus-go
```

### Example to run Producer
```go
type listenableCallback struct {}

func main() {
    // Initialise the logger and set logging level
    logger.LogInit(logging.ERROR)

    serviceUrl := "http://platformairbus.mjint.myntra.com"
    appName := "idea"
    clientCallback := listenableCallback{}
    config := make(map[string]interface{})
    config["batch.size"] = 5
    config["batch.timeout"] = 100       // 100 ms
    config["request.timeout"] = 10000       // 10000 ms

	prod, err := NewAirbusProducer(serviceUrl, appName, config, clientCallback, false)
	if err != nil {
		fmt.Println(err)
		return
	}

	headers := make(map[string]string)
    headers["X-Hello"] = "TestHeader"

	event := &entry.EventEntry{
		AppName: "idea",
		EventName: "SIGNUPEVENT",
		Data: "hello idea. This is a test event.",
		Headers: headers,
	}

	for i := 0; i < 10; i++ {
		if err := prod.AsyncSend(event); err != nil {
            fmt.Println(err.Error())
        }
	}
	prod.Close()
}

func (listenableCallback) OnSuccess(result *entry.Result) {
	fmt.Printf("Config: %+v\n", result)
}

func (listenableCallback) OnFailure(err error, msg *entry.Result) {
	fmt.Printf("Error: %s\n", err.Error())
}
```

### Example to run Consumer
```go
type eventListener struct{}
type eventListenerHeaders struct{}
type eventListenerComplete struct{}

func main() {
    // Initialise the logger and set logging level
    logger.LogInit(logging.ERROR)

	var eventListenerEntities []*entry.EventListenerEntity
    	eventListenerEntities = append(eventListenerEntities,
    	&entry.EventListenerEntity{
    		AirbusEvent: &entry.EventEntry{
    			AppName: "platform",
    			EventName: "acehigh",
    		},
    		AirbusEventListener: eventListener{},           // Only one of these two listeners is required, Depending on wether
    		AirbusHeadersEventListener: eventListenerHeaders{},        //  you are sending and receiving headers or not
    		AirbusCompleteEventListener: eventListenerComplete{},  // Use this one only if you want complete msg info
    		ConcurrentListeners: 10,
    		AutoCommitDisable:  true,
    	},
    	&entry.EventListenerEntity{
    		AirbusEvent: &entry.EventEntry{
    			AppName: "idea",
    			EventName: "SIGNUPEVENT",
    		},
    		AirbusEventListener: eventListener{},
    		ConcurrentListeners: 10,    // Concurrently Listening to a single consumer
    		AutoCommitDisable:  false,
    		ConsumerCount: 4,       // Multiple Consumers
    	})

	appName := "platform"
	serviceUrl := "http://platformairbus.mjint.myntra.com"

	config := make(map[string]interface{})
    config["auto.offset.reset"] = -1
    config["commit.interval"] = 5       // 5 sec

	shutdown, err := NewAirbusConsumer(appName, serviceUrl, config, eventListenerEntities)
	if err != nil {
		fmt.Println("Error:", err.Error())
		return
	}
    <-shutdown
}

func (this eventListener) OnEvent(key string, value interface{}) error {
	fmt.Printf("Value: %+v", value)
	return nil
}

func (this eventListenerHeaders) OnEvent(key string, value interface{}, headers map[string]string) error {
	fmt.Printf("Value: %+v, Headers: %+v\n", value, headers)
	return nil
}

func (this eventListenerComplete) OnEvent(key string, value interface{}, topic string, partition int32, offset int64, timestamp,
	blockTimestamp time.Time, headers map[string]string) error {
	fmt.Printf("Value: %+v, topic: %s\n", value, topic)
	return nil
}
```

### Points to keep in mind while using SDK
##### Plain Text Schema
* Send data as `string` type to producer
* Consumer will receive event as a `string` type

##### JSON Schema
* Either send data as `string` or `map[string]interface{}` type to producer
* Consumer will receive event as a byte array `[]byte{}`

##### AVRO Schema
* Send data as generic `interface{}` to producer, but corresponding schema should be built accordingly else it will fail
* Consumer will receive event as `map[string]interface{}` type

### Who do I talk to?
* Abhishek Jain - [abhishek.jain@myntra.com](Link URL)
* Sanjeev Kumar Dangi - [sanjeev.kumar11@myntra.com](Link URL)
* Amit kumar Gupta - [amit.kg@myntra.com](Link URL)
* Amulya Varshney - [amulya.varshney@myntra.com](Link URL)# airbus-go
