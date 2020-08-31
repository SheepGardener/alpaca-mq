# Alpaca-mq

- High availability based on kafka
- Support flow control, overload protection
- Quickly complete the construction of multiple applications and multiple services
- Support message delay queue
- Point-to-point consumption based on command points

Message queue for service decoupling, You can use it to decouple services, but it is worth noting that you need to ensure that your services are idempotent.

# Installation

Install alpaca-mq using the "go get" command:

- go get github.com/SheepGardener/alpaca-mq

# Puller

The pusher of the message, produces and builds the message, and completes the push of the message,
Puller startup is very simple, you can start it quickly, of course, you need to configure puller first
```
	package main

	import (
		alpaca "github.com/SheepGardener/alpaca-mq"
	)

	func main() {
		puller := alpaca.InitPuller("./log/puller.log","./config/puller.yml", "./config/apps/")
		puller.Pull()
	}
```

# Pusher

The consumer of the message, passing the message to the service application,Here is only a simple example, in fact, you can customize a pusher service more flexibly

```

	package main

	import (
		"encoding/json"
		"fmt"
		"net/http"

		alpaca "github.com/SheepGardener/alpaca-mq"
	)

	type Response struct {
		Errno  int8   `json:"errno"`
		Errmsg string `json:"errmsg"`
		Logid  string `json:"log_id"`
	}

	var Pusher *alpaca.Pusher

	func init() {
		Pusher = alpaca.InitPusher("./log/pusher.log","./config/pusher.yml")
	}

	func sendMsg(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		resp := Response{}
		resp.Errno = 0
		resp.Errmsg = "success"

		Logid := r.Form.Get("logid")
		Cmd := r.Form.Get("cmd")
		Hashkey := r.Form.Get("hash_key")
		Data := r.Form.Get("data")

		if Logid == "" {
			Logid = "test" //alpaca.GetLogId()
		}

		if Cmd == "" {
			w.Write([]byte("{\"errno\":-1,\"errmsg\":\"Command cannot be empty\"}"))
			return
		}

		resp.Logid = Logid

		kmsg := &alpaca.Kmessage{
			Cmd:     Cmd,
			Data:    Data,
			LogId:   Logid,
			HashKey: Hashkey,
			Delay: 12,
		}

		err := Pusher.Push(kmsg)

		if err != nil {
			resp.Errno = -1
			resp.Errmsg = fmt.Sprintf("%s", err)
		}

		respJson, err := json.Marshal(resp)

		if err != nil {
			w.Write([]byte("{\"errno\":-1,\"errmsg\":\"ResponData json marchal failed\"}"))
			return
		}
		w.Write(respJson)
	}

	func main() {

		http.HandleFunc("/sendmsg", sendMsg)
		http.ListenAndServe(":8009", nil)
	}
```

# Kmessage
The message is transmitted in the form of alpaca.Kmessage

```
  	alpaca.Kmessage{
		Cmd:     Cmd, // Command point
		Data:    Data, // transfer data
		LogId:   Logid, // Log ID
		HashKey: Hashkey, // The same hashkey will be assigned to the same queue for sequential processing
		Delay: 12, //Message delay time
	}

```

# Personalise
Alpace implements message processing and service load balancing strategy by default. If you want to customize your own message processing and load balancing strategy, you can implement it in the following custom ways

```
	// Custom message processing implementation
	type customizeHandleMessage struct {
		...Implement your own logic
	}

	func (c *customizeHandleMessage) MessageHandle(url stirng, msg *alpaca.Kmessage) {
		...Implement your own logic
	}

	//Register message handler
	handlemsg := &customizeHandleMessage{}
	pull.SetMessageHandle(handlemsg)

	**********************************************************************************


	// Implement a custom service load balancing strategy
	type customizeServerSelector struct {
		...Implement your own logic
	}

	func (c *customizeServerSelector) GetAppUrl(ap App) {
			...Implement your own logic
	}

	//Register service load balancing selector
	selector := &customizeServerSelector{}
	pull.SetServerSelect(selector)

```

# Notes

- Service request succeeded, The condition for the puller request service to be successful is that the requested service needs to return 200 and the return parameter exists errno and is 0
- Puller deployment recommendations. If you want to deploy multiple pullers, it is recommended that you initialize the number of kafka partitions equal to the number of pullers. If you can, please ensure that one puller only handles one partition, which will make full use of the performance advantages of alpaca-mq
- Puller and pusher will only handle one topic, so don’t expect it to handle multiple topics. If you want to implement multiple topics, it is recommended that you divide different topics according to different businesses.
- In order to ensure the entire message link, please be sure to carry the [LogId](), which will bring great benefits, not just limited to the location of the problem
- Of course, you have more suggestions and ideas, you can contact me

# More

If you want to know more how to use it, you can refer to the [examples](https://github.com/SheepGardener/alpaca-mq/tree/master/examples), it can provide you with more help

- [Puller configuration](https://github.com/SheepGardener/alpaca-mq/blob/master/examples/config/puller.yml)
- [Pusher configuration](https://github.com/SheepGardener/alpaca-mq/blob/master/examples/config/pusher.yml)
- [Service application configuration](https://github.com/SheepGardener/alpaca-mq/blob/master/examples/config/apps/test-app.yml)