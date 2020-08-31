package alpaca

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
)

// Message core interface processing,
// through which the final consumption of messages, business logic,
// service address and message body Kmessage will be accepted
type MessageHandle interface {
	HandleMessage(string, *Kmessage) error
}

type MsgHandle struct {
	logger *Logger
}

// Message processing, if the returned status is not equal to 200 or the returned errno is not equal to 0,
// it will be marked as a message processing failure
func NewMsgHandle(klogger *Logger) *MsgHandle {
	return &MsgHandle{
		logger: klogger,
	}
}

func (m *MsgHandle) HandleMessage(url string, kmsg *Kmessage) error {

	bytedate := stringToByte(kmsg.Data)

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(bytedate))

	if err != nil {
		m.logger.WithFields(Fields{"method": "POST", "url": url, "data": kmsg.Data, "logId": kmsg.LogId}).Warnf("Init Request Failed Err:%s", err)
		return errors.New("Request Error")
	}

	client := &http.Client{}

	resp, err := client.Do(req)

	defer resp.Body.Close()

	if err != nil {
		m.logger.WithFields(Fields{"method": "POST", "url": url, "data": kmsg.Data, "logId": kmsg.LogId}).Warn("Request Error")
		return errors.New("Request Error")
	}

	statuscode := resp.StatusCode

	if statuscode != 200 {
		m.logger.WithFields(Fields{"method": "POST", "url": url, "data": kmsg.Data, "http_status": statuscode, "logId": kmsg.LogId}).Warn("Request Http_status Not 200")
		return errors.New("Request Error")
	}

	respBody, readErr := ioutil.ReadAll(resp.Body)

	if readErr != nil {
		m.logger.WithFields(Fields{"method": "POST", "url": url, "data": kmsg.Data, "http_status": statuscode, "logId": kmsg.LogId}).Warnf("Read Response Data Failed Err:%s", readErr)
		return errors.New("Request Error")
	}

	var response map[string]interface{}

	err1 := json.Unmarshal(respBody, &response)

	if err1 != nil {
		m.logger.WithFields(Fields{"method": "POST", "url": url, "data": kmsg.Data, "http_status": statuscode, "response": byteToString(respBody), "logId": kmsg.LogId}).Warnf("Response Decode Failed Err:%s", err1)
		return err1
	}

	errno, _ := response["errno"].(int)

	if errno != 0 {
		m.logger.WithFields(Fields{"method": "POST", "url": url, "data": kmsg.Data, "http_status": statuscode, "response": byteToString(respBody), "logId": kmsg.LogId}).Warn("Request Error Errno Not 0")
		return errors.New("Request failed!")
	}

	m.logger.WithFields(Fields{"method": "POST", "url": url, "data": kmsg.Data, "http_status": statuscode, "response": byteToString(respBody), "logId": kmsg.LogId}).Info("Request Success")
	return nil
}
