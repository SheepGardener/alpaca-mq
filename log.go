package alpaca

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type Fields map[string]interface{}

type Logger struct {
	lger          *logrus.Logger
	lf            *os.File
	seq           int
	lfp           string
	lastTimeStamp int64
	lock          sync.Mutex
}

func NewLogger(logFilePath string) *Logger {
	return &Logger{
		lger:          logrus.New(),
		lastTimeStamp: time.Now().UnixNano() / 1000000,
		lfp:           logFilePath,
		seq:           0,
	}
}

func (l *Logger) Init(timeForm time.Duration) {
	l.lger.SetFormatter(&logrus.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
		FullTimestamp:   true,
	})

	l.openLogFile()

	l.logSegmentation(timeForm)
}

func (l *Logger) WithFields(fields map[string]interface{}) *logrus.Entry {
	return l.lger.WithFields(fields)
}
func (l *Logger) Warn(args ...interface{}) {
	l.lger.Warn(args)
}
func (l *Logger) Warnf(format string, args ...interface{}) {
	l.lger.Warnf(format, args)
}
func (l *Logger) Info(args ...interface{}) {
	l.lger.Info(args)
}
func (l *Logger) Infof(format string, args ...interface{}) {
	l.lger.Infof(format, args)
}
func (l *Logger) Panic(args ...interface{}) {
	l.lger.Panic(args)
}
func (l *Logger) Panicf(format string, args ...interface{}) {
	l.lger.Panicf(format, args)
}
func (l *Logger) Fatal(args ...interface{}) {
	l.lger.Fatal(args)
}
func (l *Logger) Fatalf(format string, args ...interface{}) {
	l.lger.Fatalf(format, args)
}

func (l *Logger) openLogFile() {

	bfile, err := os.OpenFile(l.lfp, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)

	if err != nil {

		l.Panic("Init log failed")
	}

	l.lger.SetOutput(bfile)

	l.lf = bfile

	l.lger.SetReportCaller(true)
}

func (l *Logger) reOpenLogFile() {

	l.lock.Lock()
	defer l.lock.Unlock()

	l.lf.Close()

	l.openLogFile()
}

func (l *Logger) logSegmentation(timeForm time.Duration) {

	ch := make(chan bool)

	go func() {
		for {
			now := time.Now()
			nextHour := now.Truncate(timeForm).Add(timeForm).Add(time.Second)
			timer := time.NewTimer(nextHour.Sub(now))
			<-timer.C
			ch <- true
		}
	}()
	go func() {
		for {
			<-ch
			t := l.getTimeStr(timeForm)

			filename := fmt.Sprintf("%s.%s", l.lfp, t)
			os.Rename(l.lfp, filename)
			l.reOpenLogFile()
		}
	}()
}

func (l *Logger) getTimeStr(timeForm time.Duration) string {
	t := time.Now().Add(time.Second * -10)

	if timeForm == time.Minute {
		return fmt.Sprintf("%04d%02d%02d%02d%02d",
			t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute())
	}
	if timeForm == time.Hour {
		return fmt.Sprintf("%04d%02d%02d%02d",
			t.Year(), t.Month(), t.Day(), t.Hour())
	}
	if timeForm == time.Hour*24 {
		return fmt.Sprintf("%04d%02d%02d",
			t.Year(), t.Month(), t.Day())
	}

	return fmt.Sprintf("%04d%02d%02d%02d%02d%02d",
		t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second())
}

func (l *Logger) GetLogId() string {

	machineId := os.Getpid()
	datacenterId := machineId + 1
	curTimeStamp := time.Now().UnixNano() / 1000000

	if curTimeStamp == l.lastTimeStamp {

		if l.seq > 4095 {
			time.Sleep(time.Millisecond)
			curTimeStamp = time.Now().UnixNano() / 1000000
			l.seq = 0
		}

	} else {
		l.seq = 0
	}

	l.seq++

	l.lastTimeStamp = curTimeStamp

	curTimeStamp = curTimeStamp << 22
	machineId = machineId << 17
	datacenterId = datacenterId << 12

	return strconv.Itoa(int(curTimeStamp) | machineId | datacenterId | l.seq)
}
