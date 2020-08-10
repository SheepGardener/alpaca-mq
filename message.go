package alpaca

type Kmessage struct {
	Cmd     string
	LogId   string
	HashKey string
	Data    string
	Delay   int
}

type AlpaceMsg struct {
	kmsg Kmessage
	part int32
	oft  int64
}
