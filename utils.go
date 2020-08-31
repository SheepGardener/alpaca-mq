package alpaca

import (
	"bytes"
	"unsafe"
)

func stringToByte(s string) []byte {
	x := (*[2]uintptr)(unsafe.Pointer(&s))
	t := [3]uintptr{x[0], x[1], x[1]}
	return *(*[]byte)(unsafe.Pointer(&t))
}

func byteToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func AssembleApUrl(pro string, host string, path string) string {

	var url bytes.Buffer

	url.WriteString(pro)
	url.WriteString(":")
	url.WriteString("//")
	url.WriteString(host)
	url.WriteString(path)

	return url.String()
}
