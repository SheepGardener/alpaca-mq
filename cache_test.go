package alpaca

import "testing"

func TestCache(t *testing.T) {

	ce := InitPool("127.0.0.1:6379")

	serr := ce.SetInt64("testKey", 32)
	if serr != nil {
		t.Fatalf("Cache SetInt64 Failed err:%s", serr)
	}

	dt, gerr := ce.GetInt64("testKey")

	if dt != 32 || gerr != nil {
		t.Fatalf("Cache GetInt64 Failed err:%s", serr)
	}
	ok, exerr := ce.Exists("testKey")
	if !ok || exerr != nil {
		t.Fatalf("Cache Exists Failed err:%s", serr)
	}
}
