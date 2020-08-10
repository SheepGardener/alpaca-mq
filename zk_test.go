package alpaca

import (
	"testing"
	"time"
)

func TestZk(t *testing.T) {

	zk, cterr := NewZk([]string{"127.0.0.1:2181"}, 20*time.Second)

	if cterr != nil {
		t.Fatalf("ZK Init Failed %s", cterr)
	}

	path := "/zktest"

	err := zk.Create(path, []byte("msg"), 0, zk.WorldACL())

	if err != nil {
		t.Fatalf("ZK Create Failed %s", err)
	}

	ok, ext := zk.Exists(path)

	if !ok || ext != nil {
		t.Fatalf("ZK Exists Failed")
	}

	_, cerr := zk.GetChildren(path)

	if cerr != nil {
		t.Fatalf("ZK GetChildren Failed")
	}

	_, sate, gerr := zk.Get(path)

	if gerr != nil {
		t.Fatalf("ZK Get Failed")
	}

	serr := zk.Set(path, []byte("zk"), sate.Version)

	if serr != nil {
		t.Fatalf("ZK Set Failed")
	}

	_, st, serrs := zk.Get(path)

	if serrs != nil {
		t.Fatalf("ZK Get Failed")
	}

	derr := zk.Delete(path, st.Version)

	if derr != nil {
		t.Fatalf("ZK Delete Failed")
	}
}
