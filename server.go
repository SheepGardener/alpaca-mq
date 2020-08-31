package alpaca

import (
	"math/rand"
	"time"
)

type Server interface {
	GetAppUrl(ap App) string
}

type RoundRobin struct {
	cmaps map[string]*CurrMap
}

type CurrMap struct {
	curr int
	clen int
}

func NewRoundRobin(aplist map[string]App) *RoundRobin {

	cmlist := make(map[string]*CurrMap)

	for _, v := range aplist {

		cmap := &CurrMap{
			curr: 0,
			clen: len(v.Servers),
		}

		cmlist[v.Cmd] = cmap

	}

	return &RoundRobin{
		cmaps: cmlist,
	}
}

func (r *RoundRobin) GetAppUrl(ap App) string {

	cmap, _ := r.cmaps[ap.Cmd]

	host := ap.Servers[cmap.curr]

	cmap.curr = (cmap.curr + 1) % cmap.clen

	return AssembleApUrl(ap.Protocol, host, ap.Path)
}

type RandomSelect struct {
}

func NewRandomSelect() *RandomSelect {
	return &RandomSelect{}
}

func (r *RandomSelect) GetAppUrl(ap App) string {

	rand.Seed(time.Now().UnixNano())

	host := ap.Servers[rand.Intn(len(ap.Servers)-1)]

	return AssembleApUrl(ap.Protocol, host, ap.Path)
}
