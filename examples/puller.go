package main

import (
	alpaca "github.com/SheepGardener/alpaca-mq"
)

func main() {
	puller := alpaca.InitPuller("./log/puller.log", "./config/puller.yml", "./config/apps/")
	puller.Pull()
}
