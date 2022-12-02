package config

import (
	"encoding/json"
	"log"
	"os"
)

var (
	Config *Configuration
)

type Configuration struct {
	RabbitHost       string
	RabbitPort       int
	RabbitExchange   string
	RabbitQueue      string
	Rabbituser       string
	Rabbitpass       string
	ClickhouseHost   string
	ClickhousePort   int
	ClickhouseDBName string
	CountInsertRow   int
}

func init() {
	var err error
	Config, err = readConcfig()
	if err != nil {
		log.Println("unknown configuration. Check the config file")
		panic(err)
	}
}

func readConcfig() (*Configuration, error) {
	config := Configuration{}
	var err error
	config_file := "conf.json"
	_, err = os.Stat(config_file)
	if err == nil {
		file, _ := os.Open(config_file)
		defer file.Close()
		decoder := json.NewDecoder(file)
		err = decoder.Decode(&config)
	}
	return &config, err
}
