package main

import "log"

type badgerLogger struct {
}

func (b badgerLogger) Errorf(format string, args ...interface{}) {
	log.Printf("Badger: "+format, args...)
}

func (b badgerLogger) Warningf(format string, args ...interface{}) {
	log.Printf("Badger: "+format, args...)
}

func (b badgerLogger) Infof(format string, args ...interface{}) {
	log.Printf("Badger: "+format, args...)
}

func (b badgerLogger) Debugf(format string, args ...interface{}) {
	log.Printf("Badger: "+format, args...)
}
