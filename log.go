package work

import (
	log "github.com/sirupsen/logrus"
)

func logError(key string, err error) {
	log.Errorf("ERROR: %s - %s\n", key, err.Error())
}
