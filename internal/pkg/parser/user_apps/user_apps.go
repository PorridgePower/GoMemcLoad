package user_apps

import (
	"errors"
	"strconv"
	"strings"

	log "github.com/sirupsen/logrus"
)

var (
	ErrInvalidLine = errors.New("invalid line")
	ErrInvalidLat  = errors.New("invalid lat")
	ErrInvalidLon  = errors.New("invalid lon")
)

type UserApps struct {
	DevType string
	DevId   string
	Lat     float64
	Lon     float64
	Apps    []uint32
}

func Parse(line string) (UserApps, error) {
	var userApps UserApps

	parts := strings.Split(line, "\t")
	if len(parts) < 5 {
		return userApps, ErrInvalidLine
	}

	userApps.DevType = parts[0]
	userApps.DevId = parts[1]

	lat, err := strconv.ParseFloat(parts[2], 64)
	if err != nil {
		return userApps, ErrInvalidLat
	}
	userApps.Lat = lat

	lon, err := strconv.ParseFloat(parts[3], 64)
	if err != nil {
		return userApps, ErrInvalidLon
	}
	userApps.Lon = lon

	apps := strings.Split(parts[4], ",")
	for _, app := range apps {
		pid, err := strconv.ParseUint(app, 10, 32)
		if err != nil {
			log.Debugf("Failed to parse app: %v\n", app)
			continue
		}
		userApps.Apps = append(userApps.Apps, uint32(pid))
	}

	return userApps, nil
}
