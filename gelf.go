package main

import (
	"encoding/json"
	"fmt"
	"regexp"
)

var reIdentifier = regexp.MustCompile("^[a-zA-Z][a-zA-Z0-9_-]*$")

type BasicGelfMessage struct {
	Version           string             `json:"version"`
	Host              string             `json:"host"`
	ShortMessage      string             `json:"short_message"`
	FullMessage       string             `json:"full_message"`
	Timestamp         uint32             `json:"timestamp"`
	AdditionalStrings map[string]string  `json:"-"`
	AdditionalNumbers map[string]float64 `json:"-"`
}

func ParseGelfMessage(data []byte) (msg BasicGelfMessage, err error) {
	msg.AdditionalStrings = map[string]string{}
	msg.AdditionalNumbers = map[string]float64{}

	generic := map[string]interface{}{}
	err = json.Unmarshal(data, &generic)
	if err != nil {
		return
	}

	var ok bool
	for k, v := range generic {
		switch k {
		case "version":
			msg.Version, ok = v.(string)
			if !ok {
				err = fmt.Errorf("String expected at %s", k)
				return
			}
		case "host":
			msg.Host, ok = v.(string)
			if !ok {
				err = fmt.Errorf("String expected at %s", k)
				return
			}
		case "short_message":
			msg.ShortMessage, ok = v.(string)
			if !ok {
				err = fmt.Errorf("String expected at %s", k)
				return
			}
		case "full_message":
			msg.FullMessage, ok = v.(string)
			if !ok {
				err = fmt.Errorf("String expected at %s", k)
				return
			}
		case "timestamp":
			v2, ok := v.(float64)
			if !ok {
				err = fmt.Errorf("Number expected at %s", k)
				return
			}
			msg.Timestamp = uint32(v2)
		default:
			if len(k) == 0 {
				err = fmt.Errorf("0-length key")
				return
			}
			if k[0] == '_' {
				k = k[1:]
			}
			if !reIdentifier.MatchString(k) {
				err = fmt.Errorf("Invalid GELF message key: '%s'", k)
				return
			}
			switch v2 := v.(type) {
			case float64:
				msg.AdditionalNumbers[k] = v2
			case string:
				msg.AdditionalStrings[k] = v2
			case bool:
				if v2 {
					msg.AdditionalNumbers[k] = 1
				} else {
					msg.AdditionalNumbers[k] = 0
				}
			default:
				err = fmt.Errorf("Invalid type at %s", k)
				return
			}
		}
	}
	return
}
