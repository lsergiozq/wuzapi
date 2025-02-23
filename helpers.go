package main

import (
	"fmt"
	"strconv"
)

func Find(slice []string, val string) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

// Update entry in User map
func updateUserInfo(values interface{}, field string, value string) interface{} {
	//log.Debug().Str("field",field).Str("value",value).Msg("User info updated")
	values.(Values).m[field] = value
	return values
}

// webhook for regular messages
func callHook(myurl string, payload map[string]string, id int) {
	log.Info().Str("url", myurl).Msg("Sending POST to client " + strconv.Itoa(id))

	// Log the payload map
	//log.Debug().Msg("Payload:")
	// for key, value := range payload {
	//     log.Debug().Str(key, value).Msg("")
	// }

	_, err := clientHttp[id].R().SetFormData(payload).Post(myurl)
	if err != nil {
		//log.Debug().Str("error",err.Error())
	}
}

// webhook for messages with file attachments
func callHookFile(myurl string, payload map[string]string, id int, file string) error {
	log.Info().Str("file", file).Str("url", myurl).Msg("Sending POST")

	resp, err := clientHttp[id].R().
		SetFiles(map[string]string{
			"file": file,
		}).
		SetFormData(payload).
		Post(myurl)

	if err != nil {
		log.Error().Err(err).Str("url", myurl).Msg("Failed to send POST request")
		return fmt.Errorf("failed to send POST request: %w", err)
	}

	// Optionally, you can log the response status
	log.Info().Int("status", resp.StatusCode()).Msg("POST request completed")

	return nil
}
