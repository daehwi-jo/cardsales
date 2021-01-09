package main

import (
	"bytes"
	"net/http"
	"strings"
)

func sendChannel(title, msg string) {
	body := `{ "conversation_id": 655403, "text": "캐시컴바인 알림",	"blocks": [ { "type": "header",	"text": "${TITLE}", "style": "blue" },  { "type": "text", "text": "${MSG}", "markdown": true } ] }`
	body = strings.Replace(body, "${TITLE}", title, -1)
	body = strings.Replace(body, "${MSG}", msg, -1)

	urlStr := "https://api.kakaowork.com/v1/messages.send?Content-Type=application/json"
	lprintf(4, "[INFO][go] url str(%s) \n", urlStr)
	req, err := http.NewRequest("POST", urlStr, bytes.NewBuffer([]byte(body)))
	if err != nil {
		lprintf(1, "[ERROR] http NewRequest (%s) \n", err.Error())
		return
	}

	req.Header.Set("Authorization", "Bearer 177f6c7f.dfa16ed40fd1493782f308ac9d15ce25")
	req.Header.Add("Content-Type", "application/json")

	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		lprintf(1, "[ERROR] do error: http (%s) \n", err)
		return
	}
	defer resp.Body.Close()

	return
}
