package main

type WwwRespDefault struct {
	Ok      bool   `json:"ok"`
	Message string `json:"message"`
}

type WwwRespQuery struct {
	Ok     bool                     `json:"ok"`
	Result []map[string]interface{} `json:"result"`
}
