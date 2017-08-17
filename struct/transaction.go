package blockwatcher

import (
//    "encoding/json"
)

type Transaction struct {
    Hash        string  `json:"hash"`
    Timestamp   int64   `json:"timestamp"`
    Value       string  `json:"value"`
    From        string  `json:"from"`
    To          string  `json:"to"`
    Number      int64   `json:"number"`
    Contract    int     `json:"contract"`
}
