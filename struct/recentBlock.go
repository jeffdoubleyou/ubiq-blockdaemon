package blockwatcher

type RecentBlock struct {
    Block       int64   `json:"block"`
    Miner       string  `json:"miner"`
    Timestamp   int64   `json:"timestamp"`
}
