package blockwatcher

type Miner struct {
    Block       int64   `json:"block"`
    Difficulty  int64   `json:"difficulty"`
    Timestamp   int64   `json:"timestamp"`
    Gas         int64  `json:"gas"`
}
