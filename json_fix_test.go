package admin

import (
	"encoding/json"
	"testing"
)

func TestFixJSONBodyObjectKeyedOffsetTable(t *testing.T) {
	raw := []byte(`{"consumeTps":19.8,"offsetTable":{{"brokerName":"broker-a","queueId":0,"topic":"T"}:{"brokerOffset":12077,"consumerOffset":5110,"lastTimestamp":1,"pullOffset":5156},{"brokerName":"broker-a","queueId":1,"topic":"T"}:{"brokerOffset":12080,"consumerOffset":5109,"lastTimestamp":1,"pullOffset":5159}}}`)
	fixed := fixJSONBody(raw)

	var stats ConsumeStats
	if err := json.Unmarshal(fixed, &stats); err != nil {
		t.Fatalf("unmarshal fixed body: %v\nfixed=%s", err, fixed)
	}
	if len(stats.OffsetTable) != 2 {
		t.Fatalf("offsetTable len=%d want 2; fixed=%s", len(stats.OffsetTable), fixed)
	}
	var lag int64
	for _, off := range stats.OffsetTable {
		if off == nil {
			continue
		}
		if d := off.BrokerOffset - off.ConsumerOffset; d > 0 {
			lag += d
		}
	}
	want := (12077 - 5110) + (12080 - 5109)
	if lag != int64(want) {
		t.Fatalf("lag=%d want=%d", lag, want)
	}
	if stats.ConsumeTps != 19.8 {
		t.Fatalf("tps=%v", stats.ConsumeTps)
	}
}

func TestFixJSONBodyKeepsNormalMaps(t *testing.T) {
	raw := []byte(`{"brokerAddrs":{0:"127.0.0.1:10911"},"name":"broker-a"}`)
	fixed := fixJSONBody(raw)
	var decoded map[string]any
	if err := json.Unmarshal(fixed, &decoded); err != nil {
		t.Fatalf("unmarshal: %v\nfixed=%s", err, fixed)
	}
	addrs, ok := decoded["brokerAddrs"].(map[string]any)
	if !ok || addrs["0"] != "127.0.0.1:10911" {
		t.Fatalf("brokerAddrs=%v", decoded["brokerAddrs"])
	}
}
