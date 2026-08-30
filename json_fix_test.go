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

// The consumer running info a client reports, in the shape RocketMQ sends it.
//
// mqTable is a Fastjson map keyed by MessageQueue objects, so nothing here
// parses until fixJSONBody runs. The queue fields are ProcessQueueInfo's own
// names - "droped" included - and a mismatch there decodes to a silent zero
// rather than an error, which is why they are asserted one by one.
func TestFixJSONBodyConsumerRunningInfo(t *testing.T) {
	raw := []byte(`{"mqPopTable":{},"mqTable":{{"brokerName":"broker-a","queueId":0,"topic":"T"}:` +
		`{"cachedMsgCount":12,"cachedMsgMaxOffset":9,"cachedMsgMinOffset":1,"cachedMsgSizeInMiB":3,` +
		`"commitOffset":1166360,"droped":true,"lastConsumeTimestamp":1788097312437,` +
		`"lastLockTimestamp":1788092587684,"lastPullTimestamp":1788097312438,"locked":true,` +
		`"transactionMsgCount":0,"transactionMsgMaxOffset":0,"transactionMsgMinOffset":0,` +
		`"tryUnlockTimes":2}},"properties":{"PROP_CLIENT_VERSION":"V5_3_2"},` +
		`"statusTable":{"T":{"consumeFailedMsgs":4,"consumeFailedTPS":0.5,"consumeOKTPS":963.52,` +
		`"consumeRT":0.0014,"pullRT":9.48,"pullTPS":963.1}},` +
		`"subscriptionSet":[{"classFilterMode":false,"codeSet":[],"expressionType":"TAG",` +
		`"subString":"*","subVersion":1788092587690,"tagsSet":[],"topic":"T"}]}`)

	var info ConsumerRunningInfo
	if err := json.Unmarshal(fixJSONBody(raw), &info); err != nil {
		t.Fatalf("unmarshal fixed body: %v", err)
	}

	if len(info.MqTable) != 1 {
		t.Fatalf("mqTable len=%d want 1", len(info.MqTable))
	}
	if info.Properties["PROP_CLIENT_VERSION"] != "V5_3_2" {
		t.Errorf("properties=%v", info.Properties)
	}
	if len(info.SubscriptionSet) != 1 || info.SubscriptionSet[0].Topic != "T" {
		t.Errorf("subscriptionSet=%v", info.SubscriptionSet)
	}

	for key, queue := range info.MqTable {
		if key != `{"brokerName":"broker-a","queueId":0,"topic":"T"}` {
			t.Errorf("mqTable key=%q", key)
		}
		if queue.CachedMsgCount != 12 {
			t.Errorf("CachedMsgCount=%d want 12", queue.CachedMsgCount)
		}
		if queue.CachedMsgSizeInMiB != 3 {
			t.Errorf("CachedMsgSizeInMiB=%d want 3", queue.CachedMsgSizeInMiB)
		}
		if queue.CommitOffset != 1166360 {
			t.Errorf("CommitOffset=%d", queue.CommitOffset)
		}
		if queue.LastPullTimestamp != 1788097312438 || queue.LastConsumeTimestamp != 1788097312437 {
			t.Errorf("timestamps pull=%d consume=%d", queue.LastPullTimestamp, queue.LastConsumeTimestamp)
		}
		if !queue.Locked || !queue.Dropped {
			t.Errorf("locked=%v dropped=%v, want both true", queue.Locked, queue.Dropped)
		}
		if queue.TryUnlockTimes != 2 {
			t.Errorf("TryUnlockTimes=%d want 2", queue.TryUnlockTimes)
		}
	}

	status, ok := info.StatusTable["T"]
	if !ok {
		t.Fatalf("statusTable=%v", info.StatusTable)
	}
	if status.PullTPS != 963.1 || status.ConsumeOKTPS != 963.52 || status.ConsumeFailedMsgs != 4 {
		t.Errorf("status=%+v", status)
	}
}
