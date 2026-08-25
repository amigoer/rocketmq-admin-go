package admin

// A RocketMQ 5.x Proxy forwards client requests to a backing Broker, and it
// must know the target Broker's name to do so, so the request header has to
// carry a bname field (SEND_MESSAGE_V2 spells it n). Without the field the
// Proxy rejects the request outright:
//
//	Request doesn't have field bname
//
// Against a Broker directly the field changes nothing: header parsing reads
// only the fields a request declares and ignores the rest, and since 4.9 most
// headers declare brokerName anyway. So it is always filled in, and callers
// need not know whether they are talking to a Proxy or a Broker.
//
// Broker names are learned from route and cluster info: both map names to
// addresses, and a client always queries one of them before reaching a Broker.

// brokerNameField is the header field a Proxy uses to pick a forward target.
const brokerNameField = "bname"

func (c *Client) rememberBrokerName(brokerAddr, brokerName string) {
	if brokerAddr == "" || brokerName == "" {
		return
	}
	c.brokerNameMu.Lock()
	defer c.brokerNameMu.Unlock()
	if c.brokerNames == nil {
		c.brokerNames = make(map[string]string)
	}
	c.brokerNames[brokerAddr] = brokerName
}

// rememberRouteBrokerNames learns Broker names from a topic route.
//
// A Proxy rewrites the Broker addresses in a route to its own address, so what
// gets recorded is "Proxy address -> real Broker name" -- exactly the pairing
// forwarding needs.
func (c *Client) rememberRouteBrokerNames(route *TopicRouteData) {
	if route == nil {
		return
	}
	for _, brokerData := range route.BrokerDatas {
		if brokerData == nil {
			continue
		}
		for _, addr := range brokerData.BrokerAddrs {
			c.rememberBrokerName(addr, brokerData.BrokerName)
		}
	}
}

func (c *Client) rememberClusterBrokerNames(clusterInfo *ClusterInfo) {
	if clusterInfo == nil {
		return
	}
	for brokerName, brokerData := range clusterInfo.BrokerAddrTable {
		if brokerData == nil {
			continue
		}
		name := brokerData.BrokerName
		if name == "" {
			name = brokerName
		}
		for _, addr := range brokerData.BrokerAddrs {
			c.rememberBrokerName(addr, name)
		}
	}
}

// brokerNameFor returns the known name of a Broker, or "" if it is unknown.
func (c *Client) brokerNameFor(brokerAddr string) string {
	c.brokerNameMu.RLock()
	defer c.brokerNameMu.RUnlock()
	return c.brokerNames[brokerAddr]
}
