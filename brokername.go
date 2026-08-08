package admin

// RocketMQ 5.x 的 Proxy 会把客户端请求转发给后端 Broker，转发前必须先知道
// 目标 Broker 的名称，因此要求请求头里带上 bname 字段（SEND_MESSAGE_V2 用 n）。
// 缺少该字段时 Proxy 直接拒绝：
//
//	Request doesn't have field bname
//
// 直连 Broker 时该字段不影响任何行为——Broker 的请求头解析只读取自己声明的
// 字段，多出来的会被忽略，而 4.9 之后的多数请求头本来就声明了 brokerName。
// 所以这里无条件补上，无需调用方感知连接的是 Proxy 还是 Broker。
//
// Broker 名称从路由和集群信息里学习：两者都给出了名称到地址的映射，而客户端
// 在访问某个 Broker 之前必然先查过其中之一。

// brokerNameField 是 Proxy 用来选择转发目标的请求头字段名。
const brokerNameField = "bname"

// rememberBrokerName 记录一个 Broker 地址对应的名称。
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

// rememberRouteBrokerNames 从 Topic 路由中学习 Broker 名称。
//
// Proxy 会把路由里的 Broker 地址改写成 Proxy 自己的地址，因此这里记录下来的
// 是「Proxy 地址 -> 真实 Broker 名称」，正好是转发时需要的组合。
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

// rememberClusterBrokerNames 从集群信息中学习 Broker 名称。
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

// brokerNameFor 返回已知的 Broker 名称，未知时返回空串。
func (c *Client) brokerNameFor(brokerAddr string) string {
	c.brokerNameMu.RLock()
	defer c.brokerNameMu.RUnlock()
	return c.brokerNames[brokerAddr]
}
