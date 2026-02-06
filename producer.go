package admin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/codermast/rocketmq-admin-go/protocol/remoting"
)

// =============================================================================
// 生产者管理
// =============================================================================

// ExamineProducerConnectionInfo 查询生产者连接信息
func (c *Client) ExamineProducerConnectionInfo(ctx context.Context, producerGroup, topic string) (*ProducerConnection, error) {
	// 先获取集群信息
	clusterInfo, err := c.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		return nil, err
	}

	// 尝试从任意 Broker 获取生产者连接信息
	for _, brokerData := range clusterInfo.BrokerAddrTable {
		var brokerAddr string
		for _, addr := range brokerData.BrokerAddrs {
			brokerAddr = addr
			break
		}

		if brokerAddr == "" {
			continue
		}

		extFields := map[string]string{
			"producerGroup": producerGroup,
			"topic":         topic,
		}
		cmd := remoting.NewRequest(remoting.GetProducerConnectionList, extFields)

		resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
		if err != nil {
			continue
		}

		if resp.Code != remoting.Success {
			continue
		}

		var connInfo ProducerConnection
		if err := json.Unmarshal(resp.Body, &connInfo); err != nil {
			continue
		}

		return &connInfo, nil
	}

	return nil, fmt.Errorf("未找到生产者组 %s 的连接信息", producerGroup)
}

// GetAllProducerInfo 获取所有生产者信息
func (c *Client) GetAllProducerInfo(ctx context.Context, brokerAddr string) (map[string][]Connection, error) {
	cmd := remoting.NewRequest(remoting.GetProducerInfo, nil)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	result := make(map[string][]Connection)
	if err := json.Unmarshal(resp.Body, &result); err != nil {
		return nil, fmt.Errorf("解析生产者信息失败: %w", err)
	}

	return result, nil
}
