package admin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/amigoer/rocketmq-admin-go/protocol/remoting"
)

// ExamineProducerConnectionInfo returns the connections of a producer group on a topic.
func (c *Client) ExamineProducerConnectionInfo(ctx context.Context, producerGroup, topic string) (*ProducerConnection, error) {
	clusterInfo, err := c.ExamineBrokerClusterInfo(ctx)
	if err != nil {
		return nil, err
	}

	// Any Broker holding the group can answer; try them in turn.
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

	return nil, fmt.Errorf("no connection info found for producer group %s", producerGroup)
}

// GetAllProducerInfo returns every producer group connected to one Broker.
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
		return nil, fmt.Errorf("failed to parse producer info: %w", err)
	}

	return result, nil
}
