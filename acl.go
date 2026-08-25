package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/amigoer/rocketmq-admin-go/protocol/remoting"
)

// The user and ACL calls in this file are RocketMQ 5.x only. A 4.x cluster
// has no notion of users; it is configured through the PlainAccessConfig
// calls further down, which edit plain_acl.yml.

// CreateUser creates a user on one Broker.
func (c *Client) CreateUser(ctx context.Context, brokerAddr string, user UserInfo) error {
	body, err := json.Marshal(user)
	if err != nil {
		return fmt.Errorf("failed to marshal user info: %w", err)
	}

	cmd := remoting.NewRequest(remoting.CreateUser, nil)
	cmd.Body = body

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// UpdateUser updates an existing user on one Broker.
func (c *Client) UpdateUser(ctx context.Context, brokerAddr string, user UserInfo) error {
	body, err := json.Marshal(user)
	if err != nil {
		return fmt.Errorf("failed to marshal user info: %w", err)
	}

	cmd := remoting.NewRequest(remoting.UpdateUser, nil)
	cmd.Body = body

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// DeleteUser removes a user from one Broker.
func (c *Client) DeleteUser(ctx context.Context, brokerAddr, username string) error {
	extFields := map[string]string{
		"username": username,
	}
	cmd := remoting.NewRequest(remoting.DeleteUser, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// GetUser returns a single user from one Broker.
func (c *Client) GetUser(ctx context.Context, brokerAddr, username string) (*UserInfo, error) {
	extFields := map[string]string{
		"username": username,
	}
	cmd := remoting.NewRequest(remoting.GetUser, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var user UserInfo
	if err := json.Unmarshal(resp.Body, &user); err != nil {
		return nil, fmt.Errorf("failed to parse user info: %w", err)
	}

	return &user, nil
}

// ListUser returns every user known to one Broker.
func (c *Client) ListUser(ctx context.Context, brokerAddr string) (*UserList, error) {
	cmd := remoting.NewRequest(remoting.ListUser, nil)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var users UserList
	if err := json.Unmarshal(resp.Body, &users); err != nil {
		return nil, fmt.Errorf("failed to parse user list: %w", err)
	}

	return &users, nil
}

// CreateAcl creates an ACL rule on one Broker.
func (c *Client) CreateAcl(ctx context.Context, brokerAddr string, acl AclInfo) error {
	body, err := json.Marshal(acl)
	if err != nil {
		return fmt.Errorf("failed to marshal ACL info: %w", err)
	}

	cmd := remoting.NewRequest(remoting.CreateAcl, nil)
	cmd.Body = body

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// UpdateAcl updates an existing ACL rule on one Broker.
func (c *Client) UpdateAcl(ctx context.Context, brokerAddr string, acl AclInfo) error {
	body, err := json.Marshal(acl)
	if err != nil {
		return fmt.Errorf("failed to marshal ACL info: %w", err)
	}

	cmd := remoting.NewRequest(remoting.UpdateAcl, nil)
	cmd.Body = body

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// DeleteAcl removes an ACL rule from one Broker.
func (c *Client) DeleteAcl(ctx context.Context, brokerAddr, subject string) error {
	extFields := map[string]string{
		"subject": subject,
	}
	cmd := remoting.NewRequest(remoting.DeleteAcl, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// GetAcl returns a single ACL rule from one Broker.
func (c *Client) GetAcl(ctx context.Context, brokerAddr, subject string) (*AclInfo, error) {
	extFields := map[string]string{
		"subject": subject,
	}
	cmd := remoting.NewRequest(remoting.GetAcl, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var acl AclInfo
	if err := json.Unmarshal(resp.Body, &acl); err != nil {
		return nil, fmt.Errorf("failed to parse ACL info: %w", err)
	}

	return &acl, nil
}

// ListAcl returns every ACL rule on one Broker.
func (c *Client) ListAcl(ctx context.Context, brokerAddr string) (*AclList, error) {
	cmd := remoting.NewRequest(remoting.ListAcl, nil)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	var acls AclList
	if err := json.Unmarshal(resp.Body, &acls); err != nil {
		return nil, fmt.Errorf("failed to parse ACL list: %w", err)
	}

	return &acls, nil
}

// UpdatePlainAccessConfig creates or updates one legacy access config, matched
// by accessKey. Java: RequestCode.UPDATE_AND_CREATE_ACL_CONFIG = 50.
//
// Everything travels in ExtFields (the request header), with topicPerms and
// groupPerms comma-joined.
func (c *Client) UpdatePlainAccessConfig(ctx context.Context, brokerAddr string, config PlainAccessConfig) error {
	extFields := map[string]string{
		"accessKey": config.AccessKey,
	}
	if config.SecretKey != "" {
		extFields["secretKey"] = config.SecretKey
	}
	if config.WhiteRemoteAddress != "" {
		extFields["whiteRemoteAddress"] = config.WhiteRemoteAddress
	}
	if config.Admin {
		extFields["admin"] = "true"
	} else {
		extFields["admin"] = "false"
	}
	if config.DefaultTopicPerm != "" {
		extFields["defaultTopicPerm"] = config.DefaultTopicPerm
	}
	if config.DefaultGroupPerm != "" {
		extFields["defaultGroupPerm"] = config.DefaultGroupPerm
	}
	if len(config.TopicPerms) > 0 {
		extFields["topicPerms"] = strings.Join(config.TopicPerms, ",")
	}
	if len(config.GroupPerms) > 0 {
		extFields["groupPerms"] = strings.Join(config.GroupPerms, ",")
	}

	cmd := remoting.NewRequest(remoting.UpdateAndCreateAclConfig, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// DeletePlainAccessConfig removes one legacy access config by accessKey.
// Java: RequestCode.DELETE_ACL_CONFIG = 51.
func (c *Client) DeletePlainAccessConfig(ctx context.Context, brokerAddr, accessKey string) error {
	extFields := map[string]string{
		"accessKey": accessKey,
	}
	cmd := remoting.NewRequest(remoting.DeleteAclConfig, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}

// GetBrokerClusterAclInfo returns the ACL version info of a Broker cluster.
// Java: RequestCode.GET_BROKER_CLUSTER_ACL_INFO = 52.
//
// The version comes back in the response ExtFields, not the body.
func (c *Client) GetBrokerClusterAclInfo(ctx context.Context, brokerAddr string) (*BrokerClusterAclVersionInfo, error) {
	cmd := remoting.NewRequest(remoting.GetBrokerClusterAclInfo, nil)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return nil, err
	}

	if resp.Code != remoting.Success {
		return nil, NewAdminError(resp.Code, resp.Remark)
	}

	info := &BrokerClusterAclVersionInfo{
		BrokerAddr:  resp.ExtFields["brokerAddr"],
		BrokerName:  resp.ExtFields["brokerName"],
		ClusterName: resp.ExtFields["clusterName"],
		Version:     resp.ExtFields["version"],
	}
	if allVersions, ok := resp.ExtFields["allAclFileVersion"]; ok {
		var versions map[string]string
		if err := json.Unmarshal([]byte(allVersions), &versions); err == nil {
			info.AllAclFileVersion = versions
		}
	}

	return info, nil
}

// UpdateGlobalWhiteAddrsConfig replaces the global IP whitelist.
// Java: RequestCode.UPDATE_GLOBAL_WHITE_ADDRS_CONFIG = 53.
func (c *Client) UpdateGlobalWhiteAddrsConfig(ctx context.Context, brokerAddr string, globalWhiteAddrs []string, aclFilePath string) error {
	extFields := map[string]string{
		"globalWhiteAddrs": strings.Join(globalWhiteAddrs, ","),
	}
	if aclFilePath != "" {
		extFields["aclFilePath"] = aclFilePath
	}

	cmd := remoting.NewRequest(remoting.UpdateGlobalWhiteAddrsConfig, extFields)

	resp, err := c.invokeBroker(ctx, brokerAddr, cmd)
	if err != nil {
		return err
	}

	if resp.Code != remoting.Success {
		return NewAdminError(resp.Code, resp.Remark)
	}

	return nil
}
