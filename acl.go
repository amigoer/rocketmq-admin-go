package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/amigoer/rocketmq-admin-go/protocol/remoting"
)

// =============================================================================
// ACL 用户管理接口
// =============================================================================

// CreateUser 创建用户
func (c *Client) CreateUser(ctx context.Context, brokerAddr string, user UserInfo) error {
	body, err := json.Marshal(user)
	if err != nil {
		return fmt.Errorf("序列化用户信息失败: %w", err)
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

// UpdateUser 更新用户
func (c *Client) UpdateUser(ctx context.Context, brokerAddr string, user UserInfo) error {
	body, err := json.Marshal(user)
	if err != nil {
		return fmt.Errorf("序列化用户信息失败: %w", err)
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

// DeleteUser 删除用户
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

// GetUser 获取用户信息
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
		return nil, fmt.Errorf("解析用户信息失败: %w", err)
	}

	return &user, nil
}

// ListUser 列出所有用户
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
		return nil, fmt.Errorf("解析用户列表失败: %w", err)
	}

	return &users, nil
}

// =============================================================================
// ACL 规则管理接口
// =============================================================================

// CreateAcl 创建 ACL 规则
func (c *Client) CreateAcl(ctx context.Context, brokerAddr string, acl AclInfo) error {
	body, err := json.Marshal(acl)
	if err != nil {
		return fmt.Errorf("序列化 ACL 信息失败: %w", err)
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

// UpdateAcl 更新 ACL 规则
func (c *Client) UpdateAcl(ctx context.Context, brokerAddr string, acl AclInfo) error {
	body, err := json.Marshal(acl)
	if err != nil {
		return fmt.Errorf("序列化 ACL 信息失败: %w", err)
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

// DeleteAcl 删除 ACL 规则
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

// GetAcl 获取 ACL 规则
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
		return nil, fmt.Errorf("解析 ACL 信息失败: %w", err)
	}

	return &acl, nil
}

// ListAcl 列出所有 ACL 规则
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
		return nil, fmt.Errorf("解析 ACL 列表失败: %w", err)
	}

	return &acls, nil
}

// =============================================================================
// 旧版 ACL 配置管理接口（RocketMQ 4.x，基于 plain_acl.yml）
// =============================================================================

// UpdatePlainAccessConfig 创建或更新一条旧版 access config（按 accessKey 匹配）
// 对应 Java RequestCode.UPDATE_AND_CREATE_ACL_CONFIG = 50
// 参数通过 ExtFields（请求头）传递，topicPerms/groupPerms 用逗号拼接
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

// DeletePlainAccessConfig 按 accessKey 删除一条旧版 access config
// 对应 Java RequestCode.DELETE_ACL_CONFIG = 51
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

// GetBrokerClusterAclInfo 获取 Broker 集群 ACL 版本信息
// 对应 Java RequestCode.GET_BROKER_CLUSTER_ACL_INFO = 52
// 版本信息在 response 的 ExtFields 中返回
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

// UpdateGlobalWhiteAddrsConfig 更新全局白名单地址
// 对应 Java RequestCode.UPDATE_GLOBAL_WHITE_ADDRS_CONFIG = 53
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
