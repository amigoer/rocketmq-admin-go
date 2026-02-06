package admin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/codermast/rocketmq-admin-go/protocol/remoting"
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
