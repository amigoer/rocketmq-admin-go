package admin

import (
	"errors"
	"time"
)

// Options configures a Client.
type Options struct {
	// NameServers lists the NameServer addresses, e.g. "127.0.0.1:9876".
	NameServers []string

	// Timeout bounds a single request.
	Timeout time.Duration

	// RetryTimes is how often a failed request is retried.
	RetryTimes int

	// ACL credentials; leave empty to talk to a cluster without authentication.
	AccessKey string
	SecretKey string
}

// Option applies a single setting to Options.
type Option func(*Options)

func defaultOptions() *Options {
	return &Options{
		Timeout:    3 * time.Second,
		RetryTimes: 2,
	}
}

func (o *Options) validate() error {
	if len(o.NameServers) == 0 {
		return errors.New("NameServers 不能为空")
	}
	return nil
}

// WithNameServers sets the NameServer addresses.
func WithNameServers(addrs []string) Option {
	return func(o *Options) {
		o.NameServers = addrs
	}
}

// WithTimeout sets the per-request timeout.
func WithTimeout(timeout time.Duration) Option {
	return func(o *Options) {
		o.Timeout = timeout
	}
}

// WithRetryTimes sets the retry count for failed requests.
func WithRetryTimes(times int) Option {
	return func(o *Options) {
		o.RetryTimes = times
	}
}

// WithACL sets the ACL credentials.
func WithACL(accessKey, secretKey string) Option {
	return func(o *Options) {
		o.AccessKey = accessKey
		o.SecretKey = secretKey
	}
}
