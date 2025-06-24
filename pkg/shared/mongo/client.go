// Package mongo provides shared MongoDB connection management
package mongo

import (
	"context"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	defaultDB           = "blobby"
	defaultTimeout      = 10 * time.Second
	defaultPingTimeout  = 3 * time.Second
)

// Client provides shared MongoDB connection management with connection pooling
type Client struct {
	url    string
	db     *mongo.Database
	mu     sync.Mutex
	
	// Configuration options
	timeout     time.Duration
	pingTimeout time.Duration
	direct      bool // for testing
}

// NewClient creates a new MongoDB client with default settings
func NewClient(url string) *Client {
	return &Client{
		url:         url,
		timeout:     defaultTimeout,
		pingTimeout: defaultPingTimeout,
		direct:      false,
	}
}

// WithTimeout sets the connection timeout
func (c *Client) WithTimeout(timeout time.Duration) *Client {
	c.timeout = timeout
	return c
}

// WithPingTimeout sets the ping timeout
func (c *Client) WithPingTimeout(pingTimeout time.Duration) *Client {
	c.pingTimeout = pingTimeout
	return c
}

// WithDirect enables direct connection (for testing)
func (c *Client) WithDirect(direct bool) *Client {
	c.direct = direct
	return c
}

// GetDB returns a MongoDB database connection, creating one if needed
func (c *Client) GetDB(ctx context.Context) (*mongo.Database, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if c.db != nil {
		return c.db, nil
	}
	
	// Configure client options
	opt := options.Client().ApplyURI(c.url).SetTimeout(c.timeout)
	if c.direct {
		opt = opt.SetDirect(true)
	}
	
	client, err := mongo.Connect(ctx, opt)
	if err != nil {
		return nil, err
	}
	
	// Ping to verify connection
	pingCtx, cancel := context.WithTimeout(ctx, c.pingTimeout)
	defer cancel()
	if err := client.Ping(pingCtx, nil); err != nil {
		return nil, err
	}
	
	c.db = client.Database(defaultDB)
	return c.db, nil
}

// Close closes the MongoDB connection
func (c *Client) Close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	if c.db != nil {
		if client := c.db.Client(); client != nil {
			return client.Disconnect(ctx)
		}
	}
	return nil
}