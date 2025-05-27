package api

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNotFound(t *testing.T) {
	err := &NotFound{Key: "test-key"}
	
	require.Equal(t, "key not found: test-key", err.Error())
	
	// test Is method
	require.True(t, errors.Is(err, &NotFound{}))
	require.False(t, errors.Is(err, errors.New("other error")))
}
