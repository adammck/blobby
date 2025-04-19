package types

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRecordTombstone(t *testing.T) {
	ts := time.Now().UTC().Truncate(time.Millisecond)

	testCases := []struct {
		name      string
		record    Record
		expectDoc bool
	}{
		{
			name: "normal record",
			record: Record{
				Key:       "test-key",
				Timestamp: ts,
				Document:  []byte("test-document"),
				Tombstone: false,
			},
			expectDoc: true,
		},
		{
			name: "tombstone record",
			record: Record{
				Key:       "test-key",
				Timestamp: ts,
				Document:  nil,
				Tombstone: true,
			},
			expectDoc: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Serialize
			var buf bytes.Buffer
			n, err := tc.record.Write(&buf)
			require.NoError(t, err)
			require.Greater(t, n, 0)

			// Deserialize
			readRecord, err := Read(&buf)
			require.NoError(t, err)
			require.NotNil(t, readRecord)

			// Verify fields
			require.Equal(t, tc.record.Key, readRecord.Key)
			require.Equal(t, tc.record.Timestamp.Unix(), readRecord.Timestamp.Unix())
			require.Equal(t, tc.record.Tombstone, readRecord.Tombstone)

			if tc.expectDoc {
				require.NotNil(t, readRecord.Document)
				require.Equal(t, tc.record.Document, readRecord.Document)
			} else {
				// For tombstone records, Document should be nil or empty
				if readRecord.Document != nil {
					require.Empty(t, readRecord.Document)
				}
			}
		})
	}
}

func TestTombstoneBackwardCompatibility(t *testing.T) {
	// Create a record without explicitly setting Tombstone
	record := Record{
		Key:       "test-key",
		Timestamp: time.Now().UTC().Truncate(time.Millisecond),
		Document:  []byte("test-document"),
		// Tombstone not set, should default to false
	}

	// Serialize
	var buf bytes.Buffer
	_, err := record.Write(&buf)
	require.NoError(t, err)

	// Deserialize
	readRecord, err := Read(&buf)
	require.NoError(t, err)
	require.NotNil(t, readRecord)

	// Verify tombstone defaults to false
	require.False(t, readRecord.Tombstone)
}
