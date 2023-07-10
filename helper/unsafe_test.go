package helper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestByte(t *testing.T) {
	// given
	tests := []struct {
		name     string
		input    string
		expected []byte
	}{
		{
			name:     "Empty string",
			input:    "",
			expected: nil,
		},
		{
			name:     "Non-empty string",
			input:    "Hello, World!",
			expected: []byte("Hello, World!"),
		},
		{
			name:     "String with special characters",
			input:    "Hello, #@$%&* World!",
			expected: []byte("Hello, #@$%&* World!"),
		},
	}

	// when & then
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := Byte(test.input)
			assert.Equal(t, test.expected, result)
		})
	}
}
