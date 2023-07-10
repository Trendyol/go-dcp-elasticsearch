package helper

import (
	"reflect"
	"testing"
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
			if !reflect.DeepEqual(result, test.expected) {
				t.Errorf("Byte() = %v, expected %v", result, test.expected)
			}
		})
	}
}
