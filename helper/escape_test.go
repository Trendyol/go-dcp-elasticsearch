package helper

import (
	"testing"
)

func TestEscapeQuote(t *testing.T) {
	input := "12345-999\""
	byteArr := Byte(input)
	result := EscapePredefinedBytes(byteArr)
	if len(result) == 10 || result[len(result)-1] != 34 {
		t.Error("Expected backslash byte")
	}
}

func TestDoNotEscapeQuote(t *testing.T) {
	input := "12345-999"
	byteArr := Byte(input)

	result := EscapePredefinedBytes(byteArr)
	if len(result) != 9 {
		t.Error("Not expected any change related with length")
	}
	for _, b := range result {
		if b == 34 {
			t.Error("Not expected any backslash")
		}
	}
}
