package helper

func Flatten(s [][]byte) []byte {
	if len(s) == 1 {
		// Just return a copy.
		return append([]byte(nil), s[0]...)
	}
	c := 0
	for i := 0; i < len(s); i++ {
		c += len(s[i])
	}
	b := make([]byte, c)
	bp := copy(b, s[0])
	for _, v := range s[1:] {
		bp += copy(b[bp:], v)
	}
	return b
}
