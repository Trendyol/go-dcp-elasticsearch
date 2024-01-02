package helper

var (
	EscapeBytes = []byte{
		34, // quote
	}
	BackSlash byte = 92
)

func EscapePredefinedBytes(docID []byte) []byte {
	newByteArr := make([]byte, 0, len(docID))
	for _, byt := range docID {
		for _, escapeByte := range EscapeBytes {
			if escapeByte == byt {
				newByteArr = append(newByteArr, BackSlash)
			}
		}
		newByteArr = append(newByteArr, byt)
	}
	return newByteArr
}
