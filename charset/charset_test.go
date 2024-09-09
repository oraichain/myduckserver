package charset

import (
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
)

func TestIsSupported(t *testing.T) {
	testCases := []struct {
		id       sql.CharacterSetID
		expected bool
	}{
		{sql.CharacterSet_Unspecified, true},
		{sql.CharacterSet_ascii, true},
		{sql.CharacterSet_latin1, true},
		{sql.CharacterSet_utf8mb3, true},
		{sql.CharacterSet_utf8mb4, true},
		{sql.CharacterSet_ucs2, true},
		{sql.CharacterSet_utf16, true},
		{sql.CharacterSet_utf16le, true},
		{sql.CharacterSet_utf32, true},
		{sql.CharacterSet_gb2312, true},
		{sql.CharacterSet_gbk, true},
		{sql.CharacterSet_gb18030, true},
		{sql.CharacterSet_big5, true},
		{sql.CharacterSet_binary, false},
	}

	for _, tc := range testCases {
		t.Run(tc.id.String(), func(t *testing.T) {
			assert.Equal(t, tc.expected, IsSupported(tc.id))
		})
	}
}

func TestIsUTF8(t *testing.T) {
	testCases := []struct {
		id       sql.CharacterSetID
		expected bool
	}{
		{sql.CharacterSet_Unspecified, true},
		{sql.CharacterSet_ascii, true},
		{sql.CharacterSet_latin1, false},
		{sql.CharacterSet_utf8mb3, true},
		{sql.CharacterSet_utf8mb4, true},
		{sql.CharacterSet_ucs2, false},
		{sql.CharacterSet_utf16, false},
		{sql.CharacterSet_utf16le, false},
		{sql.CharacterSet_utf32, false},
		{sql.CharacterSet_gb2312, false},
		{sql.CharacterSet_gbk, false},
		{sql.CharacterSet_gb18030, false},
		{sql.CharacterSet_big5, false},
		{sql.CharacterSet_binary, false},
	}

	for _, tc := range testCases {
		t.Run(tc.id.String(), func(t *testing.T) {
			assert.Equal(t, tc.expected, IsUTF8(tc.id))
		})
	}
}

func TestIsSupportedNonUTF8(t *testing.T) {
	testCases := []struct {
		id       sql.CharacterSetID
		expected bool
	}{
		{sql.CharacterSet_Unspecified, false},
		{sql.CharacterSet_ascii, false},
		{sql.CharacterSet_latin1, true},
		{sql.CharacterSet_utf8mb3, false},
		{sql.CharacterSet_utf8mb4, false},
		{sql.CharacterSet_ucs2, true},
		{sql.CharacterSet_utf16, true},
		{sql.CharacterSet_utf16le, true},
		{sql.CharacterSet_utf32, true},
		{sql.CharacterSet_gb2312, true},
		{sql.CharacterSet_gbk, true},
		{sql.CharacterSet_gb18030, true},
		{sql.CharacterSet_big5, true},
		{sql.CharacterSet_binary, false},
	}

	for _, tc := range testCases {
		t.Run(tc.id.String(), func(t *testing.T) {
			assert.Equal(t, tc.expected, IsSupportedNonUTF8(tc.id))
		})
	}
}

func TestDecode(t *testing.T) {
	testCases := []struct {
		id       sql.CharacterSetID
		encoded  string
		expected string
		err      error
	}{
		{sql.CharacterSet_ascii, "hello", "hello", nil},
		{sql.CharacterSet_latin1, "\x80", "€", nil},
		{sql.CharacterSet_ucs2, "\x00\x68\x00\x65\x00\x6c\x00\x6c\x00\x6f", "hello", nil},
		{sql.CharacterSet_utf16le, "\x68\x00\x65\x00\x6c\x00\x6c\x00\x6f\x00", "hello", nil},
		{sql.CharacterSet_utf32, "\x00\x00\x00\x68\x00\x00\x00\x65\x00\x00\x00\x6c\x00\x00\x00\x6c\x00\x00\x00\x6f", "hello", nil},
		{sql.CharacterSet_gb2312, "\xc4\xe3\xba\xc3", "你好", nil},
		{sql.CharacterSet_gbk, "\xc4\xe3\xba\xc3", "你好", nil},
		{sql.CharacterSet_gb18030, "\xc4\xe3\xba\xc3", "你好", nil},
		{sql.CharacterSet_big5, "\xa7\x41\xa6\x6e", "你好", nil},
		{sql.CharacterSet_binary, "hello", "hello", ErrUnsupported},
	}

	for _, tc := range testCases {
		t.Run(tc.id.String(), func(t *testing.T) {
			decoded, err := Decode(tc.id, tc.encoded)
			assert.Equal(t, tc.expected, decoded)
			assert.ErrorIs(t, err, tc.err)
		})
	}
}

func TestEncode(t *testing.T) {
	testCases := []struct {
		id       sql.CharacterSetID
		utf8     string
		expected string
		err      error
	}{
		{sql.CharacterSet_ascii, "hello", "hello", nil},
		{sql.CharacterSet_latin1, "€", "\x80", nil},
		{sql.CharacterSet_ucs2, "hello", "\x00\x68\x00\x65\x00\x6c\x00\x6c\x00\x6f", nil},
		{sql.CharacterSet_utf16le, "hello", "\x68\x00\x65\x00\x6c\x00\x6c\x00\x6f\x00", nil},
		{sql.CharacterSet_utf32, "hello", "\x00\x00\x00\x68\x00\x00\x00\x65\x00\x00\x00\x6c\x00\x00\x00\x6c\x00\x00\x00\x6f", nil},
		{sql.CharacterSet_gb2312, "你好", "\xc4\xe3\xba\xc3", nil},
		{sql.CharacterSet_gbk, "你好", "\xc4\xe3\xba\xc3", nil},
		{sql.CharacterSet_gb18030, "你好", "\xc4\xe3\xba\xc3", nil},
		{sql.CharacterSet_big5, "你好", "\xa7\x41\xa6\x6e", nil},
		{sql.CharacterSet_binary, "hello", "hello", ErrUnsupported},
	}

	for _, tc := range testCases {
		t.Run(tc.id.String(), func(t *testing.T) {
			encoded, err := Encode(tc.id, tc.utf8)
			assert.Equal(t, tc.expected, encoded)
			assert.ErrorIs(t, err, tc.err)
		})
	}
}
