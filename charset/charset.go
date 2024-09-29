package charset

import (
	"errors"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"golang.org/x/text/encoding"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/traditionalchinese"
	"golang.org/x/text/encoding/unicode"
	"golang.org/x/text/encoding/unicode/utf32"
)

var ErrUnsupported = errors.New("unsupported charset")

func IsSupported(id sql.CharacterSetID) bool {
	switch id {
	case sql.CharacterSet_Unspecified,
		sql.CharacterSet_ascii,
		sql.CharacterSet_latin1,
		sql.CharacterSet_utf8mb3, sql.CharacterSet_utf8mb4,
		sql.CharacterSet_ucs2, sql.CharacterSet_utf16, sql.CharacterSet_utf16le,
		sql.CharacterSet_utf32,
		sql.CharacterSet_gb2312, sql.CharacterSet_gbk, sql.CharacterSet_gb18030,
		sql.CharacterSet_big5:
		return true
	}
	return false
}

func IsUTF8(id sql.CharacterSetID) bool {
	switch id {
	case sql.CharacterSet_Unspecified, sql.CharacterSet_ascii, sql.CharacterSet_utf8mb3, sql.CharacterSet_utf8mb4:
		return true
	}
	return false
}

func IsSupportedNonUTF8(id sql.CharacterSetID) bool {
	return IsSupported(id) && !IsUTF8(id)
}

func getEncoding(id sql.CharacterSetID) (encoding.Encoding, error) {
	switch id {
	case sql.CharacterSet_Unspecified, sql.CharacterSet_ascii, sql.CharacterSet_utf8mb3, sql.CharacterSet_utf8mb4:
		return encoding.Nop, nil
	case sql.CharacterSet_latin1:
		// https://dev.mysql.com/doc/refman/8.4/en/charset-we-sets.html
		// > MySQL's latin1 is the same as the Windows cp1252 character set.
		return charmap.Windows1252, nil

	case sql.CharacterSet_ucs2, sql.CharacterSet_utf16:
		// https://dev.mysql.com/doc/refman/8.4/en/charset-unicode-utf16.html
		return unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM), nil
	case sql.CharacterSet_utf16le:
		// https://dev.mysql.com/doc/refman/8.4/en/charset-unicode-utf16le.html
		return unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM), nil
	case sql.CharacterSet_utf32:
		// https://dev.mysql.com/doc/refman/8.4/en/charset-unicode-utf32.html
		return utf32.UTF32(utf32.BigEndian, utf32.IgnoreBOM), nil

	// https://dev.mysql.com/doc/refman/8.4/en/faqs-cjk.html
	case sql.CharacterSet_gb2312, sql.CharacterSet_gbk:
		return simplifiedchinese.GBK, nil
	case sql.CharacterSet_gb18030:
		return simplifiedchinese.GB18030, nil
	case sql.CharacterSet_big5:
		return traditionalchinese.Big5, nil
	}
	return encoding.Nop, fmt.Errorf("%s: %w", id, ErrUnsupported)
}

func Encode(id sql.CharacterSetID, utf8 string) (string, error) {
	en, err := getEncoding(id)
	if err != nil {
		return utf8, err
	} else if en == encoding.Nop { // allocation-free fast path
		return utf8, nil
	}
	return en.NewEncoder().String(utf8)
}

func Decode(id sql.CharacterSetID, encoded string) (string, error) {
	en, err := getEncoding(id)
	if err != nil {
		return encoded, err
	} else if en == encoding.Nop {
		return encoded, nil
	}
	return en.NewDecoder().String(encoded)
}

func EncodeBytes(id sql.CharacterSetID, utf8 []byte) ([]byte, error) {
	en, err := getEncoding(id)
	if err != nil {
		return utf8, err
	} else if en == encoding.Nop {
		return utf8, nil
	}
	return en.NewEncoder().Bytes(utf8)
}

func DecodeBytes(id sql.CharacterSetID, encoded []byte) ([]byte, error) {
	en, err := getEncoding(id)
	if err != nil {
		return encoded, err
	} else if en == encoding.Nop {
		return encoded, nil
	}
	return en.NewDecoder().Bytes(encoded)
}
