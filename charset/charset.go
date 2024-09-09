package charset

import (
	"errors"
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
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

func Decode(id sql.CharacterSetID, encoded string) (string, error) {
	switch id {
	case sql.CharacterSet_Unspecified, sql.CharacterSet_ascii, sql.CharacterSet_utf8mb3, sql.CharacterSet_utf8mb4:
		return encoded, nil
	case sql.CharacterSet_latin1:
		// https://dev.mysql.com/doc/refman/8.4/en/charset-we-sets.html
		// > MySQL's latin1 is the same as the Windows cp1252 character set.
		return charmap.Windows1252.NewDecoder().String(encoded)

	case sql.CharacterSet_ucs2, sql.CharacterSet_utf16:
		// https://dev.mysql.com/doc/refman/8.4/en/charset-unicode-utf16.html
		return unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM).NewDecoder().String(encoded)
	case sql.CharacterSet_utf16le:
		// https://dev.mysql.com/doc/refman/8.4/en/charset-unicode-utf16le.html
		return unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM).NewDecoder().String(encoded)
	case sql.CharacterSet_utf32:
		// https://dev.mysql.com/doc/refman/8.4/en/charset-unicode-utf32.html
		return utf32.UTF32(utf32.BigEndian, utf32.IgnoreBOM).NewDecoder().String(encoded)

	// https://dev.mysql.com/doc/refman/8.4/en/faqs-cjk.html
	case sql.CharacterSet_gb2312, sql.CharacterSet_gbk:
		return simplifiedchinese.GBK.NewDecoder().String(encoded)
	case sql.CharacterSet_gb18030:
		return simplifiedchinese.GB18030.NewDecoder().String(encoded)
	case sql.CharacterSet_big5:
		return traditionalchinese.Big5.NewDecoder().String(encoded)
	}
	return encoded, fmt.Errorf("%s: %w", id, ErrUnsupported)
}

func Encode(id sql.CharacterSetID, utf8 string) (string, error) {
	switch id {
	case sql.CharacterSet_Unspecified, sql.CharacterSet_ascii, sql.CharacterSet_utf8mb3, sql.CharacterSet_utf8mb4:
		return utf8, nil
	case sql.CharacterSet_latin1:
		return charmap.Windows1252.NewEncoder().String(utf8)
	case sql.CharacterSet_ucs2, sql.CharacterSet_utf16:
		return unicode.UTF16(unicode.BigEndian, unicode.IgnoreBOM).NewEncoder().String(utf8)
	case sql.CharacterSet_utf16le:
		return unicode.UTF16(unicode.LittleEndian, unicode.IgnoreBOM).NewEncoder().String(utf8)
	case sql.CharacterSet_utf32:
		return utf32.UTF32(utf32.BigEndian, utf32.IgnoreBOM).NewEncoder().String(utf8)
	case sql.CharacterSet_gb2312, sql.CharacterSet_gbk:
		return simplifiedchinese.GBK.NewEncoder().String(utf8)
	case sql.CharacterSet_gb18030:
		return simplifiedchinese.GB18030.NewEncoder().String(utf8)
	case sql.CharacterSet_big5:
		return traditionalchinese.Big5.NewEncoder().String(utf8)
	}
	return utf8, fmt.Errorf("%s: %w", id, ErrUnsupported)
}
