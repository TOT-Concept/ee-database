package apply

import (
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
)

// DescribeError renders everything the database reported about a failed
// script: the driver's one-line Error() plus the PostgreSQL fields it drops
// (detail, hint, context, table/column/constraint) and, when the server
// reports an error cursor, the offending line of the script. MySQL errors
// already carry code, SQLSTATE and "at line N" in Error(), and SQLite errors
// are flat strings — both pass through unchanged.
func DescribeError(sqlText string, err error) string {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err.Error()
	}
	var b strings.Builder
	b.WriteString(err.Error())
	field := func(label, value string) {
		if value != "" {
			fmt.Fprintf(&b, "\n  %s: %s", label, value)
		}
	}
	field("detail", pgErr.Detail)
	field("hint", pgErr.Hint)
	field("context", pgErr.Where)
	field("table", pgErr.TableName)
	field("column", pgErr.ColumnName)
	field("constraint", pgErr.ConstraintName)
	if pgErr.Position > 0 {
		line, text := locateInScript(sqlText, int(pgErr.Position))
		field(fmt.Sprintf("script line %d", line), text)
	}
	return b.String()
}

// locateInScript maps PostgreSQL's 1-based error cursor — measured in
// characters, not bytes — to the number and text of the script line holding it.
func locateInScript(sqlText string, pos int) (int, string) {
	line, lineStart, chars := 1, 0, 0
	for i, r := range sqlText {
		chars++
		if chars == pos {
			end := strings.IndexByte(sqlText[i:], '\n')
			if end < 0 {
				end = len(sqlText)
			} else {
				end += i
			}
			return line, capLine(sqlText[lineStart:end])
		}
		if r == '\n' {
			line++
			lineStart = i + 1
		}
	}
	return line, capLine(sqlText[lineStart:])
}

// capLine trims and bounds one script line for log output (entity upserts can
// be thousands of characters wide).
func capLine(s string) string {
	s = strings.TrimSpace(s)
	const maxRunes = 160
	runes := []rune(s)
	if len(runes) > maxRunes {
		return string(runes[:maxRunes]) + "…"
	}
	return s
}
