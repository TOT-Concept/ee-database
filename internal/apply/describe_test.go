package apply

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestDescribeErrorPgFields(t *testing.T) {
	err := &pgconn.PgError{
		Severity: "ERROR", Code: "42701",
		Message: `column "very_long_name_" specified more than once`,
		Detail:  "the detail", Hint: "the hint", TableName: "magic_mushroom",
	}
	got := DescribeError("CREATE TABLE x ();", err)
	for _, want := range []string{
		"SQLSTATE 42701", "detail: the detail", "hint: the hint", "table: magic_mushroom",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}
	if strings.Contains(got, "script line") {
		t.Errorf("position 0 must not render a script line:\n%s", got)
	}
}

func TestDescribeErrorPosition(t *testing.T) {
	script := "BEGIN;\nCREATE TABLE t (\n    \"a\" nosuchtype\n);\nCOMMIT;\n"
	pos := strings.Index(script, "nosuchtype") + 1 // ASCII: bytes == characters
	wrapped := fmt.Errorf("delta 430 (schema migrate): %w", &pgconn.PgError{
		Severity: "ERROR", Code: "42704",
		Message: `type "nosuchtype" does not exist`, Position: int32(pos),
	})
	got := DescribeError(script, wrapped)
	if !strings.HasPrefix(got, "delta 430 (schema migrate): ERROR:") {
		t.Errorf("wrap prefix lost:\n%s", got)
	}
	if !strings.Contains(got, `script line 3: "a" nosuchtype`) {
		t.Errorf("missing located line in:\n%s", got)
	}
}

func TestDescribeErrorNonPostgres(t *testing.T) {
	err := errors.New("Error 1064 (42000): You have an error in your SQL syntax at line 3")
	if got := DescribeError("SELECT 1;", err); got != err.Error() {
		t.Errorf("non-pg error must pass through, got:\n%s", got)
	}
}

func TestLocateInScriptBounds(t *testing.T) {
	line, text := locateInScript("SELECT 1;\nSELECT 2;", 1000)
	if line != 2 || text != "SELECT 2;" {
		t.Errorf("out-of-range cursor should land on the last line, got %d %q", line, text)
	}
	long := "SELECT '" + strings.Repeat("é", 300) + "';"
	_, capped := locateInScript(long, 3)
	if !strings.HasSuffix(capped, "…") || len([]rune(capped)) != 161 {
		t.Errorf("long line not rune-capped: %d runes", len([]rune(capped)))
	}
}
