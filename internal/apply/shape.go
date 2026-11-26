package apply

// Write-shape summary of a delta's rendered SQL.
//
// An operator sizing a nightly re-enrichment needs to know what ONE entity
// upsert costs their replica: how many rows are written per table, and whether
// children are reconciled in place or wiped and re-inserted. The delta feed
// carries that answer verbatim — the SQL is rendered server-side and arrives
// here in full — but the apply log used to report delta counts only, and an
// acked delta is gone. Summarize reads the script the client is about to run
// and turns it into one line per batch (plus one per delta under --verbose).
//
// It is a REPORTING path: the summary never decides what is executed, so a
// mis-parsed statement costs a wrong log word, never a wrong write. That is
// what makes a hand-rolled scanner acceptable here instead of a SQL parser.
//
// On PostgreSQL the upsert count is a row count: the renderer emits one
// `INSERT … ON CONFLICT DO UPDATE` per projected row. On SQL Server the rows
// of one list or junction share a `MERGE` (up to 1000 rows each), counted as
// one. A reconciliation DELETE is counted as one statement — how many rows it
// actually prunes is by construction "whatever no longer belongs", which only
// the replica knows.
//
// One scanner reads both dialects: the T-SQL forms it knows (`[name]`, N'…',
// MERGE, a leading IF, EXEC(N'…'), GO lines) cannot be mistaken for anything
// the PostgreSQL renderer writes.

import (
	"fmt"
	"sort"
	"strings"
)

// opKind is the shape of one statement, as an operator reads it.
type opKind int

const (
	opUpsert opKind = iota // INSERT … ON CONFLICT (one row), MERGE (one or more)
	opInsert               // plain INSERT — one row
	opUpdate               // UPDATE
	opPrune                // DELETE guarded by _sync_revision — reconciliation
	opDelete               // scoped DELETE — a tombstone or an owned cascade
	opWipe                 // DELETE with no WHERE — wholesale replace
	opDDL                  // CREATE / ALTER / DROP
	opOther
	numOpKinds
)

// label renders one kind with its count, pluralized.
func (k opKind) label(n int) string {
	var one, many string
	switch k {
	case opUpsert:
		one, many = "upsert", "upserts"
	case opInsert:
		one, many = "insert", "inserts"
	case opUpdate:
		one, many = "update", "updates"
	case opPrune:
		one, many = "prune", "prunes"
	case opDelete:
		one, many = "delete", "deletes"
	case opWipe:
		one, many = "wipe", "wipes"
	case opDDL:
		one, many = "ddl", "ddl"
	default:
		one, many = "statement", "statements"
	}
	if n == 1 {
		return fmt.Sprintf("%d %s", n, one)
	}
	return fmt.Sprintf("%d %s", n, many)
}

// TableOps counts one table's statements by kind.
type TableOps struct {
	Table  string
	Counts [numOpKinds]int
	Total  int
}

func (t *TableOps) String() string {
	parts := make([]string, 0, 3)
	for k := opKind(0); k < numOpKinds; k++ {
		if t.Counts[k] > 0 {
			parts = append(parts, k.label(t.Counts[k]))
		}
	}
	return t.Table + " " + strings.Join(parts, " + ")
}

// Shape is the per-table roll-up of one script or of a whole applied window.
// Tables keep first-appearance order: emission order is meaningful (target
// before referrer, owner before owned child), so the log reads like the write.
type Shape struct {
	Statements int
	order      []string
	byTable    map[string]*TableOps
}

// ensure returns the roll-up for one table, creating it in emission order.
func (s *Shape) ensure(table string) *TableOps {
	if s.byTable == nil {
		s.byTable = map[string]*TableOps{}
	}
	t := s.byTable[table]
	if t == nil {
		t = &TableOps{Table: table}
		s.byTable[table] = t
		s.order = append(s.order, table)
	}
	return t
}

func (s *Shape) add(table string, kind opKind) {
	s.Statements++
	if table == "" {
		return
	}
	t := s.ensure(table)
	t.Counts[kind]++
	t.Total++
}

// Merge folds another shape in (batch roll-up over its deltas).
func (s *Shape) Merge(other Shape) {
	s.Statements += other.Statements
	for _, name := range other.order {
		o := other.byTable[name]
		t := s.ensure(name)
		for k := range o.Counts {
			t.Counts[k] += o.Counts[k]
		}
		t.Total += o.Total
	}
}

// Tables returns the per-table roll-ups in emission order.
func (s *Shape) Tables() []*TableOps {
	out := make([]*TableOps, 0, len(s.order))
	for _, name := range s.order {
		out = append(out, s.byTable[name])
	}
	return out
}

// Summary renders the shape as one log fragment. maxTables caps the list —
// the busiest tables win the space and the rest collapse into a count — while
// 0 lists everything (--verbose).
func (s *Shape) Summary(maxTables int) string {
	head := fmt.Sprintf("%d statement(s)", s.Statements)
	if len(s.order) == 0 {
		return head
	}
	tables := s.Tables()
	hidden := 0
	if maxTables > 0 && len(tables) > maxTables {
		// Rank by write volume, then keep the survivors in emission order so
		// the line still reads target-before-referrer.
		ranked := append([]*TableOps(nil), tables...)
		sort.SliceStable(ranked, func(i, j int) bool { return ranked[i].Total > ranked[j].Total })
		keep := map[string]bool{}
		for _, t := range ranked[:maxTables] {
			keep[t.Table] = true
		}
		kept := tables[:0:0]
		for _, t := range tables {
			if keep[t.Table] {
				kept = append(kept, t)
			}
		}
		hidden = len(tables) - len(kept)
		tables = kept
	}
	parts := make([]string, 0, len(tables))
	for _, t := range tables {
		parts = append(parts, t.String())
	}
	out := head + ": " + strings.Join(parts, ", ")
	if hidden > 0 {
		out += fmt.Sprintf(" (+%d more table(s))", hidden)
	}
	return out
}

// Summarize reads one rendered script and reports its write shape.
func Summarize(sqlText string) Shape {
	var s Shape
	for _, stmt := range splitStatements(sqlText) {
		if sessionSetup(stmt) {
			continue
		}
		kind, table := classify(stmt)
		s.add(table, kind)
	}
	return s
}

// sessionSetup reports a statement that writes nothing and opens every T-SQL
// unit the server renders — `SET XACT_ABORT ON`, `SET QUOTED_IDENTIFIER ON`,
// the snapshot's `EXEC sp_set_session_context …` step counter. Counting them
// would add the same constant to every delta's statement count.
func sessionSetup(stmt string) bool {
	w := words(stmt, 4)
	switch {
	case len(w) == 3 && strings.EqualFold(w[0], "SET"):
		return strings.EqualFold(w[2], "ON") || strings.EqualFold(w[2], "OFF")
	case len(w) >= 2 && strings.EqualFold(w[0], "EXEC"):
		return strings.EqualFold(w[1], "sp_set_session_context")
	}
	return false
}

// === scanning ================================================================

// splitStatements cuts a script on top-level semicolons and on GO lines (the
// batch separator of a SQL Server snapshot), stepping over string literals,
// quoted identifiers, dollar-quoted bodies and comments. Backslash is NOT an
// escape inside a literal (PostgreSQL's standard_conforming_strings is on,
// and T-SQL has no such escape).
func splitStatements(sqlText string) []string {
	var out []string
	start := 0
	i := 0
	n := len(sqlText)
	emit := func(end int) {
		if stmt := strings.TrimSpace(sqlText[start:end]); stmt != "" {
			out = append(out, stmt)
		}
	}
	for i < n {
		if i == 0 || sqlText[i-1] == '\n' {
			// Outside a literal, a line holding only GO ends the statement.
			eol := n
			if j := strings.IndexByte(sqlText[i:], '\n'); j >= 0 {
				eol = i + j
			}
			if isGoLine(sqlText[i:eol]) {
				emit(i)
				i = min(eol+1, n)
				start = i
				continue
			}
		}
		switch c := sqlText[i]; {
		case c == '\'', c == '"':
			i = skipQuoted(sqlText, i, c)
		case c == '$':
			i = skipDollar(sqlText, i)
		case c == '-' && i+1 < n && sqlText[i+1] == '-':
			if j := strings.IndexByte(sqlText[i:], '\n'); j < 0 {
				i = n
			} else {
				i += j + 1
			}
		case c == '/' && i+1 < n && sqlText[i+1] == '*':
			i = skipBlockComment(sqlText, i)
		case c == ';':
			emit(i)
			i++
			start = i
		default:
			i++
		}
	}
	emit(n)
	return out
}

// skipQuoted returns the index just past the literal/identifier opening at i,
// honoring the doubled-quote escape (” "" “).
func skipQuoted(s string, i int, q byte) int {
	for j := i + 1; j < len(s); j++ {
		if s[j] != q {
			continue
		}
		if j+1 < len(s) && s[j+1] == q {
			j++ // doubled quote: stays inside
			continue
		}
		return j + 1
	}
	return len(s)
}

// skipDollar returns the index just past a dollar-quoted body ($tag$…$tag$),
// or i+1 when the `$` opens no valid tag (a `$1` placeholder, a lone `$`).
func skipDollar(s string, i int) int {
	j := i + 1
	for j < len(s) && (s[j] == '_' ||
		(s[j] >= 'a' && s[j] <= 'z') || (s[j] >= 'A' && s[j] <= 'Z') ||
		(j > i+1 && s[j] >= '0' && s[j] <= '9')) {
		j++
	}
	if j >= len(s) || s[j] != '$' {
		return i + 1
	}
	tag := s[i : j+1]
	if end := strings.Index(s[j+1:], tag); end >= 0 {
		return j + 1 + end + len(tag)
	}
	return len(s)
}

// dollarBody returns the text inside a statement's first dollar-quoted body
// ($tag$ … $tag$), or "" when there is none.
func dollarBody(stmt string) string {
	for i := 0; i < len(stmt); i++ {
		if stmt[i] != '$' {
			continue
		}
		end := skipDollar(stmt, i)
		if end == i+1 {
			continue // `$1` placeholder or a lone `$`
		}
		tagLen := strings.IndexByte(stmt[i+1:], '$') + 2
		if end-tagLen < i+tagLen {
			return ""
		}
		return stmt[i+tagLen : end-tagLen]
	}
	return ""
}

// skipBlockComment returns the index just past /* … */, counting nesting the
// way PostgreSQL does.
func skipBlockComment(s string, i int) int {
	depth := 0
	for j := i; j < len(s)-1; j++ {
		switch {
		case s[j] == '/' && s[j+1] == '*':
			depth++
			j++
		case s[j] == '*' && s[j+1] == '/':
			depth--
			j++
			if depth == 0 {
				return j + 1
			}
		}
	}
	return len(s)
}

// classify names one statement's shape and the table it writes.
func classify(stmt string) (opKind, string) {
	return classifyDepth(stmt, 0)
}

// actionKeywords are the statement heads worth reporting; anything else is
// scaffolding (BEGIN, IF … THEN, EXCEPTION, DECLARE) or noise (SET, COMMENT).
// EXEC is the T-SQL word only: PL/pgSQL spells its dynamic SQL EXECUTE, which
// stays scaffolding.
var actionKeywords = map[string]bool{
	"INSERT": true, "DELETE": true, "UPDATE": true, "MERGE": true,
	"CREATE": true, "ALTER": true, "DROP": true, "EXEC": true,
}

// classifyDepth classifies one statement, stepping INTO the guarded DDL blocks
// the projection emits (`DO $ee_mig$ BEGIN ALTER TABLE … EXCEPTION … END` for
// PostgreSQL, `EXEC(N'IF … RETURN; … ALTER TABLE …')` for SQL Server). The
// block stays one statement — reporting the ALTER it guards is the whole
// point of the line.
func classifyDepth(stmt string, depth int) (opKind, string) {
	// 12 tokens is the widest prefix a target can hide behind:
	// `CREATE UNIQUE INDEX IF NOT EXISTS <name> ON <table>`.
	w := words(stmt, 12)
	if len(w) == 0 {
		return opOther, ""
	}
	if (depth > 0 || strings.EqualFold(w[0], "IF")) && !actionKeywords[strings.ToUpper(w[0])] {
		// Inside a block, or behind a T-SQL guard (`IF OBJECT_ID(…) IS NULL
		// CREATE TABLE …`): skip the scaffolding to the guarded action.
		all := words(stmt, 0)
		for i, tok := range all {
			if actionKeywords[strings.ToUpper(tok)] {
				w = all[i:min(i+12, len(all))]
				break
			}
		}
	}
	upper := strings.ToUpper(w[0])
	if (upper == "DO" || upper == "EXEC") && depth < 2 {
		body := dollarBody(stmt)
		if upper == "EXEC" {
			body = execBody(stmt)
		}
		for _, inner := range splitStatements(body) {
			if kind, table := classifyDepth(inner, depth+1); kind != opOther {
				return kind, table
			}
		}
		return opOther, ""
	}
	switch upper {
	case "INSERT":
		table := after(w, "INTO")
		if strings.Contains(strings.ToUpper(stmt), "ON CONFLICT") {
			return opUpsert, table
		}
		return opInsert, table
	case "MERGE":
		return opUpsert, after(w, "INTO")
	case "DELETE":
		table := writeTarget(stmt, afterRaw(w, "FROM"))
		switch {
		case !hasWord(stmt, "WHERE"):
			// Only a keyless parent's children are replaced wholesale.
			return opWipe, table
		case strings.Contains(stmt, "_sync_revision"):
			// Every reconciliation cleanup the projection renders carries the
			// revision guard; a tombstone or an owned cascade never does.
			return opPrune, table
		default:
			return opDelete, table
		}
	case "UPDATE":
		if len(w) < 2 {
			return opUpdate, ""
		}
		return opUpdate, writeTarget(stmt, w[1])
	case "CREATE", "ALTER", "DROP":
		return opDDL, ddlTarget(w)
	default:
		return opOther, ""
	}
}

// ddlTarget picks the table a DDL statement acts on: the word after TABLE, or
// the one after ON for `CREATE [UNIQUE] INDEX … ON <table>` (an index name is not
// what an operator tracks). Everything else (extensions, DO blocks) has
// no table.
func ddlTarget(w []string) string {
	if t := after(w, "TABLE"); t != "" {
		return t
	}
	return after(w, "ON")
}

// after returns the identifier following the given keyword, skipping the
// noise words that may sit between (IF NOT EXISTS, CONCURRENTLY, ONLY…).
func after(w []string, keyword string) string {
	return ident(afterRaw(w, keyword))
}

// afterRaw is after with the identifier as it is written, quoting included.
func afterRaw(w []string, keyword string) string {
	for i, tok := range w {
		if !strings.EqualFold(tok, keyword) {
			continue
		}
		for j := i + 1; j < len(w); j++ {
			switch strings.ToUpper(w[j]) {
			case "IF", "NOT", "EXISTS", "ONLY", "CONCURRENTLY":
				continue
			}
			return w[j]
		}
	}
	return ""
}

// writeTarget names the table an UPDATE or a DELETE writes. T-SQL lets them
// name an alias (`UPDATE r SET … FROM [note] AS r`, `DELETE r FROM [links] AS
// r`) or a CTE (`WITH s AS (SELECT … FROM [t]) DELETE FROM s`): a bare target
// is looked up, and reported as it is when the statement declares no such
// alias. The renderers always quote a table name, so a quoted target is one.
func writeTarget(stmt, target string) string {
	if target == "" || strings.ContainsAny(target, `"[.`) {
		return ident(target)
	}
	all := words(stmt, 0)
	for i := 2; i < len(all); i++ {
		switch {
		case strings.EqualFold(all[i], target) && strings.EqualFold(all[i-1], "AS") && i >= 3 &&
			(strings.EqualFold(all[i-3], "FROM") || strings.EqualFold(all[i-3], "JOIN")):
			return ident(all[i-2])
		case strings.EqualFold(all[i], "AS") && strings.EqualFold(all[i-1], target) &&
			strings.EqualFold(all[i-2], "WITH"):
			return after(all[i:], "FROM")
		}
	}
	return target
}

// execBody returns the script a statement's first `EXEC(N'…')` runs, quotes
// un-doubled, or "" when its EXEC runs something else (a procedure, a
// variable).
func execBody(stmt string) string {
	skipSpace := func(i int) int {
		for i < len(stmt) && strings.IndexByte(" \t\r\n", stmt[i]) >= 0 {
			i++
		}
		return i
	}
	for _, t := range tokens(stmt, 0) {
		if !strings.EqualFold(t.text, "EXEC") {
			continue
		}
		i := skipSpace(t.pos + len(t.text))
		if i >= len(stmt) || stmt[i] != '(' {
			return ""
		}
		i = skipSpace(i + 1)
		if i < len(stmt) && (stmt[i] == 'N' || stmt[i] == 'n') {
			i++
		}
		if i >= len(stmt) || stmt[i] != '\'' {
			return ""
		}
		body := strings.TrimSuffix(stmt[i+1:skipQuoted(stmt, i, '\'')], "'")
		return strings.ReplaceAll(body, "''", "'")
	}
	return ""
}

// hasWord reports a standalone keyword outside literals — `WHERE` in a value
// must not turn a wholesale DELETE into a prune.
func hasWord(stmt, keyword string) bool {
	for _, tok := range words(stmt, 0) {
		if strings.EqualFold(tok, keyword) {
			return true
		}
	}
	return false
}

// words tokenizes a statement outside literals and comments, stopping after
// max tokens (0 = all). Quoted identifiers survive as one token, quotes
// included, so `"my table"` and `[my table]` do not split.
func words(stmt string, max int) []string {
	toks := tokens(stmt, max)
	out := make([]string, len(toks))
	for i, t := range toks {
		out[i] = t.text
	}
	return out
}

// token is one word of a statement and where it starts.
type token struct {
	text string
	pos  int
}

func tokens(stmt string, max int) []token {
	var out []token
	i, n := 0, len(stmt)
	for i < n {
		c := stmt[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == ',' || c == '(' || c == ')':
			i++
		case c == '\'':
			out = append(out, token{"''", i}) // literal placeholder: never a keyword
			i = skipQuoted(stmt, i, '\'')
		case (c == 'N' || c == 'n') && i+1 < n && stmt[i+1] == '\'':
			out = append(out, token{"''", i}) // T-SQL national literal N'…'
			i = skipQuoted(stmt, i+1, '\'')
		case c == '"':
			j := skipQuoted(stmt, i, '"')
			out = append(out, token{stmt[i:j], i})
			i = j
		case c == '$':
			j := skipDollar(stmt, i)
			if j > i+1 {
				out = append(out, token{"''", i})
			}
			i = j
		case c == '-' && i+1 < n && stmt[i+1] == '-':
			if j := strings.IndexByte(stmt[i:], '\n'); j < 0 {
				i = n
			} else {
				i += j + 1
			}
		case c == '/' && i+1 < n && stmt[i+1] == '*':
			i = skipBlockComment(stmt, i)
		default:
			j := i
			for j < n && !strings.ContainsRune(" \t\n\r,()", rune(stmt[j])) {
				// A T-SQL bracket name is one piece of the token wherever it
				// opens a name — at the token's start or after a qualifier dot
				// (`[dbo].[my table]`, `t.[k]`). Anywhere else `[` is an
				// ordinary character: PostgreSQL writes `ARRAY['a']::text[]`.
				if stmt[j] == '[' && (j == i || stmt[j-1] == '.') {
					j = skipBracket(stmt, j)
					continue
				}
				j++
			}
			out = append(out, token{stmt[i:j], i})
			i = j
		}
		if max > 0 && len(out) >= max {
			break
		}
	}
	return out
}

// skipBracket returns the index just past the T-SQL bracket name opening at i
// (`]]` is an escaped bracket), or i+1 when nothing closes it.
func skipBracket(s string, i int) int {
	for j := i + 1; j < len(s); j++ {
		if s[j] != ']' {
			continue
		}
		if j+1 < len(s) && s[j+1] == ']' {
			j++
			continue
		}
		return j + 1
	}
	return i + 1
}

// ident strips quoting and any schema qualifier: the operator reads table
// names, not physical addresses.
func ident(tok string) string {
	// A qualifier dot inside quotes ("my.table", [my.table]) is part of the
	// name: only a dot between the parts cuts.
	last := 0
	for i := 0; i < len(tok); i++ {
		switch {
		case tok[i] == '"':
			i = skipQuoted(tok, i, '"') - 1
		case tok[i] == '[' && i == last:
			i = skipBracket(tok, i) - 1
		case tok[i] == '.' && i < len(tok)-1:
			last = i + 1
		}
	}
	tok = tok[last:]
	if len(tok) > 1 && tok[0] == '[' && tok[len(tok)-1] == ']' {
		return strings.ReplaceAll(tok[1:len(tok)-1], "]]", "]")
	}
	return strings.Trim(tok, `"`)
}
