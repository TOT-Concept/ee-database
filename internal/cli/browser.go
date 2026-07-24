package cli

import (
	"os/exec"
	"runtime"
)

// openInBrowser tries to open url in the user's default browser. Best-effort:
// returns nil even when the underlying program fails so the CLI can fall back
// to printing the URL for the user to open manually.
func openInBrowser(url string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default: // linux, *bsd
		cmd = exec.Command("xdg-open", url)
	}
	return cmd.Start()
}
