package bundle

import (
	"fmt"
	"time"
)

// formatTimeDistance formats a duration as "X ago" or "live"
func formatTimeDistance(d time.Duration) string {
	if d < 10*time.Second {
		return "live"
	}
	if d < time.Minute {
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	}
	if d < time.Hour {
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	}
	if d < 24*time.Hour {
		hours := int(d.Hours())
		mins := int(d.Minutes()) % 60
		if mins > 0 {
			return fmt.Sprintf("%dh%dm ago", hours, mins)
		}
		return fmt.Sprintf("%dh ago", hours)
	}
	days := int(d.Hours() / 24)
	if days == 1 {
		return "1 day ago"
	}
	if days < 7 {
		return fmt.Sprintf("%d days ago", days)
	}
	weeks := days / 7
	if weeks < 4 {
		return fmt.Sprintf("%d weeks ago", weeks)
	}
	months := days / 30
	if months < 12 {
		return fmt.Sprintf("%d months ago", months)
	}
	return fmt.Sprintf("%.1f years ago", float64(days)/365)
}
