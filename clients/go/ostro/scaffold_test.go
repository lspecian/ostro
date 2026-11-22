package ostro

import "testing"

func TestVersion(t *testing.T) {
	if Version != "0.1.0" {
		t.Fatalf("Version = %q, want %q", Version, "0.1.0")
	}
}
