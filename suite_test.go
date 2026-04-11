package pgxcursor_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestPgxcursor(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pgxcursor Suite")
}
