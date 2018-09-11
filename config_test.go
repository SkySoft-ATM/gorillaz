package gorillaz

import (
	"bufio"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
)

func TestGetPropertiesKeys(t *testing.T) {
	propertiesFile :=
		`a=b
		b=c
		c=d`

	scanner := bufio.NewScanner(strings.NewReader(propertiesFile))
	keys := getPropertiesKeys(*scanner)
	assert.Equal(t, map[string]string(map[string]string{"a": "b", "b": "c", "c": "d"}),
		keys)
}
