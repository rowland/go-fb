package fb

import (
	"testing"
	"fmt"
	"strings"
	"time"
)

func genI(i int) int32 {
  return int32(i)
}

func genSi(i int) int16 {
  return int16(i)
}

func genBi(i int) int64 {
  return int64(i) * 1000000000
}

func genF(i int) float32 {
  return float32(i) / 2
}

func genD(i int) float64 {
  return float64(i) * 3333 / 2
}

func genC(i int) string {
  return fmt.Sprintf("%c", i + 64)
}

func genC10(i int) string {
  return strings.Repeat(genC(i), 5)
}

func genVc(i int) string {
  return genC(i)
}

func genVc10(i int) string {
  return strings.Repeat(genC(i), i)
}

func genVc10000(i int) string {
  return strings.Repeat(genC(i), i * 1000)
}

func genDt(i int) time.Time {
	return time.Time{Year:2000, Month:i+1, Day:i+1}
}

func genTm(i int) time.Time {
  return time.Time{Year:1990, Month:1, Day:1, Hour:12, Minute:i, Second:i}
}

func genTs(i int) time.Time {
  return time.Time{Year:2006, Month:1, Day:1, Hour:i, Minute:i, Second:i}
}

func genN92(i int) float64 {
  return float64(i) * 100
}

func genD92(i int) float64 {
  return float64(i) * 100
}

func TestGenI(t *testing.T) {
	if genI(3) != 3 {
		t.Errorf("Expected: %d, got: %d", 3, genI(3))
	}
}

func TestGenSi(t *testing.T) {
	if genSi(3) != 3 {
		t.Errorf("Expected: %d, got: %d", 3, genSi(3))
	}
}

func TestGenBi(t *testing.T) {
	if genBi(3) != 3 * 1000000000 {
		t.Errorf("Expected: %d, got: %d", int64(3) * 1000000000, genBi(3))
	}
}

func TestGenF(t *testing.T) {
	if genF(3) != 1.5 {
		t.Errorf("Expected: %f, got: %f", 1.5, genF(3))
	}
}

func TestGenD(t *testing.T) {
	if genD(3) != 4999.5 {
		t.Errorf("Expected: %f, got: %f", 4999.5, genD(3))
	}
}
