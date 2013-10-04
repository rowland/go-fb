package fb

import (
	"fmt"
	"strings"
	"testing"
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
	return fmt.Sprintf("%c", i+64)
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
	return strings.Repeat(genC(i), i*1000)
}

func genDt(i int) time.Time {
	return time.Date(2000, time.Month(i+1), i+1, 0, 0, 0, 0, time.Local)
}

func genTm(i int) time.Time {
	return time.Date(1990, time.Month(1), 1, 12, i, i, 0, time.Local)
}

func genTs(i int) time.Time {
	return time.Date(2006, time.Month(1), 1, i, i, i, 0, time.Local)
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
	if genBi(3) != 3*1000000000 {
		t.Errorf("Expected: %d, got: %d", int64(3)*1000000000, genBi(3))
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
