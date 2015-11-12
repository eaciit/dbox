package mongo

import (
	"fmt"

	"github.com/eaciit/dbox"
	"github.com/eaciit/toolkit"
	"testing"
)

func TestFilter(t *testing.T) {
	fmt.Println("Build Filter")
	fb := dbox.NewFilterBuilder(new(FilterBuilder))
	fb.AddFilter(fb.Or(
		fb.Eq("_id", 1),
		fb.Eq("group", "administrators")))
	b, e := fb.Build()
	if e != nil {
		t.Errorf("Error %s", e.Error())
	} else {
		fmt.Printf("Result:\n%v\n", toolkit.JsonString(b))
	}
}
