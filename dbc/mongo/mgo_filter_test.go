package mongo

import (
	"errors"
	db "github.com/eaciit/dbox"
	tk "github.com/eaciit/toolkit"
	"github.com/stretchr/testify/assert"
	"testing"
)

// private function to shorten db.Filter compilation
func _buildFilter(filter *db.Filter) (interface{}, error) {
	filterBuilder := db.NewFilterBuilder(new(FilterBuilder))
	filterBuilder.AddFilter(filter)
	return filterBuilder.Build()
}

// test simple build
func TestFilterBuilderInitializationAndBuild(t *testing.T) {
	filterBuilder := db.NewFilterBuilder(new(FilterBuilder))
	assert.NotNil(t, filterBuilder)

	filterBuilder.AddFilter(db.Eq("someField", "someValue"))
	filter, err := filterBuilder.Build()
	assert.NotNil(t, filter)
	assert.NoError(t, err)

	expected := tk.M{"someField": "someValue"}
	assert.Equal(t, expected, filter)
}

// test dbox.Eq()
func TestFilterEq(t *testing.T) {
	filter, err := _buildFilter(db.Eq("someField", "someValue"))
	assert.NoError(t, err)

	expected := tk.M{"someField": "someValue"}
	assert.Equal(t, expected, filter)
}

// test dbox.Gt()
func TestFilterGt(t *testing.T) {
	filter, err := _buildFilter(db.Gt("someField", 12))
	assert.NoError(t, err)

	expected := tk.M{"someField": tk.M{"$gt": 12}}
	assert.Equal(t, expected, filter)
}

// test dbox.Gte()
func TestFilterGte(t *testing.T) {
	filter, err := _buildFilter(db.Gte("someField", 13))
	assert.NoError(t, err)

	expected := tk.M{"someField": tk.M{"$gte": 13}}
	assert.Equal(t, expected, filter)
}

// test dbox.Lt()
func TestFilterLt(t *testing.T) {
	filter, err := _buildFilter(db.Lt("someField", 14))
	assert.NoError(t, err)

	expected := tk.M{"someField": tk.M{"$lt": 14}}
	assert.Equal(t, expected, filter)
}

// test dbox.Lte()
func TestFilterLte(t *testing.T) {
	filter, err := _buildFilter(db.Lte("someField", 15))
	assert.NoError(t, err)

	expected := tk.M{"someField": tk.M{"$lte": 15}}
	assert.Equal(t, expected, filter)
}

// test dbox.In()
func TestFilterIn(t *testing.T) {
	filter, err := _buildFilter(db.In("someField", "value1", "value2", "value3"))
	assert.NoError(t, err)

	expected := tk.M{"someField": tk.M{"$in": []interface{}{
		"value1", "value2", "value3",
	}}}
	assert.Equal(t, expected, filter)
}

// test dbox.Nin()
func TestFilterNin(t *testing.T) {
	filter, err := _buildFilter(db.Nin("someField", "value1", "value2", "value3"))
	assert.NoError(t, err)

	expected := tk.M{"someField": tk.M{"$nin": []interface{}{
		"value1", "value2", "value3",
	}}}
	assert.Equal(t, expected, filter)
}

// test dbox.Contains()
func TestFilterContains(t *testing.T) {
	filter, err := _buildFilter(db.Contains("someField", "some"))
	assert.NoError(t, err)

	expected := tk.M{"someField": tk.M{
		"$regex":   ".*some.*",
		"$options": "i",
	}}
	assert.Equal(t, expected, filter)
}

// test dbox.Contains()
func TestFilterContainsSlice(t *testing.T) {
	filter, err := _buildFilter(db.Contains("someField", "some", "value"))
	assert.NoError(t, err)

	// expected := tk.M{
	// 	"$or": []interface{}{
	// 		tk.M{"someField": tk.M{"$regex": ".*some.*", "$options": "i"}},
	// 		tk.M{"someField": tk.M{"$options": "i", "$regex": ".*value.*"}},
	// 	},
	// }

	for _, each := range filter.(tk.M)["$or"].([]interface{}) {
		field := each.(tk.M)["someField"]

		regex := field.(tk.M)["$regex"]
		options := field.(tk.M)["$options"]

		if !((regex == ".*some.*" || regex == ".*value.*") && options == "i") {
			assert.Error(t, errors.New("Invalid result"))
		}
	}
}

// test dbox.StartWith()
func TestFilterStartWith(t *testing.T) {
	filter, err := _buildFilter(db.Startwith("someField", "some"))
	assert.NoError(t, err)

	expected := tk.M{"someField": tk.M{
		"$regex":   "^some.*$",
		"$options": "i",
	}}
	assert.Equal(t, expected, filter)
}

// test dbox.Endwith()
func TestFilterEndwith(t *testing.T) {
	filter, err := _buildFilter(db.Endwith("someField", "some"))
	assert.NoError(t, err)

	expected := tk.M{"someField": tk.M{
		"$regex":   "^.*some$",
		"$options": "i",
	}}
	assert.Equal(t, expected, filter)
}

// test dbox.And()
func TestFilterAnd(t *testing.T) {
	filter, err := _buildFilter(db.And(
		db.Eq("fieldA", "jason"),
		db.Gte("fieldB", 12),
		db.Ne("fieldC", "richard"),
	))
	assert.NoError(t, err)

	expected := tk.M{"$and": []interface{}{
		tk.M{"fieldA": "jason"},
		tk.M{"fieldB": tk.M{"$gte": 12}},
		tk.M{"fieldC": tk.M{"$ne": "richard"}},
	}}
	assert.Equal(t, expected, filter)
}

// test dbox.Or()
func TestFilterOr(t *testing.T) {
	filter, err := _buildFilter(db.Or(
		db.Eq("fieldA", "jason"),
		db.Gte("fieldB", 12),
		db.Ne("fieldC", "richard"),
	))
	assert.NoError(t, err)

	expected := tk.M{"$or": []interface{}{
		tk.M{"fieldA": "jason"},
		tk.M{"fieldB": tk.M{"$gte": 12}},
		tk.M{"fieldC": tk.M{"$ne": "richard"}},
	}}
	assert.Equal(t, expected, filter)
}

// test dbox.And() and dbox.Or()
func TestFilterCombinationOfAndOr(t *testing.T) {
	filter, err := _buildFilter(db.Or(
		db.Eq("fieldA", "jason"),
		db.Gte("fieldB", 12),
		db.And(
			db.Eq("fieldC", "richard"),
			db.Gte("fieldD", 24),
		),
	))
	assert.NoError(t, err)

	expected := tk.M{"$or": []interface{}{
		tk.M{"fieldA": "jason"},
		tk.M{"fieldB": tk.M{"$gte": 12}},
		tk.M{"$and": []interface{}{
			tk.M{"fieldC": "richard"},
			tk.M{"fieldD": tk.M{"$gte": 24}},
		}},
	}}
	assert.Equal(t, expected, filter)
}

// test all filters
func TestFilterCombinationOfAll(t *testing.T) {
	filter, err := _buildFilter(db.Or(
		db.Eq("fieldA", "jason"),
		db.Gte("fieldB", 12),
		db.And(
			db.Ne("fieldC", "richard"),
			db.Or(
				db.Startwith("fieldD", "jason"),
				db.Endwith("fieldD", "todd"),
			),
			db.Lt("fieldE", 24),
			db.Lte("fieldF", 78),
		),
	))
	assert.NoError(t, err)

	expected := tk.M{"$or": []interface{}{
		tk.M{"fieldA": "jason"},
		tk.M{"fieldB": tk.M{"$gte": 12}},
		tk.M{"$and": []interface{}{
			tk.M{"fieldC": tk.M{"$ne": "richard"}},
			tk.M{"$or": []interface{}{
				tk.M{"fieldD": tk.M{"$regex": "^jason.*$", "$options": "i"}},
				tk.M{"fieldD": tk.M{"$regex": "^.*todd$", "$options": "i"}},
			}},
			tk.M{"fieldE": tk.M{"$lt": 24}},
			tk.M{"fieldF": tk.M{"$lte": 78}},
		}},
	}}
	assert.Equal(t, expected, filter)
}

func TestFilterUsingFakeFilter(t *testing.T) {
	filterFake := db.Filter{Field: "someField", Op: "apakah sama", Value: "some"}
	filter, err := _buildFilter(&filterFake)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "is not defined")
	assert.Nil(t, filter)
}

func TestParseFilterString(t *testing.T) {
	constructedFilter := db.ParseFilter("fieldA", "valueString", db.DataString, "")
	parsedFilter, err := _buildFilter(constructedFilter)

	expected := tk.M{"fieldA": "valueString"}
	assert.NoError(t, err)
	assert.Equal(t, expected, parsedFilter)
}

func TestCombineFilters(t *testing.T) {
	filterBuilder := db.NewFilterBuilder(new(FilterBuilder))
	parsedFilter, err := filterBuilder.CombineFilters([]interface{}{
		tk.M{"fieldA": "value"},
		tk.M{"fieldB": "value"},
	})

	expected := tk.M{"$and": []tk.M{
		tk.M{"fieldA": "value"},
		tk.M{"fieldB": "value"},
	}}
	assert.NoError(t, err)
	assert.Equal(t, expected, parsedFilter)
}

func TestCombineFiltersEmptySlice(t *testing.T) {
	filterBuilder := db.NewFilterBuilder(new(FilterBuilder))
	parsedFilter, err := filterBuilder.CombineFilters([]interface{}{})

	expected := tk.M{}
	assert.NoError(t, err)
	assert.Equal(t, expected, parsedFilter)
}
