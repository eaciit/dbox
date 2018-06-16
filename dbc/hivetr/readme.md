For object to be saved with this dbc it need to implement HiveRow interface. This is to make sure the order of field is same each time the object turned to string.

```Go
type Company1 struct {
	Id           int
	Name         string
	Employee_num int
}

func (c *Company1) FieldOrder() []string {
	return []string{"Id", "Name", "Employee_num"}
}
```