package typedapi

import "fmt"

func ExampleNewEntry() {
	type User struct{ Name string }
	u := User{Name: "alice"}
	inline := NewEntry(StringKey("id"), u)
	external := NewEntry(StringKey("id"), &u)
	u.Name = "bob"
	fmt.Println(inline.Value().Name)
	fmt.Println(external.Value().Name, external.Value() == &u)
	// Output:
	// alice
	// bob true
}

func ExampleInt64Key() {
	k := Int64Key(-1)
	hash, conflict := k.KeyHash()
	fmt.Println(k, hash, conflict)
	// Output:
	// -1 18446744073709551615 0
}
