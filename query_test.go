package main

import "testing"

func TestQuery(t *testing.T) {
	queries := [...]string{
		"create table users (id int, name varchar(20), age int)",

		"insert into users values (1, \"Hello\", 1337), (2, \"World\", 42)",

		"select * from users",
		"select id, name from users",
		"select id, name from users where id=1",
		"select id, name from users where id<100 and age>20",
		"select id, name from users where (id-2)*2 <= 42 or name!=\"kekus\"",

		"drop table users",
	}

	for _, query := range queries {
		_, err := ParseQuery(query)
		if err != nil {
			t.Fatalf("Failed to parse query %v: %v", query, err)
		}
	}
}
