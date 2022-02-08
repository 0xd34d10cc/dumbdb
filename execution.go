package dumbdb

import "context"

func FullScan(ctx context.Context, table *Table, filter func(Row) bool, project func(Row) Row) <-chan Row {
	c := make(chan Row, 16)
	done := ctx.Done()
	go func() {
		// TODO: handle error returned by Scan()
		table.Scan(func(r Row) error {
			select {
			case <-done:
				return ctx.Err()
			default:
			}

			if !filter(r) {
				return nil
			}

			select {
			case c <- project(r):
				return nil
			case <-done:
				return ctx.Err()
			}
		})
		close(c)
	}()

	return c
}
