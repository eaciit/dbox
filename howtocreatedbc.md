# How to create a Database Connector

Create go files for: connection, filterbuilder, query, cursor

## Connection
- Extend dbox.Connection
- Create init to register respective connection
- Implement: Connect, Close, NewQuery

## FilterBuilder
- Extend dbox.FilterBuilder
- Implement .BuildFilter to translate filter 
- Implement .CombineFilters to combine all builded filter into a filter query. Theoritically result of this will be part of Where clause on traditional ANSI SQL

## Query
- Extend dbox.Query
- Implement Cursor, Exec
- Implement Prepare if neccessary

## Cursor
- Extend dbox.Cursor
- Implement Fetch, Count, ResetFetch, Close
