[Back to index](./README.md)

# Operator Precedence

This page lists operator precedence used by the parser and evaluator (highest precedence at top).

1. Parentheses `(...)`
2. Unary operators: `-` (negation), `+` (unary plus), `NOT`
3. Multiplicative: `*`, `/`, `%`
4. Additive: `+`, `-`
5. Comparison: `<`, `<=`, `>`, `>=`
6. Equality: `=`, `!=`, `<>`
7. Membership & pattern: `BETWEEN`, `IN`, `LIKE`, `RLIKE`
8. Logical `AND`
9. Logical `OR`

**Notes and examples**
```sql
SELECT 1 + 2 * 3 AS v; -- v = 7
SELECT (1 + 2) * 3 AS v; -- v = 9
SELECT a BETWEEN 1 AND 3 OR b = 5; -- interpreted as (a BETWEEN 1 AND 3) OR (b = 5)
```

[Back to index](./README.md)
