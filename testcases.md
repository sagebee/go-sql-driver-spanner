
# Test Cases

<br>  

## SQL Tests

Standard tests 

Case | Behavior 
--- | --- 
Empty Query | Returns 0 rows, err is nil, prints spanner error message to stdout (not stderr)
Query with syntax error | Returns 0 rows, err is nil, prints spanner error message to stdout (not stderr)
Query that should return nothing | Returns nothing 
Query that returns one tuple | Returns expected tuple 
Query that returns multiple tupes | Returns expected tuples
Query that gets entire table | Returns expected tuples
Query subset of tupples | Returns expected tuples

<br>
<br>

Type tests 
Case | Done? 
--- | --- 
STRING | 
INT64 |
BYTES |
BOOL |
FLOAT64 |
" quotes around str |
' quotes around str |


<br>
<br>

## DML Tests

Insert tests 
Case | Done? 
--- | --- 
Correct signle tuple | 
Correct multiple tuples |
Insert with wrong types |
Primary key duplicate | 
Refferencual integrity violation |
Insert null into non null type |
Too many values | 
Too few values |


<br>
<br>

Delete tests
Case | Done? 
--- | --- 
Correct signle tuple | 
Correct multiple tuples |
Referential integrity violation |
Delete with no tupes |
Delete all tuples |


<br>
<br>

## DDL Tests 





## Transaction Tests


<br>
<br>

## Test Tables

<br>

testa

Delete tests
A | B | C
--- | --- | ---
a1 |  b1 | c1
a2 |  b2 | c2
a3 |  b3 | c3