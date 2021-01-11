
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
Case | Behavior 
--- | --- 
STRING | 
INT64 |
BYTES |
BOOL |
FLOAT64 |
NaN |
+inf |
-inf |
" quotes around str |
' quotes around str |

<br>
<br>

Overflow tests
Case | Behavior 
--- | --- 
Read too large string | 
Write too large string |
Read too large bytes | 
Write too large bytes |
Read too large int |
Write too large int |
Read too large float |
Write too large float |

<br>
<br>

Inner Join tests
Case | Behavior 
--- | --- 
Returns nothing | 
Returns tuples from first table |
Returns tuples from second table |
Returns tuples from both tables | 


<br>
<br>

views?


<br>
<br>

## DML Tests

Insert tests 
Case | Behavior
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
Case | Behavior
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

Testa

A | B | C
--- | --- | ---
a1 |  b1 | c1
a2 |  b2 | c2
a3 |  b3 | c3

<br>

TypeTesta

stringt | bytest | intt | floatt | boolt
--- | --- | --- | --- | ---
