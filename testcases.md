
# Test Cases

<br>  

## SQL Tests

<br>

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
Query non existant table | 
Query row function | 
Scan into nil pointer (db/sql)|
Cancel context while scanning rows (db/sql) | 

<br>
<br>

General Type tests 
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

Overflow Read tests
Case | Behavior 
--- | --- 
Read too large string | 
Read too large bytes | 
Read too large int |
Read too large float |


<br>
<br>

Null read tests
Case | Behavior 
--- | --- 
Read NULL into string | 
Read NULL into int |
Read NULL into float |
Read NULL into byte | 

<br>
<br>

Bad Conversion Read tests
Case | Behavior 
--- | --- 
Read string into int |
Read int into string |
Read int into float |
Read float into int |
Read bool into string |
Read int into bool |

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

## DML Tests

<br>

Insert tests 
Case | Behavior
--- | --- 
Correct signle tuple | 
Correct multiple tuples |
Insert with wrong types |
Primary key duplicate | 
Refferentual integrity violation cascade |
Refferential integrity violation no cascade |
Insert null into non null type |
Too many values | 
Too few values |

<br>
<br>

Overflow Write tests
Case | Behavior 
--- | --- 
Write too large string |
Write too large bytes |
Write too large int |
Write too large float |

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

<br>

Create tests 
Case | Behavior
--- | --- 
Create table with all types | 
Create table syntax error |
Create table that violates naming | 
Create table with no primary key | 

<br>
<br>

Drop tests 
Case | Behavior
--- | --- 
Drop table ok | 
Drop nonexistant table |
Drop table refferencial integrity violation no cascade |
Drop table refferencial integrity violation cascade |

<br>
<br>

Rename tests 
Case | Behavior
--- | --- 
Rename tablle ok |
Rename table to existing table |
Rename table to violate naming convention | 
Rename column ok | 
Rename column to existing column name |
Rename column to violate naming rules | 

<br>
<br>

Change tests
Case | Behavior
--- | --- 
Change string to int empty table |
Change string to int with data |
Other combinations | 

<br>
<br>

Truncate tests 
Case | Behavior
--- | --- 
Truncate table ok |
Truncate refferencial integrity violation no cascade |
Truncate refferencial integrity violation cascade |

<br>
<br>

## Transaction Tests

<br>

General 
Case | Behavior
--- | --- 
Prepare | 
Stmt | 
Use Stmt after close | 
Run query tests in transaction | 
Row implicit close (go/sql) | 
Commit after rollback | 
Query context after timeout |

<br>
<br> 

Rollback tests 
Case | Behavior
--- | ---  
Truncate with rollback | 
Delete with rollback |
Insert with rollback |
CRUD with rollback | 

<br>
<br>

## Miscelanious Tests

Case | Behavior
--- | --- 
Run unsupported options (should err) (db/sql) | 
CRUD (mysql) | 

<br>
<br>

<br>
<br>

todo? views, driver panic, concurrency, connection pool things, deadlock, timestamp/date

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
