# Testing setup 

To run tests, a spanner instnace must be up & running.

## Emulator 

Cloud spanner emulator doc: https://cloud.google.com/spanner/docs/emulator

start emulator 
```
gcloud beta emulators spanner start
```


in working shell, set emulator host environment variable

```
export SPANNER_EMULATOR_HOST=0.0.0.0:9010
```

<br>

## Cloud 

*TODO*

<br>

## Environment variables 

Before running, set appropriate environmant variables, e.g.: 

### Unix

```
$ export SPANNER_TEST_PROJECT="name"
```

### Windows, probably 

```
$ set SPANNER_TEST_PROJECT="name"
```

Variable list:
* SPANNER_TEST_PROJECT
* SPANNER_TEST_INSTANCE
* TSPANNER_TEST_DBNAME


*TODO concurrency, auth*
