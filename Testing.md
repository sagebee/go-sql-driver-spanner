# Testing setup 

To run tests, a spanner instnace must be up & running. The tests also require a test database, they will create and destroy objects in the database. Environment variables are used to point the script to the appropriate database and spanner configuration.

<br>

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

Set credentials 
```
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/file.json
```

Set Project

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
* SPANNER_TEST_DBID


*TODO concurrency, auth*
