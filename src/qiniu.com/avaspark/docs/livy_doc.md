# Spark Job Submit
## Livy Api
### baseurl:61.153.154.154:8998
#### POST /batches
```json
Request Body
{
    "file":"/pi.py",
    "args":["5"]
}
```
###
| name           | description                                       | type            |
|:---------------|:--------------------------------------------------|:----------------|
| file           | File containing the application to execute        | path (required) |
| args           | Command line arguments for the application        | list of strings |
| pyFiles        | Python files to be used in this session           | List of string  |
| driverMemory   | Amount of memory to use for the driver process    | string          |
| driverCores    | Number of cores to use for the driver process     | int             |
| executorMemory | Amount of memory to use per executor process      | string          |
| executorCores  | Number of cores to use for each executor          | int             |
| numExecutors   | Number of executors to launch for this session    | int             |
| queue          | The name of the YARN queue to which submitted     | string          |
| name           | The name of this session                          | string          |
```json
Response
{
  "id": 8,
  "state": "starting",
  "appId": null,
  "appInfo": {
    "driverLogUrl": null,
    "sparkUiUrl": null
  },
  "log": []
}
```
#### GET /batches/<batch id>
```json
Response
{
  "id": 12,
  "state": "running",
  "appId": "application_1490776155737_0228",
  "appInfo": {
    "driverLogUrl": "http://10-131-29-201:8042/node/containerlogs/container_1490776155737_0228_01_000001/root",
    "sparkUiUrl": "http://10-131-29-218:8088/proxy/application_1490776155737_0228/"
  },
  "log": [
    "\t diagnostics: N/A",
    "\t ApplicationMaster host: N/A",
    "\t ApplicationMaster RPC port: -1",
    "\t queue: default",
    "\t start time: 1491993452730",
    "\t final status: UNDEFINED",
    "\t tracking URL: http://10-131-29-218:8088/proxy/application_1490776155737_0228/",
    "\t user: root",
    "17/04/12 10:37:32 INFO util.ShutdownHookManager: Shutdown hook called",
    "17/04/12 10:37:32 INFO util.ShutdownHookManager: Deleting directory /tmp/spark-54f4bc4d-5cb8-4454-95fe-1462cd40c075"
  ]
}
```
#### [other livy api](https://github.com/cloudera/livy)

# Spark Admin and Monitor
- [Spark History Server](http://61.153.154.173:18080)
- [Yarn Applications](http://115.238.147.143:8088/cluster)