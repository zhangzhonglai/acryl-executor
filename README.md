# Acryl Executor

Remote execution agent used for running DataHub tasks, such as ingestion powered through the UI. 


```bash
python3 -m venv --upgrade-deps venv
source venv/bin/activate
pip3 install .
```

## Notes

By default, this library comes with a set of default task implementations: 

### RUN_INGEST Task

- **SubprocessProcessIngestionTask** - Executes a metadata ingestion run by spinning off a subprocess. Supports ingesting from a particular version, and with a specific plugin (based on the platform type requested) 

- **InMemoryIngestionTask** - Executes a metadata ingestion run using the datahub library in the same process. Not great for production where we can see strange dependency conflicts when certain packages are executed together. Use this for testing, as it has no ability to check out a specific DataHub Metadata Ingestion plugin. 
