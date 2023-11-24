# Remove unused KsqlDB Streams

## Update .env
Set authentication details in `.env` file

## Execute script

```bash
python2 main.py > drop_unused_streams.sql 2>/dev/null
```

## Delete stream

### Review the content of `drop_unused_streams.sql`

### Drop unused streams
```bash
ksql
```

```sql
RUN SCRIPT drop_unused_streams.sql;
```
