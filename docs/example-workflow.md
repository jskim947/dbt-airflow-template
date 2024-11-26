# Example DBT Workflow Guide

This guide walks you through running and fixing the example dbt
workflow that comes with this template.

## Understanding the Example Workflow

The example workflow (`example_dbt_dag.py`) includes several dbt
tasks:
- `dbt debug`: Validates your dbt configuration
- `dbt deps`: Installs dbt dependencies
- `dbt run`: Runs the example models
- `dbt test`: Tests the example models
- `dbt source test`: Tests any defined sources

## Initial Run and Expected Failure

When you first run the example workflow, it will fail at the `dbt
test` step. This is expected because the example models include a
deliberate data quality issue to demonstrate dbt's testing
capabilities.

### Viewing the Failure

1. In the Airflow UI (http://localhost:18080), navigate to DAGs
2. Find and enable the `example_dbt_dag`
3. Click "Trigger DAG"
4. The DAG will fail at the `dbt_test` task

### Understanding the Failure

The failure occurs because:

1. `my_first_dbt_model.sql` deliberately includes a null ID:
```sql
with source_data as (
    select 1 as id
    union all
    select null as id
)
```

2. The schema tests in `models/example/schema.yml` require that the
   `id` column be:
   - Unique
   - Not null

## Fixing the Tests

The example includes a commented solution in
`my_first_dbt_model.sql`. To fix the tests:

1. Open `airflow/dbt/models/example/my_first_dbt_model.sql`
2. Uncomment the line at the bottom:
```sql
-- where id is not null
```
to:
```sql
where id is not null
```

This change filters out the null ID value, satisfying the `not_null`
test in the schema.

## Rerunning the Workflow

1. Clear the failed task in Airflow:
   - Go to Tree View
   - Right-click on the failed task
   - Select "Clear"
   - Choose "Clear" in the confirmation dialog

2. The DAG will automatically retry, or you can trigger it manually

3. All tasks should now complete successfully

## Verifying the Fix

After the workflow succeeds:

1. `my_first_dbt_model` will only contain the row with `id = 1`
2. `my_second_dbt_model` will contain the same row, as it selects
   `where id = 1`
3. All schema tests will pass because:
   - No null values exist in the `id` column
   - The single value is naturally unique

## Next Steps

This example demonstrates:
- How dbt tests work
- The relationship between model SQL and schema tests
- Basic data quality enforcement
- The workflow for fixing and rerunning failed tests

You can now:
1. Examine the models and schema tests in more detail
2. Try adding your own models and tests
3. Experiment with different schema test configurations

## Troubleshooting

If you continue to experience failures:

1. Check the task logs in Airflow UI for detailed error messages

2. Run dbt commands directly using the development environment:
```bash
# First, connect to the scheduler container
docker exec -it dbt-airflow-scheduler-1 bash

# Then run dbt commands
cd dbt
dbt debug
dbt test
```

3. Verify your PostgreSQL connection:
```bash
# From scheduler container
psql -h postgres -U airflow -d airflow
```

Remember to check the logs in `airflow/logs/` for detailed error messages and debugging information.
