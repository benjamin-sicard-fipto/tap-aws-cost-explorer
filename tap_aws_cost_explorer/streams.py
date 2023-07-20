"""Stream type classes for tap-aws-cost-explorer."""

import datetime
from pathlib import Path
from typing import Optional, Iterable

import pendulum
from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_aws_cost_explorer.client import AWSCostExplorerStream

class CostAndUsageWithResourcesStream(AWSCostExplorerStream):
    """Define custom stream."""
    name = "cost_and_usage"
    primary_keys = ["group_keys", "metric_name", "time_period_start"]
    replication_key = "time_period_start"
    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property("time_period_start", th.DateTimeType),
        th.Property("time_period_end", th.DateTimeType),
        th.Property("group_keys", th.ArrayType(th.StringType)),
        th.Property("metric_name", th.StringType),
        th.Property("amount", th.StringType),
        th.Property("amount_unit", th.StringType),
    ).to_dict()

    def _get_end_date(self):
        if self.config.get("end_date") is None:
            return datetime.datetime.today() - datetime.timedelta(days=1)
        return th.cast(datetime.datetime, pendulum.parse(self.config["end_date"]))

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects."""
        next_page_token = None
        start_date = self.get_starting_timestamp(context)
        end_date = self._get_end_date()

        while True:
            params = {
                'TimePeriod': {
                    'Start': start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    'End': end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
                },
                'Granularity': self.config.get("granularity"),
                'Metrics': self.config.get("metrics"),
                'Filter': {
                  'Not': {
                    'Dimensions': {
                      'Key': "RECORD_TYPE",
                      'Values': ["Credit" ]
                    }
                  }
                },
                'GroupBy': [
                    {
                        'Type': 'DIMENSION',
                        'Key': 'LINKED_ACCOUNT'
                    },
                    {
                        'Type': 'DIMENSION',
                        'Key': 'SERVICE'
                    },
                ]
            }

            if next_page_token:
                params['NextPageToken'] = next_page_token
            
            response = self.conn.get_cost_and_usage(**params)
            results_by_time = response.get("ResultsByTime", [])
            next_page_token = response.get("NextPageToken")


            for row in results_by_time:
              time_period = row.get("TimePeriod", {})
              groups = row.get("Groups", [])

              for group in groups:
                  keys = group.get("Keys", [])
                  metrics = group.get("Metrics", {})

                  for k, v in metrics.items():
                      yield {
                          "time_period_start": time_period.get("Start"),
                          "time_period_end": time_period.get("End"),
                          "group_keys": keys,
                          "metric_name": k,
                          "amount": v.get("Amount"),
                          "amount_unit": v.get("Unit")
                      }

            if not next_page_token:
              break
