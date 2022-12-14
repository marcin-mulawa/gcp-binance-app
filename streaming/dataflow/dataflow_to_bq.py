import argparse
from datetime import datetime
import logging
import random
import os
from decimal import Decimal

from apache_beam import (
    DoFn,
    GroupByKey,
    io,
    ParDo,
    Pipeline,
    PTransform,
    WindowInto,
    WithKeys,
)
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

# from apache_beam.transforms.window import FixedWindows, TimestampedValue, SlidingWindows
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io.gcp.internal.clients import bigquery_messages

PROJECT_ID = os.popen("gcloud config get-value project").read().strip()
DATASET_ID = "binance"
TABLE_ID = "binance_klines"


# process data from pubsub to bigquery
class ProcessData(DoFn):
    def process(self, element, window=DoFn.WindowParam):
        # get window start and end
        window_start = window.start.to_utc_datetime()
        window_end = window.end.to_utc_datetime()
        # get data
        data = element[1]
        # get symbol
        symbol = data["s"]
        # get interval
        interval = data["k"]["i"]
        # get open time
        open_time = datetime.fromtimestamp(data["k"]["t"] / 1000)
        # get close time
        close_time = datetime.fromtimestamp(data["k"]["T"] / 1000)
        # get open price
        open_price = Decimal(data["k"]["o"])
        # get high price
        high_price = Decimal(data["k"]["h"])
        # get low price
        low_price = Decimal(data["k"]["l"])
        # get close price
        close_price = Decimal(data["k"]["c"])
        # get volume
        volume = Decimal(data["k"]["v"])
        # get number of trades
        number_of_trades = data["k"]["n"]
        # get quote asset volume
        quote_asset_volume = Decimal(data["k"]["q"])
        # get taker buy base asset volume
        taker_buy_base_asset_volume = Decimal(data["k"]["V"])
        # get taker buy quote asset volume
        taker_buy_quote_asset_volume = Decimal(data["k"]["Q"])
        # get ignore
        ignore = data["k"]["B"]
        # return data
        yield {
            "symbol": symbol,
            "interval": interval,
            "open_time": open_time,
            "close_time": close_time,
            "open_price": open_price,
            "high_price": high_price,
            "low_price": low_price,
            "close_price": close_price,
            "volume": volume,
            "number_of_trades": number_of_trades,
            "quote_asset_volume": quote_asset_volume,
            "taker_buy_base_asset_volume": taker_buy_base_asset_volume,
            "taker_buy_quote_asset_volume": taker_buy_quote_asset_volume,
            # "ignore": ignore,
            # "window_start": window_start,
            # "window_end": window_end,
        }


# Write data to BigQuery
class WriteToBigQuery(DoFn):
    def __init__(self, table_name, dataset_name, project_name):
        self.table_name = table_name
        self.dataset_name = dataset_name
        self.project_name = project_name
        self.table_spec = bigquery_messages.TableReference(
            projectId=self.project_name,
            datasetId=self.dataset_name,
            tableId=self.table_name,
        )
        self.table = bigquery_messages.Table(
            tableReference=self.table_spec, schema=self.schema
        )
        self.create_disposition = bigquery.enums.BigQueryDisposition.CREATE_IF_NEEDED
        self.write_disposition = bigquery.enums.BigQueryDisposition.WRITE_APPEND
        self.batch_size = 100

    def start_bundle(self):
        self.client = bigquery.BigqueryV2(
            url="https://www.googleapis.com/bigquery/v2/"
        )
        self.insert_all_data = []

    def process(self, element):
        self.insert_all_data.append(element)
        if len(self.insert_all_data) >= self.batch_size:
            self.flush()

    def finish_bundle(self):
        self.flush()

    def flush(self):
        insert_all_request = bigquery_messages.BigqueryTabledataInsertAllRequest(
            rows=[
                bigquery_messages.TableDataInsertAllRequestRows(
                    insertId=str(random.randint(0, 1000000)), json=element
                )
                for element in self.insert_all_data
            ],
            skipInvalidRows=False,
            ignoreUnknownValues=False,
        )
        self.client.tabledata().InsertAll(
            insertAllRequest=insert_all_request,
            projectId=self.project_name,
            datasetId=self.dataset_name,
            tableId=self.table_name,
        ).execute()
        self.insert_all_data = []



# run pipeline
def run(project_name, dataset_name, table_name, input_topic, pipeline_args=None):
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    pipeline_options = PipelineOptions(
        pipeline_args, streaming=True, save_main_session=True
    )

    p = Pipeline(argv=pipeline_args)
    (
        p
        | "Read from PubSub" >> beam.io.ReadFromPubSub(topic=known_args.input_topic)
        | "Parse JSON" >> Map(lambda x: json.loads(x))
        | "Write to BigQuery"
        >> ParDo(
            WriteToBigQuery(
                project_name=known_args.project_name,
                dataset_name=known_args.dataset_name,
                table_name=known_args.table_name,
            )
        )
    )
    p.run().wait_until_finish()


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_name", help="Project name", required=True)
    parser.add_argument("--dataset_name", help="Dataset name", required=True)
    parser.add_argument("--table_name", help="Table name", required=True)
    parser.add_argument("--input_topic", help="Input topic", required=True)
    known_args, pipeline_args = parser.parse_known_args()
    run(
        project_name=known_args.project_name,
        dataset_name=known_args.dataset_name,
        table_name=known_args.table_name,
        input_topic=known_args.input_topic,
        pipeline_args=pipeline_args,
    )
