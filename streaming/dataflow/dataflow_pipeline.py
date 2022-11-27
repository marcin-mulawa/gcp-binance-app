import argparse
import json
import logging
import time
from typing import Any, Dict, List
from decimal import Decimal
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import apache_beam.transforms.window as window

# Defines the BigQuery schema for the output table.
SCHEMA = ",".join(
    [
        "symbol:STRING",
        "interval:STRING",
        "open_time:TIMESTAMP",
        "close_time:TIMESTAMP",
        "open:NUMERIC",
        "high:NUMERIC",
        "low:NUMERIC",
        "close:NUMERIC",
        "volume:NUMERIC",
        "number_of_trades:INTEGER",
        "quote_asset_volume:NUMERIC",
        "taker_buy_base_asset_volume:NUMERIC",
        "taker_buy_quote_asset_volume:NUMERIC"
    ]
)


def parse_json_message(message: str) -> Dict[str, Any]:
    """Parse the input json message and add 'score' & 'processing_time' keys."""
    row = json.loads(message)

    # get symbol
    symbol = row["s"]
    # get interval
    interval = row["k"]["i"]
    # get open time
    open_time = datetime.fromtimestamp(row["k"]["t"] / 1000)
    # get close time
    close_time = datetime.fromtimestamp(row["k"]["T"] / 1000)
    # get open price
    open_price = Decimal(row["k"]["o"])
    # get high price
    high_price = Decimal(row["k"]["h"])
    # get low price
    low_price = Decimal(row["k"]["l"])
    # get close price
    close_price = Decimal(row["k"]["c"])
    # get volume
    volume = Decimal(row["k"]["v"])
    # get number of trades
    number_of_trades = row["k"]["n"]
    # get quote asset volume
    quote_asset_volume = Decimal(row["k"]["q"])
    # get taker buy base asset volume
    taker_buy_base_asset_volume = Decimal(row["k"]["V"])
    # get taker buy quote asset volume
    taker_buy_quote_asset_volume = Decimal(row["k"]["Q"])
    # get ignore
    # ignore = row["k"]["B"]
    return {
        "symbol": symbol,
        "interval": interval,
        "open_time": open_time,
        "close_time": close_time,
        "open": open_price,
        "high": high_price,
        "low": low_price,
        "close": close_price,
        "volume": volume,
        "number_of_trades": number_of_trades,
        "quote_asset_volume": quote_asset_volume,
        "taker_buy_base_asset_volume": taker_buy_base_asset_volume,
        "taker_buy_quote_asset_volume": taker_buy_quote_asset_volume,
    }


def run(
    input_subscription: str,
    output_table: str,
    window_interval_sec: int = 60,
    beam_args: List[str] = None,
) -> None:
    """Build and run the pipeline."""
    options = PipelineOptions(beam_args, save_main_session=True, streaming=True)

    with beam.Pipeline(options=options) as pipeline:
        messages = (
            pipeline
            | "Read from Pub/Sub"
            >> beam.io.ReadFromPubSub(
                subscription=input_subscription
            ).with_output_types(bytes)
            | "UTF-8 bytes to string" >> beam.Map(lambda msg: msg.decode("utf-8"))
            | "Parse JSON messages" >> beam.Map(parse_json_message)
            | "Fixed-size windows"
            >> beam.WindowInto(window.FixedWindows(window_interval_sec, 0))
            | "Add URL keys" >> beam.WithKeys(lambda msg: msg["url"])
            | "Group by URLs" >> beam.GroupByKey()
            | "Get statistics"
            >> beam.MapTuple(
                lambda url, messages: {
                    "url": url,
                    "num_reviews": len(messages),
                    "score": sum(msg["score"] for msg in messages) / len(messages),
                    "first_date": min(msg["processing_time"] for msg in messages),
                    "last_date": max(msg["processing_time"] for msg in messages),
                }
            )
        )

        # Output the results into BigQuery table.
        _ = messages | "Write to Big Query" >> beam.io.WriteToBigQuery(
            output_table, schema=SCHEMA
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_table",
        help="Output BigQuery table for results specified as: "
        "PROJECT:DATASET.TABLE or DATASET.TABLE.",
    )
    parser.add_argument(
        "--input_subscription",
        help="Input PubSub subscription of the form "
        '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."',
    )
    parser.add_argument(
        "--window_interval_sec",
        default=60,
        type=int,
        help="Window interval in seconds for grouping incoming messages.",
    )
    args, beam_args = parser.parse_known_args()

    run(
        input_subscription=args.input_subscription,
        output_table=args.output_table,
        window_interval_sec=args.window_interval_sec,
        beam_args=beam_args,
    )