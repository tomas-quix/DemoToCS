# import the Quix Streams modules for interacting with Kafka.
# For general info, see https://quix.io/docs/quix-streams/introduction.html
from quixstreams import Application

import os

# for local dev, load env vars from a .env file
from dotenv import load_dotenv
load_dotenv()


def main():

    # Setup necessary objects
    app = Application(
        consumer_group="data_norm_v1.5",
        auto_create_topics=True,
        auto_offset_reset="earliest"
    )
    input_topic = app.topic(name=os.environ["input"])
    output_topic = app.topic(name=os.environ["output"])
    sdf = app.dataframe(topic=input_topic)

    sdf = sdf.apply(lambda row: row["payload"], expand=True)

    def transoform_value_to_row(value: dict):

      result = {
        "time": value["time"]
      }
      for dimension in value["values"].keys():
        result[value["name"] + "-" + dimension] = value["values"][dimension]

      return result

    sdf = sdf.apply(transoform_value_to_row)

    sdf[sdf.contains("accelerometer-x")].print_table(metadata=False)

    sdf = sdf.set_timestamp(lambda row, *_: int(row["time"] / 1E6))

    # Finish off by writing to the final result to the output topic
    sdf.to_topic(output_topic)

    # With our pipeline defined, now run the Application
    app.run()


# It is recommended to execute Applications under a conditional main
if __name__ == "__main__":
    main()
