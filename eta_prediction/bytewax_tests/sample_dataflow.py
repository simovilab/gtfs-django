from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.testing import TestingSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.testing import run_main

# Create and configure the Dataflow
flow = Dataflow("upper_case")

# Input source for dataflow
inp = op.input("inp",flow, TestingSource(["apple", "banana", "cherry"]))

# Define your data processing logic
def process_data(item):
    return item.upper()

# Apply processing logic
out = op.map("process", inp, process_data)

# Output the results to stdout through an StdOutSink,
# the StdOutSink can easily be changed to an output
# such as a database through the use of connectors
op.output("out", out, StdOutSink())