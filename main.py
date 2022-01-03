#
# Dataflow 0: Inside Google Shell
# Taylor Bell
#

# Command Line arguments to use inside Google Shell:
#
# python main.py \
#       --region us-central1 \
#       --runner DataflowRunner \
#       --input gs://york-project-bucket/taylorryanbell/2021-12-16-23-53/part-r-*
#       --output gs://york-project-bucket/taylorryanbell/2021-12-16-23-53/results/output
#       --project york-cdf-start \
#       --temp_location gs://york-project-bucket/taylorryanbell/2021-12-16-23-53/tmp/
#

import argparse
import logging

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class StripHeader(beam.DoFn):
    def process(self, element):

        # check to see if the given line (element) is the header, and if so, skip it, otherwise yield the line
        if str(element) == 'author,points':
            pass
        else:
            yield element


def run(argv=None, save_main_session=True):
    """Main entry point; defines and runs the csv-congregate pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        default='gs://york-project-bucket/taylorryanbell/2021-12-16-23-53/part-r-*',
        help='Input folder path to process. Use wildcard (*) for processing multiple files')
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session

    # The pipeline will be run on exiting the with block.
    with beam.Pipeline(options=pipeline_options) as pipeline:

        # read all of the files with a given name prefix from the specified storage bucket.
        # print(known_args.input)
        # print(type(known_args.input))
        lines = pipeline | 'Read' >> beam.io.ReadFromText(known_args.input)

        # using a homemade PTransform to remove the Header in each file
        out_col = lines | beam.ParDo(StripHeader())

        # write a new file and include a single header at the top.
        out_col | beam.io.WriteToText(known_args.output, file_name_suffix='.csv', header="author,points")


if __name__ == '__main__':

    print('+-------------------------+')
    print('| GCP CSV Congregate v1.3 |')
    print('+-------------------------+')
    logging.getLogger().setLevel(logging.INFO)
    run()