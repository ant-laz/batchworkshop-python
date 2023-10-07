import unittest


import apache_beam as beam

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to, equal_to_per_window
from apache_beam.transforms.window import TimestampedValue, IntervalWindow
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.testing.test_stream import TestStream
from apache_beam.transforms.trigger import AfterWatermark, AccumulationMode
from apache_beam.transforms.combiners import CountCombineFn
from apache_beam.utils.timestamp import Duration


import my_pipeline

#######################################################################################
# TESTS FOR TASK 1 : Write a DoFn to Extract speech field from CSV
#######################################################################################
class TestExtractSpeech(unittest.TestCase):
    def test_task_1_extract_speech(self):
          
        options = PipelineOptions()

        with TestPipeline(options=options) as p:
            csv_records = [
                '1,1,Albus Dumbledore,"I should have known you would be here, Professor McGonagall."',
                '5,1,Albus Dumbledore,Hagrid is bringing him.'
            ]

            input = p | beam.Create(csv_records)

            output = (
                input |
                beam.ParDo(my_pipeline.ExtractSpeech())
                )
            expected_output = [
                    'I should have known you would be here, Professor McGonagall.',
                    'Hagrid is bringing him.'
                    ]
            
            assert_that(output, equal_to(expected_output))        

#######################################################################################
# TESTS FOR TASK 2 : Write a DoFn to Split a line of speech into words
#######################################################################################


########################################################################################
#  TESTS FOR TASK 3 : Write a DoFn to remove special characters from words
########################################################################################
 

########################################################################################
# TESTS FOR TASK 4 : Write a PTransform to count occurence of words
########################################################################################
         

########################################################################################
# TESTS FOR TASK 5 : Write a PTransform to rank words by occuring and taking top N
########################################################################################

########################################################################################
# TESTS FOR TASK 6 : Write a DoFn to prettify output before writing to files
########################################################################################


########################################################################################
# TESTS FOR TASK 7 : PTransform to represent the core pipeline logic (excludes I/O)
########################################################################################

 

if __name__ == "__main__":
    unittest.main()