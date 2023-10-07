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
class TestSpeechToWords(unittest.TestCase):
    def test_task_2_speech_to_words(self):
          
        options = PipelineOptions()

        with TestPipeline(options=options) as p:
            speech = ['Hagrid is bringing him.']

            input = p | beam.Create(speech)

            output = (
                input |
                beam.ParDo(my_pipeline.SpeechToWords())
                )
            expected_output = [
                    'Hagrid',
                    'is',
                    'bringing',
                    'him.'
                    ]
            
            assert_that(output, equal_to(expected_output))  

########################################################################################
#  TESTS FOR TASK 3 : Write a DoFn to remove special characters from words
########################################################################################
class TestSanitizeWords(unittest.TestCase):
    def test_task_3_sanitize_words(self):
          
        options = PipelineOptions()

        with TestPipeline(options=options) as p:
            words = ['Hagrid', 'is', 'bringing', 'him.']

            input = p | beam.Create(words)

            output = (
                input |
                beam.ParDo(my_pipeline.SanitizeWords())
                )
            expected_output = ['hagrid', 'is', 'bringing', 'him']
            
            assert_that(output, equal_to(expected_output))   

########################################################################################
# TESTS FOR TASK 4 : Write a PTransform to count occurence of words
########################################################################################
class TestCountWordFrequency(unittest.TestCase):
    def test_task_4_count_word_frequency(self):
          
        options = PipelineOptions()

        with TestPipeline(options=options) as p:
            words = ['hagrid', 'is', 'bringing', 'him']

            input = p | beam.Create(words)

            output = (
                input |
                my_pipeline.CountWordFrequency()
                )
            expected_output = [('hagrid',1), ('is',1), ('bringing',1), ('him',1)]
            
            assert_that(output, equal_to(expected_output))  

########################################################################################
# TESTS FOR TASK 5 : Write a PTransform to rank words by occuring and taking top N
########################################################################################
class TopWords(unittest.TestCase):
    def test_task_5_top_words(self):
          
        options = PipelineOptions()

        with TestPipeline(options=options) as p:
            frequent_words = [
                ('harry',100), 
                ('voldermort',99), 
                ('draco',98), 
                ('ron',97),
                ('dobby',96),
                ('dumbeldore',95),
                ('snape',94),
                ('hagrid',93),
                ('luna',92),
                ('hedwig',91),
                ('ginny',90),
                ('bellatrix',89),
                ]

            input = p | beam.Create(frequent_words)

            output = (
                input |
                my_pipeline.TopWords()
                )
            # .Top.Of() has @with_input_types(T) & @with_output_types(List[T])
            # https://beam.apache.org/releases/pydoc/2.50.0/_modules/apache_beam/transforms/combiners.html#Top.Of
            # given input is of type List[Tuple[str,int]]
            # outpt is of type List[List[Tuple[str,int]]]
            expected_output = [[
                ('harry',100), 
                ('voldermort',99), 
                ('draco',98), 
                ('ron',97),
                ('dobby',96),
                ('dumbeldore',95),
                ('snape',94),
                ('hagrid',93),
                ('luna',92),
                ('hedwig',91)
                ]]        
            
            assert_that(output, equal_to(expected_output))
########################################################################################
# TESTS FOR TASK 6 : Write a DoFn to prettify output before writing to files
########################################################################################
class Prettify(unittest.TestCase):
    def test_task_6_prettify(self):
          
        options = PipelineOptions()

        with TestPipeline(options=options) as p:
            topwords = [[
                ('harry',100), 
                ('voldermort',99), 
                ('draco',98), 
                ('ron',97),
                ('dobby',96),
                ('dumbeldore',95),
                ('snape',94),
                ('hagrid',93),
                ('luna',92),
                ('hedwig',91)
                ]] 

            input = p | beam.Create(topwords)

            output = (
                input |
                beam.ParDo(my_pipeline.Prettify())
                )
            # use '()' as a python trick to contact strings across rows into 1 string.
            expected_output = [
                (
                "harry,100\n"
                "voldermort,99\n" 
                "draco,98\n"
                "ron,97\n"
                "dobby,96\n"
                "dumbeldore,95\n"
                "snape,94\n"
                "hagrid,93\n"
                "luna,92\n"
                "hedwig,91\n"
                )
                ]
            
            assert_that(output, equal_to(expected_output))   


########################################################################################
# TESTS FOR TASK 7 : PTransform to represent the core pipeline logic (excludes I/O)
########################################################################################
class FrequentWords(unittest.TestCase):
    def test_task_7_frequent_words(self):
          
        options = PipelineOptions()

        with TestPipeline(options=options) as p:
            csv_records = [
                '1,1,Albus Dumbledore,"harry Harry harry Harry',
                '2,2,Albus Dumbledore,"potter, potter potter."',
                '3,3,Albus Dumbledore,"magic, magic"',
                '4,4,Albus Dumbledore,"hogwarts"',
            ]

            input = p | beam.Create(csv_records)

            output = (
                input |
                my_pipeline.FrequentWords()
                )
            # use '()' as a python trick to contact strings across rows into 1 string.
            expected_output = [
                (
                "harry,4\n"
                "potter,3\n" 
                "magic,2\n"
                "hogwarts,1\n"
                )
                ]        
            
            assert_that(output, equal_to(expected_output))
 

if __name__ == "__main__":
    unittest.main()