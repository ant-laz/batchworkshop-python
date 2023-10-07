# Copyright 2023 Google LLC

#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at

#        http://www.apache.org/licenses/LICENSE-2.0

#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

########################################################################################
# Imports
########################################################################################
from typing import Optional, List, Tuple

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam import PCollection

########################################################################################
# TASK 1 : Extract speech field from CSV
########################################################################################

class ExtractSpeech(beam.DoFn):
   
   # element is a csv record
   # example 1 speech filed has escape character enclosed in doulbe quotes '"'
   # 1,1,Albus Dumbledore,"I should have known you would be here, Professor McGonagall."
   # example 2 speech field has no escape character
   # 5,1,Albus Dumbledore,Hagrid is bringing him.
   # need to handle both cases, extract speech field for all lines
   def process(self, element: str) -> List[str]:
    speech_field = None
    if element.find('"') != -1:
        speech_field = element.split('"')[1]
    else:
        speech_field = element.split(',')[3]
    return [speech_field]

########################################################################################
# TASK 2 : Split a line of speech into words
########################################################################################

class SpeechToWords(beam.DoFn):

    def process(self, element: str) -> List[str]:
        return element.split()

########################################################################################
# TASK 3 : remove special characters from words
########################################################################################

class SanitizeWords(beam.DoFn):

    def process(self, element: str) -> List[str]:
        word = element
        word = word.lower()
        word = word.replace(",", "").replace(".", "")
        return[word]


########################################################################################
# TASK 4 : counting occurence of words
########################################################################################

########################################################################################
# TASK 5 : ranking words by occuring and taking top N
########################################################################################

########################################################################################
# TASK 6 : prettify output before writing to files
########################################################################################

def prettify(tl: List[Tuple[str, int]]) -> str:
    pretty_str = ""
    for t in tl:
        pretty_str += f"{t[0]},{t[1]}\n"
    return pretty_str

########################################################################################
# TASK 7 : PTransform to represent the core pipeline logic (excludes input + output)
########################################################################################

class FrequentWords(beam.PTransform):
 
    def expand(self, pcoll):
        speech: PCollection[str] = pcoll | "speech" >> beam.ParDo(ExtractSpeech())
        words: PCollection[str] = speech | "words" >> beam.ParDo(SpeechToWords())
        sanitized: PCollection[str] = words | "clean" >> beam.ParDo(SanitizeWords())
        counted: PCollection[Tuple[str, int]] = sanitized | "Count" >> beam.combiners.Count.PerElement()
        ranked: PCollection[List[Tuple[str, int]]] = counted | "Rank" >> beam.combiners.Top.Of(10,key=lambda t: t[1])
        prettied: PCollection[str] = ranked | "Pretty print" >> beam.Map(prettify)   
        return prettied  

#######################################################################################
# Pipeline
####################################################################################### 

def run(
    beam_options: Optional[PipelineOptions] = None
) -> None:
    with beam.Pipeline(options=beam_options) as pipeline:    
        # 1. Extract
        records: PCollection[str] = pipeline | "Reading input" >> beam.io.ReadFromText(
            file_pattern=beam_options.input_filename, 
            skip_header_lines=1)
        # 2. Transform        
        stats: PCollection[dict] = records | "calculations" >> FrequentWords()
        # 3. Load
        stats | "Write output" >> beam.io.WriteToText(beam_options.output_filename)    