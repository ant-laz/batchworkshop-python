# Copyright 2023 Google LLC
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
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

from my_pipeline.transforms import FrequentWords


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
        # For alternative style transforms, use frequent_words() instead
        # (see transforms_alternative_style.py)
        stats: PCollection[str] = records | "calculations" >> FrequentWords()
        # 3. Load
        stats | "Write output" >> beam.io.WriteToText(beam_options.output_filename)
