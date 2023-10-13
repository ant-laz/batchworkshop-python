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

from typing import List, Tuple, Iterable

import apache_beam as beam
from apache_beam import PCollection


########################################################################################
# TASK 1 : Write a DoFn to Extract speech field from CSV
########################################################################################

class ExtractSpeech(beam.DoFn):

  # input: element is a csv record
  # output: just the text field of the csv records
  # example 1 speech filed has escape character enclosed in doulbe quotes '"'
  # 1,1,Albus Dumbledore,"I should have known you would be here, Professor McGonagall."
  # example 1 output:
  # 'I should have known you would be here, Professor McGonagall.'
  # example 2 speech field has no escape character
  # 5,1,Albus Dumbledore,Hagrid is bringing him.
  # example 2 output:
  # 'Hagrid is bringing him.'
  # need to handle both cases, extract speech field for all lines
  def process(self, element: str) -> Iterable[str]:
    pass


########################################################################################
# TASK 2 : Write a DoFn to Split a line of speech into words
########################################################################################

class SpeechToWords(beam.DoFn):

  # input: string of text
  # output: list of strings, each of word of the inputted text
  # example input 1:
  # 'Hagrid is bringing him.'
  # example output 1:
  # [ 'Hagrid', 'is', 'bringing', 'him.' ]
  def process(self, element: str) -> Iterable[str]:
    pass


########################################################################################
# TASK 3 : Write a DoFn to remove special characters from words
########################################################################################

class SanitizeWords(beam.DoFn):

  # input: list of words
  # output: list of clean up words with characters "," & "." removed. And lower case.
  # exampe input:
  # ['Hagrid', 'is', 'bringing', 'him.']
  # example output:
  # ['hagrid', 'is', 'bringing', 'him']
  def process(self, element: str) -> Iterable[str]:
    pass


########################################################################################
# TASK 4 : Write a PTransform to count occurence of words
########################################################################################

class CountWordFrequency(beam.PTransform):

  # intput: a list of words as strings
  # output: a list of tuple, each tuple looks like (word, count_of_word_occuring)
  # example input:
  # ['harry', 'harry', 'harry', 'ron', 'ron', 'magic']
  # example output:
  # [('harry',3), ('ron',2), ('magic',1)]
  def expand(self, pcoll: PCollection[str]) -> PCollection[Tuple[str, int]]:
    pass


########################################################################################
# TASK 5 : Write a PTransform to rank words by occuring and taking top N
########################################################################################

class TopWords(beam.PTransform):

  # input is a list of tuples, each looks like (word, count_of_word_occuring)
  # output is the top 10 most frequently occuring words, as tuples (word, count)
  # example input
  # [('harry',100), ('voldermort',99), ('draco',98), ('ron',97),
  #  ('dobby',96),  ('dumbeldore',95), ('snape',94), ('hagrid',93),
  #  ('luna',92),   ('hedwig',91),     ('ginny',90), ('bellatrix',89) ]
  # example output
  # [('harry',100), ('voldermort',99), ('draco',98), ('ron',97),
  #  ('dobby',96),  ('dumbeldore',95), ('snape',94), ('hagrid',93),
  #  ('luna',92),   ('hedwig',91)]
  def expand(self, pcoll: PCollection[Tuple[str, int]]
             ) -> PCollection[List[Tuple[str, int]]]:
    pass


########################################################################################
# TASK 6 : Write a DoFn to prettify output before writing to files
########################################################################################

class Prettify(beam.DoFn):

  # input: a list of tuples, (word, count_of_word_occuring)
  # output: a list of 1 string. Format: word,count\nword,count\nword,count
  # example input:
  # [('harry',100), ('voldermort',99), ('draco',98)]
  # example output:
  # ["harry,100\nvoldermort,99\ndraco,98\n"]
  def process(self, element: List[Tuple[str, int]]) -> Iterable[str]:
    pass


########################################################################################
# TASK 7 : PTransform to represent the core pipeline logic (excludes input + output)
########################################################################################

class FrequentWords(beam.PTransform):

  # input: pcoll is a PCollection of CSV records, each in a string format
  # output: list of a single string. format: word,count\nword,count\nword,count
  # example input
  # [
  #            '1,1,Albus Dumbledore,"harry Harry harry Harry',
  #            '2,2,Albus Dumbledore,"potter, potter potter."',
  #            '3,3,Albus Dumbledore,"magic, magic"',
  #            '4,4,Albus Dumbledore,"hogwarts"',
  # ]
  # example output
  # [ "harry,4\npotter,3\nmagic,2\nhogwarts,1\n"]
  def expand(self, pcoll: PCollection[str]) -> PCollection[str]:
    pass
