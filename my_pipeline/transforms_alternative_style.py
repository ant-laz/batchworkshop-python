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
from typing import List

import apache_beam as beam
from apache_beam import PCollection


def extract_speech(element: str) -> str:
  """
  Example 1 speech filed has escape character enclosed in double quotes '"'
  1,1,Albus Dumbledore,"I should have known you would be here, Professor McGonagall."
  Example 1 output:
  'I should have known you would be here, Professor McGonagall.'

  Example 2 speech field has no escape character
  5,1,Albus Dumbledore,Hagrid is bringing him.
  Example 2 output:
  'Hagrid is bringing him.'

  Need to handle both cases, extract speech field for all lines.

  :param element: A string with a CSV line.
  :return: Just the text field of the csv records
  """
  if element.find('"') != -1:
    speech_field = element.split('"')[1]
  else:
    speech_field = element.split(',')[3]
  return speech_field


def speech_to_words(element: str) -> List[str]:
  """
  Example input:
  'Hagrid is bringing him.'
  Example output:
  [ 'Hagrid', 'is', 'bringing', 'him.' ]

  :param element: The output of extract speech
  :return: A list of words
  """
  return element.split()


def sanitize_word(element: str) -> str:
  """

  Example inputs:
  ['Hagrid', 'is', 'bringing', 'him.']
  Example outputs:
  ['hagrid', 'is', 'bringing', 'him']

  :param element: A word
  :return: A cleaned word with characters "," & "." removed. And lower case.
  """
  word = element
  word = word.lower()
  word = word.replace(",", "").replace(".", "")
  return word


@beam.ptransform_fn
def count_words_by_frequency(words: PCollection[str]) -> PCollection[tuple[str, int]]:
  """

  Example input:
  # ['harry', 'harry', 'harry', 'ron', 'ron', 'magic']
  # example output:
  # [('harry',3), ('ron',2), ('magic',1)]

  :param words: A :class:`PCollection` with words as strings.
  :return:  A :class:`PCollection` of tuples of (word, count)
  """
  result = words | "Count" >> beam.combiners.Count.PerElement()
  return result


@beam.ptransform_fn
def top_words(tuples: PCollection[tuple[str, int]]) -> PCollection[List[tuple[str, int]]]:
  """
  Example input
  [('harry',100), ('voldermort',99), ('draco',98), ('ron',97),
   ('dobby',96),  ('dumbeldore',95), ('snape',94), ('hagrid',93),
   ('luna',92),   ('hedwig',91),     ('ginny',90), ('bellatrix',89) ]
  Example output
  [('harry',100), ('voldermort',99), ('draco',98), ('ron',97),
   ('dobby',96),  ('dumbeldore',95), ('snape',94), ('hagrid',93),
   ('luna',92),   ('hedwig',91)]

  :param tuples: A :class:`PCollection` of tuples, each looks like (word, count_of_word_occurring)
  :return: Top 10 most frequently occurring words, as tuples (word, count), PCollection with a single element.
  """
  result = tuples | "Rank" >> beam.combiners.Top.Of(10, key=lambda t: t[1])
  return result


def prettify(element: list[tuple[str, int]]) -> str:
  """
  Example input:
  [('harry',100), ('voldermort',99), ('draco',98)]
  Example output:
  "harry,100\nvoldermort,99\ndraco,98\n"

  :param element: A list of tuples (word, count_of_word_occurring)
  :return: A string with CSV lines: word,count\nword,count\nword,count
  """
  pretty_str = ""
  for t in element:
    pretty_str += f"{t[0]},{t[1]}\n"
  return pretty_str


@beam.ptransform_fn
def frequent_words(lines: PCollection[str]) -> PCollection[str]:
  """

  Example input
  [
             '1,1,Albus Dumbledore,"harry Harry harry Harry',
             '2,2,Albus Dumbledore,"potter, potter potter."',
             '3,3,Albus Dumbledore,"magic, magic"',
             '4,4,Albus Dumbledore,"hogwarts"',
  ]

  Example output
  [ "harry,4\npotter,3\nmagic,2\nhogwarts,1\n" ]

  :param lines: A :class:`PCollection`of CSV records, each in a string format
  :return: A single element PCollection with word,count\nword,count\nword,count
  """
  speech: PCollection[str] = lines | "Speech" >> beam.Map(extract_speech)
  words: PCollection[str] = speech | "Words" >> beam.FlatMap(speech_to_words)
  sanitized_words: PCollection[str] = words | "Sanitize" >> beam.Map(sanitize_word)
  sums: PCollection[tuple[str, int]] = sanitized_words | "Count" >> count_words_by_frequency()
  ranked: PCollection[List[tuple[str, int]]] = sums | "Rank" >> top_words()
  prettied: PCollection[str] = ranked | "Prettify" >> beam.Map(prettify)
  return prettied


