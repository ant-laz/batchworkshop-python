# Apache Beam batch pipeline workshop (python)

Here we present a step by step guide to building a batch pipeline in Python using Apache Beam.

# Choosing the correct code branch

The "main" brach has the complete code for the batch pipeline.

the "workshop" branch has code with gaps that are to be completed live in the workshop.

Checout the "workshop" branch and challenge yourself ! 

```sh
git checkout workshop
```

# 1. Setting up your environment

## Python 3.10

This is a hands on workshop, please come ready with Python 3.10 installed: 

 * use the Python offical [downloads](https://www.python.org/downloads/)
 * use pyenv
 * following the instructions for Google [Cloud](https://cloud.google.com/python/docs/setup)
 * ...of whatever you prefer

 ## Development environment

 This is a hands on workshop, please come ready with a development environment: 

  * VSCode
  * PyCharm
  * GCP cloud shell
  * GCP cloud shell editor
  * GCP workstation
  * ... or whatever you prefer ! 

  ## Python virtual env

  With python pointing at python 3.10 run the following

  Create a virtual environment

  ```sh
  python -m venv venv
  ```

  Activate the virual environment
  
  ```sh
  source venv/bin/activate
  ```

  While activated, your python and pip commands with point to the virtual environment, 
  so any changes or installed dependencies are self-contained.

  ## Initialize pipeline code

  Execute from the root of this repo to initialize the pipeline code.

  First, update pip before installing dependencies. It's always a good idea to do this.

  ```sh
  pip install -U pip
  ```

  Next, install the project as a local package. This installs all dependencies as well.

  ```sh
  pip install -e .
  ```

# 2. Developing the pipeline by making the test suite pass

To run all the tests

```sh
python -m unittest -v
```

To run just a single test, choose from the below.
```sh
python -m unittest test.test_pipeline.TestTaxiPointCreation.test_task_1_taxi_point_creation
```

# 3. Executing the pipline using the DirectRunner

It is possible to run the pipeline on your machine using the DirectRunner.

Run it for a sample of the script of Harry Potter & the Philosopher's stone.

```sh
python main.py  \
  --runner DirectRunner \
  --save_main_session \
  --setup_file ./setup.py \
  --input_filename=data/input/harry_potter_philosopher_stone_script_sample.csv \
  --output_filename=data/output/harry_potter_philosopher_stone_script_sample_results.txt \
  --number_of_top_words_to_report_on=100
```

Run it for the entire script of Harry Potter & the Philosopher's stone.

```sh
python main.py  \
  --runner DirectRunner \
  --save_main_session \
  --setup_file ./setup.py \
  --input_filename=data/input/harry_potter_philosopher_stone_script.csv \
  --output_filename=data/output/harry_potter_philosopher_stone_script_results.txt \
  --number_of_top_words_to_report_on=100
```


Here is a description of what each of these flags mean

| Flag | Description |
| --- | --- |
| runner | Apache Beam execution engine or "runner", e.g. DirectRunner or DataflowRunner |
| streaming | If present, pipeline executes in streaming mode otherwise in batch mode |
| save_main_session | Make global imports availabe to all dataflow workers [details](https://cloud.google.com/dataflow/docs/guides/common-errors#name-error) |
| setup_file | To hanle Multiple File Dependencies [details](https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/) |
| input-filename | Custom. The input file to the pipeline. |
| output-filename | Custom. The output file of the pipeline |

# 4. Executing the pipeline using the DataflowRunner

TODO