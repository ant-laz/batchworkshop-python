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

  ## checking everything is setup correctly 

  run this python one-liner to check that you python + apache beam installed

  ```sh
  python -c "import apache_beam as beam; import sys; print(f'beam version is = {beam.__version__}'); print(f'python version is = {sys.version}')"
  ```

  Output shold be similar to
  ```sh
beam version is = 2.50.0
python version is = 3.10.0 (default, Apr 28 2023, 17:16:10) [Clang 14.0.3 (clang-1403.0.22.14.1)]
  ```

# 2. Developing the pipeline by making the test suite pass

To run all the tests

```sh
python -m unittest -v
```

To run just a single test, choose from the below.

For task 1
```sh
python -m unittest test.test_pipeline.TestExtractSpeech.test_task_1_extract_speech
```

For task 2
```sh
TODO
```

For task 3
```sh
TODO
```

For task 4
```sh
TODO
```

For task 5
```sh
TODO
```

For task 6
```sh
TODO
```

For task 7
```sh
TODO
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
  --output_filename=data/output/harry_potter_philosopher_stone_script_sample_results.txt
```

Run it for the entire script of Harry Potter & the Philosopher's stone.

```sh
python main.py  \
  --runner DirectRunner \
  --save_main_session \
  --setup_file ./setup.py \
  --input_filename=data/input/harry_potter_philosopher_stone_script.csv \
  --output_filename=data/output/harry_potter_philosopher_stone_script_results.txt
```


Here is a description of what each of these flags mean

| Flag | Description |
| --- | --- |
| runner | Apache Beam execution engine or "runner", e.g. DirectRunner or DataflowRunner |
| streaming | By omitting this the pipeline does not execute in streaming mode but in batch mode |
| save_main_session | Make global imports availabe to all dataflow workers [details](https://cloud.google.com/dataflow/docs/guides/common-errors#name-error) |
| setup_file | To hanle Multiple File Dependencies [details](https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/) |
| input-filename | Custom. The input file to the pipeline. |
| output-filename | Custom. The output file of the pipeline |

# 4. Executing the pipeline using the DataflowRunner

TODO

# 5. Credits

This is based on a workshop created by Israel Herraiz which can be found [here](https://youtu.be/ljoba-i6ZPk)

This uses a dataset from Kaggle which can be found [here](https://www.kaggle.com/datasets/eward96/harry-potter-and-the-philosophers-stone-script)