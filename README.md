# Glue job local development using Python

This project is a sample project shows how to develop and test AWS Glue job on a local machine to optimize the costs and have a fast feedback about correct code behavior after doing any code change.

We will analyze movie's data calculating the weighted average and selecting top 10 most popular movies. 

You can download input files movies.csv and ratings.csv datasets from [this location](http://files.grouplens.org/datasets/movielens/ml-latest.zip).

It is demonstrating:

1. Project structure 
2. Unit testing
3. Packaging with the versioning of the source code including dependent libraries

## Project structure
This project contains 2 Spark jobs with generic Spark driver implementation.

```bash
├──driver                    <-- Directory contains a Spark driver file
│   ├──__init__.py
│   └── main.py              <-- Spark driver generic implementation
├── tasks                    <-- Directory contains Spark jobs multiple implementations
│   ├── __init__.py
│   ├── common.py
│   └── jobs.py
└── tests                    <-- Directory contains unit tests and the subset of input data used for the testing purposes
    ├──__init__.py
    ├── samples
    │   ├── movies.csv
    │   └── ratings.csv
    ├── test_average_job.py  <-- Unit test of movie ratings average calculation
    └──test_top10_job.py     <-- Unit test of top 10 movies calculation
├── setup.cfg
├── setup.py
├── pytest.ini               <-- Environment variables used by pytest
├── requirements.txt
├── test-requirements.txt
└── README.md
```

## Unit tests
The idea of the unit test is to validate the business logic correctness using pre-generated data as an input and comparing the output to the expected result. 

There is a unit test implementation in *tests.test_average_job.py* for example.

To be able to implement unit tests there is a need to isolate a code related to a business logic of Spark job from read/write operations. You can clearly see this isolation in *tasks.jobs.create_and_stage_top_movies* for example.

Testing the read/write operations and integration with Glue runtime environment is done as a part of the integration test usually implemented with CI/CD pipeline.

## Walkthrough

### Prerequisites
1. [Install Python](https://realpython.com/installing-python/) 3.8.2 or later
2. Install Java

### Download Glue 1.0 Python libraries
Follow the instructions on Glue documentation [Developing Locally with Python](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html#develop-local-python)

```bash
cd ~
mkdir glue-libs
cd glue-libs
wget https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-1.0/spark-2.4.3-bin-hadoop2.8.tgz
tar -xvzf spark-2.4.3-bin-hadoop2.8.tgz
```

### Checkout [aws-glue-libs repo](https://github.com/awslabs/aws-glue-libs) glue-1.0 branch
```bash
cd ~/glue-libs
git clone https://github.com/awslabs/aws-glue-libs.git -b glue-1.0
```

### Build ```AWSGlueETLPython-1.0.0.jar``` using Maven
```bash
cd aws-glue-libs
mvn package
```
Once Maven task is finished, jar will be created here ```~/aws-glue-libs/target/AWSGlueETLPython-1.0.0.jar```
 
### Create and activate Python virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### Install dependencies
```bash
pip install --upgrade pip
pip install -U -e  .
pip install -r requirements-test.txt
```

### Configure environment variables in the ``pytest.ini`` file.

Some important config options:
- ``PYTHONPATH``: Configure <YOUR HOME DIR>/glue-libs/spark-2.4.3-bin-spark-2.4.3-bin-hadoop2.8/python:<YOUR HOME DIR>/glue-libs/aws-glue-libs/target/AWSGlueETLPython-1.0.0.jar:$PYTHONPATH
- ``JAVA_HOME``: Spark requires Java 8 currently (on Linux: /usr/lib/jvm/java-1.8.0-openjdk-amd64)

### Once you're ready with the configuration you can run the test-set:
```bash
pytest -s -m 'not integration'
```

### Package the source code as ```wheel``` package

```bash
python setup.py build -vf && python setup.py bdist_wheel
```

### Package dependencies separately to be able to configure *Referenced files path* as a part of Glue job deployment
```bash
pip wheel -r requirements.txt -w dist/dependencies
``` 

### To clean all generated directories and files

```bash
python setup.py clean_all
```

## License

This library is licensed under the MIT-0 License. See the [LICENSE](https://github.com/aws-samples/aws-glue-local-development/blob/main/LICENSE) file.