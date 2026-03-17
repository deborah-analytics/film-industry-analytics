## Scripts and Data Processing Pipeline

This folder contains the Python and PySpark scripts used to process, transform, and analyse the film and television datasets.

### Main Script

* **film_analysis_pipeline.py**
  This script implements the full data processing and analysis workflow.

### Key Processes Covered

* Data ingestion from multiple IMDb datasets
* Data cleaning and preprocessing
* Handling missing values and inconsistencies
* Data transformation using PySpark
* Dataset integration using Spark SQL joins
* Feature preparation for analysis

### Analysis Tasks

* Genre-based performance analysis
* Runtime and rating relationship exploration
* Temporal trends in ratings
* Cast and crew role analysis

### Output

The script produces:

* cleaned and integrated datasets
* analytical summaries
* visualisations used for reporting

### Note

PySpark was used to efficiently process large-scale datasets and enable distributed data analysis.
