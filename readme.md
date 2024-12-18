# Netflix Data Analysis with Spark & Scala

This project leverages Apache Spark and Scala to perform exploratory data analysis (EDA) on the Netflix dataset, focusing on TV Shows and Movies. Key insights are derived to understand the distribution of content, the popularity of genres, and trends over the years.

---

## Table of Contents
- [Project Overview](#Project-Overview)
- [Important Concepts in Spark](#Important-Concepts-in-Spark)
- [Installation Guide](#Installation-Guide)
- [Running the Project](#Running-the-Project)
- [Generated Outputs](#Generated-Outputs)
- [Additional Information](#Additional-Information)


---

## Project Overview
The Netflix TV Shows and Movies Dataset is analyzed using Apache Spark to uncover patterns and statistics across different attributes like type (Movie/TV Show), country, genre, and release year. The project is designed to handle large datasets efficiently, thanks to the distributed nature of Spark.

---

## Important Concepts in Spark
### RDD (Resilient Distributed Dataset)
RDD is the fundamental abstraction in Spark that represents a distributed collection of data that can be processed in parallel. It ensures fault tolerance and parallelism across clusters.

### DAG (Directed Acyclic Graph)
Spark utilizes a DAG to represent the execution plan for a series of transformations. This structure helps optimize the execution of tasks, ensuring high efficiency in large-scale data processing.

### In-Memory Computing
Spark processes data primarily in memory, which speeds up computations by reducing the need to read/write to disk. This significantly boosts the performance for iterative algorithms, such as those used in machine learning.

### Fault Tolerance
Spark’s fault tolerance is maintained by tracking the lineage of RDDs. If a partition of an RDD is lost, it can be recomputed using the lineage information.

---

## Installation Guide
To get started with this project, follow the steps below:

### Prerequisites
1. **Docker**: Make sure Docker is installed on your system.
2. **Git**: Used to clone the repository.
3. Clone this repository:
   ```bash
   git clone <repository-url>
   cd <repository-folder>
   ```

4. Build the Docker image:

    ```bash
    docker build -t spark-ready .
    ```

--- 

## Running the Project
1. Start the Docker Container
Once the Docker image is built, you can run the container. Make sure to map the output directory to your local file system to access results:

```bash
    docker run --rm -v $(pwd)/output:/app/output spark-ready
```
This will execute the Spark job inside the Docker container, and the results will be saved in the output directory on your local machine.

2. View the Output
Once the project runs successfully, the results will be stored in various files within the output/ directory. These results include insights on the Netflix dataset, including popular countries, genres, and more.

---

## Generated Outputs
The project generates the following outputs, which are saved in the output/ folder:

### Dataset Schema: 
A comprehensive view of the schema, saved in output/schema.txt.
### Sample Data:
The first 5 records from the dataset are saved in output/sample_data/.
### Type Distribution (Movies vs TV Shows): 
A CSV file showing the count of Movies and TV Shows, stored in output/type_count/.
### Top 5 Countries with Most Titles: 
The top 5 countries with the most content are saved in output/top_countries/.
### Popular Genres: 
The most popular genres in the dataset, saved in output/popular_genres/.
### Titles Per Year: 
A CSV file containing the count of titles released per year, saved in output/titles_per_year/.
### Null Counts: 
A summary of null values in the dataset is written to output/null_counts.txt.

---

## Additional Information
Ensure that the dataset file, netflix_titles.csv, is available and correctly referenced in the project. You can adjust the path to this dataset in the code if necessary.
The project is containerized with Docker to ensure a consistent environment across different machines. If you modify the code, you’ll need to rebuild the Docker image:
```bash
docker build -t spark-netflix-analysis .
```