# -Temperature-Analysis-using-MapReduce

## Overview
This project uses MapReduce to look at temperature data and find days that are hot or cold based on given threshold limits

## Dataset
- Name Weather_Dataset.txt
- Format Text file containing records with date, maximum temperature, minimum temperature, and other unwanted data .

## Requirements
- Hadoop installed and configured in a single-node cluster.
- Java installed.

## Project Structure
- `exp4.jar` The compiled MapReduce application.
- `data.txt` The input dataset (Weather_Dataset).


## Running the Application
To execute the MapReduce job, use the following command

```bash
hadoop jar /home/cloudera/Desktop/exp4.jar MyMaxMin /exp4/Weather_Dataset /output_exp4
