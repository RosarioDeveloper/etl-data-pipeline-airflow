# ETL Toll Data Pipeline with Apache Airflow

This project implements an ETL (Extract, Transform, Load) pipeline for toll data using Apache Airflow. The workflow automates the extraction, transformation, and consolidation of vehicle and toll plaza data from various file formats.

## Project Structure
- `dags/`: Contains Airflow DAGs and data files
- `config/`: Airflow configuration files
- `logs/`: Airflow logs
- `plugins/`: Custom Airflow plugins (if any)

## Main Workflow (`dags/workflow.py`)
The DAG `ETL_toll_data` runs daily and consists of the following tasks:

1. **Unzip Data**: Extracts the contents of `tolldata.tgz`.
2. **Extract Data from CSV**: Selects columns from `vehicle-data.csv` and writes to `csv_data.csv`.
3. **Extract Data from TSV**: Selects columns from `tollplaza-data.tsv` and writes to `tsv_data.csv`.
4. **Extract Data from Fixed Width File**: Extracts fields from `payment-data.txt` and writes to `fixed_width_data.csv`.
5. **Consolidate Data**: Merges the extracted files into `extracted_data.csv`.
6. **Transform Data**: Converts vehicle type to uppercase and writes the result to `staging/transformed_data.csv`.

## How to Run
1. Ensure Docker and Airflow are installed (see `docker-compose.yaml`).
2. Place all required data files in the `dags/` directory.
3. Start Airflow using Docker Compose:
   ```zsh
   docker-compose up
   ```
4. Access the Airflow UI at `http://localhost:8080` and trigger the `ETL_toll_data` DAG.

## Output
- Transformed data is saved in `dags/staging/transformed_data.csv`.
- Intermediate files are created in the `dags/` directory.

## Notes
- Update email settings in `workflow.py` for notifications.
- Check `.gitignore` to avoid committing data, logs, and environment files.

## License
MIT
