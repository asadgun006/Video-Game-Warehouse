# Airflow and Docker guidelines for hosting and running the pipeline

## Prerequisites
* Docker Desktop
* Apache Airflow

### Airflow configuration
For configuring Airflow with Docker compose I used official Airflow documentation found in the below link:
`https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html`

Create a folder that you want to store your dags and docker environment files in, navigate to the folder using command line (I used Bash for this),
and run the following command to download the docker-compose file:
`curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.7.3/docker-compose.yaml`

After creating the airflow directories and mapping the Airflow host user, I tweaked the `docker-compose.yaml` file
to remove the flower, remove redis, and use `LocalExecutor` instead of the default `CeleryExecutor`. See the
`docker-compose.yaml` file above for exact configurations.

## Customizations for Airflow for Docker
1. For adding custom packages to your dags for data extraction and transformation, use a `requirements.txt` for the packages you want to use in addition to the airflow default operators. Once the additional packages are listed, create a `DockerFile` within the same directory as the `docker-compose.yaml` and the `requirements.txt` file. The `DockerFile` defined above is straightforward and is only used to install the required packages.
2. For creating a custom Airflow image, use the command `docker build . -t <your-image-name>:<your-image-tag> --no-cache -f Dockerfile`. This will import all the packages defined in the `requirements.txt` file to the image. Once the image is built, replace the `image:` line in the `docker-compose.yaml` file with `image: ${AIRFLOW_IMAGE_NAME:-<your-image-name>:<your-image-tag>}` to use the image in the future without Airflow building images on the fly everytime.
3. Use `docker compose up -d` to run the image. The image will start the required containters for Airflow. The default port for the Airflow werbserver is `localhost:8080` or `127.0.0.1:8080`. Open any of the ports above on your browser and use `airflow` for both the username and password to open Airflow UI.
4. To create a custom airflow user, enter into the airflow webserver container with bash using `winpty docker exec -it <container_id> bash` (Windows using WSL). Use `docker ps` to list the running containers and copy the webserver container id. Once inside the container, execute airflow users create --help to see configurations.

## Running the pipeline
* DAGs are stores in the `dags` folder by default. Once your ETL python file is ready, enter the Airflow webserver container within Docker and use the `airflow dags unpause <your-dag>` to unpause the DAG, or you can use the Airflow UI for unpausing a DAG.
* In my case, I manually triggered the DAG to process the data in batches, and to monitor any errors. Use `airflow dags trigger <your-dag>` to manually trigger a DAG, and navigate to the airflow webserver to monitor.
* The DAG ran for ~3.5 hours and the API calls were handled for any exceptions. This is crucial as you don't want to waste your API requests limit provided by RAWG by encountering an error after thousands of API calls.

Once finished, the database was transformed and normalized for better querying and analysis
