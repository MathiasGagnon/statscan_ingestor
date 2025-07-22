# Airflow Dev Environment

This repository contains an Apache Airflow project built to orchestrate the ELT work for my Statistics Canada data interpretation project.

---

## Prerequisites

Make sure you have the following installed on your machine:

- **Python** (3.8+ recommended)
- **Docker** (latest stable version)
- **Visual Studio Code** with the **Remote - Containers** extension

---

## Getting Started

### 1. Clone the repository

```bash
git clone <your-repo-url>
cd <your-repo-folder>
```

### 2. Setup .env file
Create a .env file in the root folder with the following content:

AIRFLOW__API_AUTH__JWT_SECRET=your-generated-key

To generate a key, run:
```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

### 3. Open in VSCode Dev Container
Open this project folder in VSCode

When prompted, reopen in container (or click the green button in the bottom-left and select Reopen in Container)

VSCode will build the Docker container and mount your dags and plugins

### 4. Start Airflow services
Inside the dev container terminal, run:

``` bash
docker compose up
```

This will start all of Airflow's services.

To access the UI, open your browser and go to:

http://localhost:8080

The default login is Airflow:Airflow, which can be modified in the docker-compose file.

--- 
## References
Official Apache Airflow Docker Compose documentation:
https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html#docker-compose
