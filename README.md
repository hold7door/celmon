# Celmon

![Celery Monitor Logs](https://github.com/hold7door/celmon/blob/master/celmon.jpeg)

This project implements a monitoring system for Celery using Prometheus and Grafana. It tracks worker and task metrics, providing insights into your Celery cluster's performance.

## Components

- **Celery Monitor**: A custom Python application that captures Celery events and exposes them as Prometheus metrics.
- **Redis**: Used as a message broker for Celery and for persisting monitor state.
- **Prometheus**: Scrapes and stores metrics from the Celery Monitor.
- **Grafana**: Provides visualization of the metrics collected by Prometheus.

## Prerequisites

- Docker
- Docker Compose

## Project Structure

- `main.py`: The main application that initializes Celery, sets up Prometheus metrics, and captures Celery events.
- `celeryconfig.py`: Configuration for Celery.
- `prometheus.yml`: Prometheus configuration.
- `docker-compose.yml`: Docker Compose configuration for running the services.
- `grafana-dashboard.json`: Grafana dashboard template.

## Running the Project

1. Clone the repository:
   ```
   git clone https://github.com/your-username/celery-monitoring-system.git
   cd celery-monitoring-system
   ```

2. Start the services using Docker Compose:
   ```
   docker-compose up -d
   ```

3. Access the services:
   - Celery Monitor: http://localhost:8099
   - Prometheus: http://localhost:9090
   - Grafana: http://localhost:3000

4. Set up Grafana:
   - Log in to Grafana (default credentials: admin/admin)
   - Add Prometheus as a data source (URL: http://prometheus:9090)
   - Import the dashboard using the provided `grafana-dashboard.json` file

5. Start generating Celery tasks to see the metrics in action.

## Monitoring

- The Celery Monitor exposes metrics at http://localhost:8099
- Prometheus scrapes these metrics and stores them
- Grafana visualizes the data from Prometheus

You can customize the Grafana dashboard or create new ones based on the available metrics.

## Shutting Down

To stop the services:

```
docker-compose down
```

This will stop and remove all the containers created by Docker Compose.

## License

This project is licensed under the MIT License. See the `LICENSE` file for more details.
