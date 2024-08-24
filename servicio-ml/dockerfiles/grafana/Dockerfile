# Usa la imagen base oficial de Grafana
FROM grafana/grafana:latest

# Copia las configuraciones de provisioning
# Creacion de carpetas para dashboards y datasources
COPY ./provisioning /etc/grafana/provisioning

# Los dashboards se copian en la carpeta /var/lib/grafana/dashboards
# se partio de dashboards publicos de grafana y se aplicaron las modificaciones necesarias

# fuente: https://min.io/docs/minio/container/operations/monitoring/grafana.html
COPY ./dashboards/minio /var/lib/grafana/dashboards/minio

# fuente: https://github.com/data-burst/airflow-monitoring-and-alerting/tree/main/config_files/grafana/var/lib/grafana/dashboards
COPY ./dashboards/airflow /var/lib/grafana/dashboards/airflow

# fuente: https://grafana.com/grafana/dashboards/14114-postgres-overview/
COPY ./dashboards/postgres /var/lib/grafana/dashboards/postgres