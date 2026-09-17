output "connection_name" {
  description = "Cloud SQL connection name in project:region:instance format."
  value       = google_sql_database_instance.postgres.connection_name
}

output "private_ip" {
  description = "Private IP address assigned to the Cloud SQL instance."
  value       = google_sql_database_instance.postgres.private_ip_address
}

output "database_name" {
  description = "Created PostgreSQL database name."
  value       = google_sql_database.database.name
}

output "network_name" {
  description = "Dedicated VPC network name."
  value       = google_compute_network.gapguard.name
}

output "subnet_name" {
  description = "Dedicated workload subnet name."
  value       = google_compute_subnetwork.gapguard.name
}
