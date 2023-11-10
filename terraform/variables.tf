# GCP variables
variable "credentials" {
  description = "The GCP credentials JSON file path."
  type        = string
}

variable "project_id" {
  description = "The GCP project ID."
  type        = string
  default     = "idealista-scraper-384619"
}

variable "region" {
  description = "The GCP region."
  type        = string
  default     = "europe-southwest1"
}

variable "zone" {
  description = "The zone to host the resources."
  type        = string
  default     = "europe-southwest1-b"
}

variable "machine_type" {
  description = "The machine type for the Prefect VM."
  type        = string
  default     = "e2-medium"
}

variable "vm_image" {
  description = "The image for the Prefect VM."
  type        = string
  default     = "ubuntu-os-cloud/ubuntu-2004-lts"
}

variable "boot_disk_size" {
  description = "The size of the boot disk for the Prefect VM in GB."
  type        = number
  default     = 20
}

variable "vm_ssh_user" {
  description = "The SSH username for the Idealista VM."
  type        = string
}

variable "vm_ssh_pub_key" {
  description = "The path to the SSH public key for the Idealista VM."
  type        = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "data_lake_bucket" {
  description = "Name of the data lake - GCS, where scraped data will be placed to"
  type        = string
  default     = "idealista_data_lake"
}

variable "bq_dataset" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "idealista_listings"
}

# DigitalOcean variables
variable "do_token" {
  description = "The DigitalOcean API token."
  type        = string
}

variable "do_region" {
  description = "The DigitalOcean region."
  type        = string
  default     = "fra1"
}

variable "do_machine_type" {
  description = "The machine type for the Prefect VM in DigitalOcean."
  type        = string
  default     = "s-2vcpu-4gb"
}

variable "do_vm_image" {
  description = "The image for the Prefect VM in DigitalOcean."
  type        = string
  default     = "ubuntu-20-04-x64"
}

# AWS variables
variable "aws_region" {
  description = "AWS region to launch servers."
  default     = "eu-south-2"
}

variable "ami_id" {
  description = "The ID of the AMI to use for the instance"
  type        = string
  default     = "ami-056f255d137f5a970"
}

variable "instance_type" {
  description = "The size of instance to launch."
  type        = string
  default     = "t4g.small"
}

variable "ssh_cidr_blocks" {
  description = "The CIDR blocks that are allowed to access the instance over SSH."
  type        = list(string)
  default     = ["0.0.0.0/0"]  # Be cautious with this default value; it allows SSH access from any IP.
}
