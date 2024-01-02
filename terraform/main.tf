terraform {
  required_version = ">= 1.0"
  backend "local" {}  # Can change from "local" to "gcs" (for google) or "s3" (for aws), if you would like to preserve your tf-state online
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
    aws = {
      source = "hashicorp/aws"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
  credentials = file(var.credentials)
}

provider "aws" {
  region = var.aws_region
}

# AWS VMs
resource "aws_instance" "idealista_vm_1" {
  ami           = var.ami_id
  instance_type = var.instance_type

  key_name = aws_key_pair.idealista_vm_key.key_name

  root_block_device {
    volume_size = 20
  }

  vpc_security_group_ids = [aws_security_group.allow_ssh.id]

  depends_on = [
    aws_key_pair.idealista_vm_key,
    aws_security_group.allow_ssh,
  ]
  
  tags = {
    Name = "idealista-pipeline-vm-sale"
  }
}

resource "aws_instance" "idealista_vm_2" {
  ami           = var.ami_id
  instance_type = var.instance_type

  key_name = aws_key_pair.idealista_vm_key.key_name

  root_block_device {
    volume_size = 20
  }

  vpc_security_group_ids = [aws_security_group.allow_ssh.id]

  depends_on = [
    aws_key_pair.idealista_vm_key,
    aws_security_group.allow_ssh,
  ]
  
  tags = {
    Name = "idealista-pipeline-vm-rent"
  }
}

resource "aws_key_pair" "idealista_vm_key" {
  key_name   = "idealista_vm_key"
  public_key = file(var.vm_ssh_pub_key)
}

resource "aws_security_group" "allow_ssh" {
  name        = "allow_ssh"
  description = "Allow SSH inbound traffic"

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = var.ssh_cidr_blocks
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "allow_ssh"
  }
}

# Data Lake Bucket
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/storage_bucket
resource "google_storage_bucket" "data-lake-idealista" {
  name          = "${var.data_lake_bucket}_${var.project_id}" 
  location      = var.region

  # Optional, but recommended settings:
  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30
    }
  }
}

# Data Warehouse
# Ref: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset
resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bq_dataset
  project    = var.project_id
  location   = var.region
}
