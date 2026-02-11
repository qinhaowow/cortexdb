variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "dev"
}

variable "project" {
  description = "Project name"
  type        = string
  default     = "coretexdb"
}

variable "vpc_cidr" {
  description = "VPC CIDR block"
  type        = string
  default     = "10.0.0.0/16"
}

variable "number_of_azs" {
  description = "Number of availability zones"
  type        = number
  default     = 3
}

variable "private_subnets" {
  description = "Private subnet CIDR blocks"
  type        = list(string)
  default = [
    "10.0.1.0/24",
    "10.0.2.0/24",
    "10.0.3.0/24",
  ]
}

variable "public_subnets" {
  description = "Public subnet CIDR blocks"
  type        = list(string)
  default = [
    "10.0.101.0/24",
    "10.0.102.0/24",
    "10.0.103.0/24",
  ]
}

variable "allowed_cidr_blocks" {
  description = "Allowed CIDR blocks for ingress"
  type        = list(string)
  default     = ["0.0.0.0/0"]
}

variable "k8s_version" {
  description = "Kubernetes version"
  type        = string
  default     = "1.28"
}

variable "node_groups" {
  description = "EKS node groups"
  type = map(object({
    instance_types = list(string)
    desired_size   = number
    min_size       = number
    max_size       = number
    capacity_type  = string
  }))
  default = {
    general = {
      instance_types = ["m5.xlarge"]
      desired_size   = 2
      min_size       = 1
      max_size       = 10
      capacity_type  = "ON_DEMAND"
    }
    memory = {
      instance_types = ["r5.xlarge"]
      desired_size   = 1
      min_size       = 1
      max_size       = 5
      capacity_type  = "ON_DEMAND"
    }
    compute = {
      instance_types = ["c5.xlarge"]
      desired_size   = 1
      min_size       = 1
      max_size       = 5
      capacity_type  = "SPOT"
    }
  }
}

variable "redis_node_type" {
  description = "Redis cache node type"
  type        = string
  default     = "cache.m5.large"
}

variable "redis_num_nodes" {
  description = "Number of Redis nodes"
  type        = number
  default     = 2
}

variable "efs_throughput_mibps" {
  description = "EFS throughput in MiB/s"
  type        = number
  default     = 100
}
