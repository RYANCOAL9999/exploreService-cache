terraform {
    required_version = ">= 1.5.0"
    required_providers {
        aws = {
            source  = "hashicorp/aws"
            version = "~> 5.0"
        }
        kubernetes = {
            source  = "hashicorp/kubernetes"
            version = "~> 2.0"
        }
    }
}

provider "aws" {
    region = var.aws_region
}

# 💡 Hints: Dynamically obtaining Kubernetes authentication tokens from AWS EKS and resolving token expiration issues.
data "aws_eks_cluster" "cluster" {
    name = var.eks_cluster_name
}

data "aws_eks_cluster_auth" "cluster" {
    name = var.eks_cluster_name
}

provider "kubernetes" {
    host                   = data.aws_eks_cluster.cluster.endpoint
    cluster_ca_certificate = base64decode(data.aws_eks_cluster.cluster.certificate_authority[0].data)
    token                  = data.aws_eks_cluster_auth.cluster.token
}
