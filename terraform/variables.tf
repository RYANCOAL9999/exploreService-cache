variable "aws_region" {
    type    = string
    default = "your_aws_region"
}

variable "eks_cluster_name" {
    type        = string
    description = "your_eks_cluster_name"
}

variable "aws_account_id" {
    type        = string
    description = "your_aws_account_id"
}

variable "redis_endpoint" {
    type        = string
    description = "Pre-configured Amazon ElastiCache for Redis Cluster Endpoint"
}

variable "redis_node_type" {
  type        = string
  default     = "cache.t4g.medium" # Cost-effective AWS Graviton instance for high concurrent microsecond execution
  description = "The compute instance type for the AWS ElastiCache clusters"
}

variable "kafka_topic_name" {
    type        = string
    description = "The default Kafka topic stream used by the decision persister server."
    default     = "myQueue"
}

variable "kafka_group_id" {
    type        = string
    description = "The consumer group coordinator identifier for K8S pod scaling orchestration."
    default     = "my-k8s-consumer-group"
}
