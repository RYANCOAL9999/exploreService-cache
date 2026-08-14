# ==========================================================================================================================================
# 1. Infrastructure Data Layers
# ==========================================================================================================================================
data "aws_eks_cluster" "eks" {
    name = var.eks_cluster_name
}

# ==========================================================================================================================================
# 2. Subnet Groups Layers (Defines physical private subnet positions in AWS data centers)
# ==========================================================================================================================================

resource "aws_elasticache_subnet_group" "redis_subnets" {
    name       = "explore-cache-redis-subnet-group"
    subnet_ids = data.aws_eks_cluster.eks.vpc_config.subnet_ids
}

# ==========================================================================================================================================
# 3. AWS Network Security Security Layer (Locks ElastiCache directly to EKS VPC boundaries)
# ==========================================================================================================================================
resource "aws_security_group" "redis_sg" {
    name        = "explore-service-cache-redis-sg"
    description = "Security firewall for the AWS ElastiCache Redis replication group"
    vpc_id      = data.aws_eks_cluster.eks.vpc_config.vpc_id

    # Inbound Rules: Strictly accept traffic only from EKS compute worker nodes on standard Redis port 6379
    ingress {
        description     = "Allow persistent gRPC connections from EKS pods"
        from_port       = 6379
        to_port         = 6379
        protocol        = "tcp"
        security_groups = [data.aws_eks_cluster.eks.vpc_config.cluster_security_group_id]
    }

    egress {
        from_port   = 0
        to_port     = 0
        protocol    = "-1"
        cidr_blocks = ["0.0.0.0/0"]
    }
}

# ==========================================================================================================================================
# 4. AWS ElastiCache for Redis (Native AWS Managed Cluster Instance)
# ==========================================================================================================================================
resource "aws_elasticache_replication_group" "redis_cluster" {
    replication_group_id          = "explore-service-cache-redis"
    description                   = "Production-grade managed Redis cluster for extreme O(1) performance"
    node_type                     = var.redis_node_type
    port                          = 6379
    
    # Engine configuration setup
    engine                        = "redis"
    engine_version                = "7.1"
    parameter_group_name          = "default.redis7"

    # High-Availability Topologies
    num_cache_clusters            = 2 # Creates 1 Master node and 1 Read-Replica node automatically
    automatic_failover_enabled    = true

    # Security and network stitching links
    subnet_group_name          = aws_elasticache_subnet_group.redis_subnets.name
    security_group_ids         = [aws_security_group.redis_sg.id]
    
    # Data retention strategy for safety
    snapshot_retention_limit   = 7
    snapshot_window            = "02:00-03:00"

    tags = {
        Environment = "production"
        Service     = "explore-cache"
    }
}

resource "aws_msk_serverless_cluster" "kafka_bus" {
    cluster_name = "explore-core-event-bus"
    
    vpc_config {
        subnet_ids         = data.aws_eks_cluster.eks.vpc_config.subnet_ids
        security_groups    = [aws_security_group.kafka_sg.id]
    }

    client_authentication {
        sasl {
            iam {
                enabled = true # Enforces enterprise-grade AWS IAM Access Control Policy
            }
        }
    }
}

# ==============================================================================
# Extract: AWS ElastiCache Serverless for Redis (Replaces Provisioned Clusters)
# ==============================================================================
# This single resource completely eliminates the need for provisioned clusters, 
# explicit node counting (num_cache_clusters), or managing failover configurations.
resource "aws_elasticache_serverless" "explore_redis" {
    name        = "explore-service-cache-redis"
    description = "Production serverless redis engine for high-throughput likes filtering"
    
    # Standard core caching open-source engine protocol binding
    engine = "redis"

    # NETWORK BRIDGING: Mounts the elastic caching infrastructure directly into your EKS subnets
    security_group_ids = [aws_security_group.redis_sg.id]
    subnet_ids         = aws_elasticache_subnet_group.redis_subnets.subnet_ids

    major_engine_version = "7" # Dynamically patches and scales within Redis 7.x baselines

    tags = {
        Environment = "production"
        ManagedBy   = "terraform"
    }
}

# below that is for if don't have redis in aws.


# ==========================================================================================================================================
# 5. Kubernetes Private Secret Sandbox
# ==========================================================================================================================================
resource "kubernetes_secret" "explore_service_cache_secret" {
    metadata {
        name      = "explore-service-cache-secret"
        namespace = "default"
    }

    type = "Opaque"

    data = {
        # Placeholders retained for future TLS mesh certificates mapping parameters
    }
}

# ==========================================================================================================================================
# Kubernetes Deployment Framework (Reconfigured for Cache Only Microservice Execution)
# ==========================================================================================================================================
resource "kubernetes_deployment" "explore_service_cache" {
    metadata {
        name      = "explore-service-cache-deployment"
        namespace = "default"
        labels = {
            app = "exploreServiceCache"
        }
    }

    spec {
        replicas = 2 # 🚀 

        strategy {
            type = "RollingUpdate"
            rolling_update {
                max_surge       = "1"
                max_unavailable = "0"
            }
        }

        selector {
            match_labels = {
                app = "exploreServiceCache"
            }
        }

        template {
            metadata {
                labels = {
                    app = "exploreServiceCache"
                }
            }

            spec {
                container {
                    name              = "exploreServiceCache"
                    image             = "${var.aws_account_id}.dkr.ecr.${var.aws_region}://"
                    image_pull_policy = "IfNotPresent"

                    port {
                        container_port = 50052
                        name           = "grpc"
                    }

                    security_context {
                        run_as_non_root = true
                        run_as_user     = 10001
                    }

                    env {
                        name  = "PORT"
                        value = "50052"
                    }

                    env {
                        name  = "REDIS_ADDR"
                        value = "${var.redis_endpoint}:6379"
                    }

                    # 💡 Added: Dynamic injection of AWS MSK Serverless Brokers Endpoint into Go application
                    env {
                        name  = "KAFKA_BROKERS"
                        value = join(",", aws_msk_serverless_cluster.kafka_bus.vpc_config[*].subnet_ids) # Or direct bootstrap broker string output
                    }

                    env {
                        name  = "KAFKA_TOPIC"
                        value = "myQueue"
                    }

                    resources {
                        limits = {
                            cpu    = "500m"
                            memory = "512Mi"
                        }
                        requests = {
                            cpu    = "100m"
                            memory = "256Mi"
                        }
                    }
                }
            }
        }
    }
}

# ==========================================================================================================================================
# 7. Kubernetes Service Layer
# ==========================================================================================================================================
resource "kubernetes_service" "explore_service_cache" {
    metadata {
        name      = "explore-service-cache-service"
        namespace = "default"
    }

    spec {
        selector = {
            app = "exploreServiceCache"
        }

        port {
            protocol    = "TCP"
            port        = 50052
            target_port = 50052

            # NodePort (The Direct-to-Machine Way) method
            # node_port   = 30125 # Outside client connect to NodeIP
        }

        type = "ClusterIP" # 💡 Using ClusterIP for internal communication within the Kubernetes cluster

        # NodePort (The Direct-to-Machine Way) method
        # type = "NodePort" # 💡 Using NodePort for external access to the service through a specific port on each node
        # Ensures traffic hits pods on the landing node directly, reducing network jitter
        # external_traffic_policy = "Local" 
    }
}

# 💡 Ingress Resource for gRPC HTTP/2 Traffic Routing with load balancer
# resource "kubernetes_ingress" "explore_service_cache" {
#     metadata {
#         name      = "explore-service-cache-ingress"
#         namespace = "default"
#         annotations = {
#             # FORCES the backend proxy connection to use pure gRPC (HTTP/2)
#             "nginx.ingress.kubernetes.io/backend-protocol" = "GRPC"

#             # DROPS all unencrypted HTTP traffic entirely (Only allows ALPN HTTP/2)
#             "nginx.ingress.kubernetes.io/ssl-redirect" = "true"

#             # CRITICAL FOR STREAMS: Prevents NGINX from cutting off long-lived grpc bidirectional streams
#             "nginx.ingress.kubernetes.io/proxy-read-time" = "86400"
#             "nginx.ingress.kubernetes.io/proxy-send-time" = "86400"

#             # Removes unnecessary HTTP buffer headers that break strict grpc payloads
#             "nginx.ingress.kubernetes.io/proxy-buffering" = "off"
#         }
#     }

#     spec {
#         ingress_class_name = "nginx"

#         # Enforces TLS termination-mandatory for production grpc (HTTP/2)
#         tls {
#             hosts       = ["grpc.example.com"]
#             secret_name = "explore-service-cache-secret" 
#         }

#         rule {
#             host = "grpc.example.com"

#             # Note: K8s schema naming dictates the use of the "http" block here,
#             # but the annotations above redefine the entire server block to run on pure grpc.
#             http {
#                 path {
#                     # Catch-all for gRPC packages (Matches: /PackageName.ServiceName/MethodName)
#                     path      = "/"
#                     path_type = "Prefix"

#                     backend {
#                         service {
#                             name = kubernetes_service_v1.grpc_backend_service.metadata[0].name
#                             port {
#                                 number = 50052 # GRPC Port
#                             }
#                         }
#                     }
#                 }
#             }
#         }
#     }
# }