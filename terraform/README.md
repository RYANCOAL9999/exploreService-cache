## 🛠️ Step 1: Initialize Configuration Files

To prevent sensitive infrastructure definitions, variables, and cluster-specific configurations from leaking into version control, active configuration files are omitted by Git. You must initialize them from the provided `.example` templates.

Navigate to the terraform directory and copy the template files:

```bash
cd terraform/
# The main.tf.example has Ingress Resource for gRPC HTTP/2 Traffic Routing with load balancer, Please be carefully to control.
cp main.tf.example main.tf
cp providers.tf.example providers.tf
cp variables.tf.example variables.tf
```

### Configuration Updates Required (Choose Option A or Option B):

#### Option A: Hardcode Variables inside `variables.tf` (Simpler for local test)
Open your newly created `variables.tf` file and update the `default` values or `description` placeholders directly with your AWS environment metrics:
* Update `your_aws_region` (e.g., `"ap-east-1"`)
* Update `your_eks_cluster_name` (e.g., `"my-production-cluster"`)
* Update `your_aws_account_id` (e.g., `"123456789012"`)
* Update `allowed_source_ranges` (e.g., `["203.0.113.0/24"]` for restricting specific IP range if you want)

#### Option B: Pass Variables via Command-Line Flags (Recommended for CI/CD)
Leave `variables.tf` as it is, and explicitly declare every dynamic variable during runtime using `-var` flags.

---

## 🚀 Step 2: Provision Infrastructure via Terraform (With Remote Backend)

Run the following commands sequentially inside the `terraform/` directory. 

```bash
# 1. Initialize the workspace 
# 💡 Engineering Note: Terraform will automatically detect the "backend" block, 
# link to AWS S3/DynamoDB, and secure the state locking system.
terraform init

# 2. Preview the actions Terraform will perform 
terraform plan \
  -var="aws_region=your_actual_aws_region" \
  -var="eks_cluster_name=your_actual_eks_cluster_name" \
  -var="aws_account_id=your_actual_aws_account_id" \
  -var="aurora_endpoint=your_actual_aurora_postgres_endpoint" \
  -var="db_password=your_actual_aurora_secure_password"

# 3. Apply changes and execute one-click deployment to EKS
terraform apply \
  -var="aws_region=your_actual_aws_region" \
  -var="eks_cluster_name=your_actual_eks_cluster_name" \
  -var="aws_account_id=your_actual_aws_account_id" \
  -var="aurora_endpoint=your_actual_aurora_postgres_endpoint" \
  -var="db_password=your_actual_aurora_secure_password" \
  --auto-approve

# 4. Special Item for Only allows specific IP range
terraform apply \
  -var="aws_region=your_actual_aws_region" \
  -var="eks_cluster_name=your_actual_eks_cluster_name" \
  -var="aws_account_id=your_actual_aws_account_id" \
  -var="aurora_endpoint=your_actual_aurora_postgres_endpoint" \
  -var="db_password=your_actual_aurora_secure_password" \
  -var='allowed_source_ranges=["203.0.113.0/24"]' \
  --auto-approve
```
