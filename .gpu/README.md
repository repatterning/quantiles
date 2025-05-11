
<br>

## Environments

**Note**, the [requirements.txt](requirements.txt) file includes

* dask[complete]

and

* --extra-index-url=https://pypi.nvidia.com
  * cudf-cu12==25.4.*
  * dask-cudf-cu12==25.4.*

for GitHub Actions code analysis purposes.  These packages are included in the base image

> nvcr.io/nvidia/rapidsai/base:25.04-cuda12.8-py3.12

by default.  Hence, during the Dockerfile building steps each applicable Docker file filters out the above packages.

<br>

### Remote Development

For this Python project/template, the remote development environment requires

* [Dockerfile](../.gpu/Dockerfile)
* [requirements.txt](../.gpu/requirements.txt)

An image is built via the command

```shell
docker build . --file .devcontainer/Dockerfile -t distributing
```

On success, the output of

```shell
docker images
```

should include

<br>

| repository   | tag    | image id | created  | size     |
|:-------------|:-------|:---------|:---------|:---------|
| distributing | latest | $\ldots$ | $\ldots$ | $\ldots$ |


<br>

Subsequently, run an instance of the image `distributing` via:


```shell
docker run --rm --gpus all -i -t -p 8000:8000 -w /app --mount 
    type=bind,src="$(pwd)",target=/app 
      -v ~/.aws:/home/rapids/.aws distributing
```

<br>

Herein, `-p 8000:8000` maps the host port `8000` to container port `8000`.  Note, the container's working environment,
i.e., `-w`, must be inline with this project's top directory.  Additionally, visit the links for more about the flags/options $\rightarrow$

* --rm: [automatically remove container](https://docs.docker.com/engine/reference/commandline/run/#:~:text=a%20container%20exits-,%2D%2Drm,-Automatically%20remove%20the)
* -i: [interact](https://docs.docker.com/engine/reference/commandline/run/#:~:text=and%20reaps%20processes-,%2D%2Dinteractive,-%2C%20%2Di)
* -t: [tag](https://docs.docker.com/get-started/02_our_app/#:~:text=Finally%2C%20the-,%2Dt,-flag%20tags%20your)
* -p: [publish the container's port/s to the host](https://docs.docker.com/engine/reference/commandline/run/#:~:text=%2D%2Dpublish%20%2C-,%2Dp,-Publish%20a%20container%E2%80%99s)
* --mount type=bind: [a bind mount](https://docs.docker.com/engine/storage/bind-mounts/#syntax)
* -v: [volume](https://docs.docker.com/engine/storage/volumes/)

<br>

The part `-v ~/.aws:/home/rapids/.aws` ascertains Amazon Web Services interactions via containers. Get the name of a running instance of ``distributing`` via:

```shell
docker ps --all
```

Never deploy a root container.  The deployed version of this container image will include

```shell

RUN apt update && apt -q -y upgrade && apt -y install sudo && sudo apt -y install graphviz && \
    sudo apt -y install wget && sudo apt -y install curl && sudo apt -y install unzip && \
    sudo apt -y install openjdk-17-jdk && \
    curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip" && \
    unzip /tmp/awscliv2.zip -d /tmp/ && cd /tmp && sudo ./aws/install && cd ~ && \
    pip install --upgrade pip && \
    pip install $(grep -ivE "dask\[complete\]|cudf-cu12|dask-cudf-cu12" /app/requirements.txt) --no-cache-dir && \
    mkdir /app/warehouse && \
    chown -R rapids /app/warehouse

USER rapids
```


<br>
<br>


## References

Large Scale Computation

* [DASK, EMR, Clusters](https://yarn.dask.org/en/latest/aws-emr.html)
* [Use the Nvidia RAPIDS Accelerator for Apache Spark](https://docs.aws.amazon.com/emr/latest/ReleaseGuide/emr-spark-rapids.html)
  * [RAPIDS Accelerator for Apache Spark Deployment Guide](https://docs.nvidia.com/ai-enterprise/deployment/spark-rapids-accelerator/latest/emr.html)
  * [NVIDIA AI Enterprise with RAPIDS Accelerator Deployment Guide](https://docs.nvidia.com/ai-enterprise/deployment/spark-rapids-accelerator/latest/index.html)
  * [rapids.ai & Amazon EMR](https://docs.nvidia.com/ai-enterprise/deployment/spark-rapids-accelerator/latest/emr.html)
  * [rapids.ai, EMR, EKS](https://aws.amazon.com/blogs/containers/run-spark-rapids-ml-workloads-with-gpus-on-amazon-emr-on-eks/)
  * [Quickstart](https://docs.nvidia.com/spark-rapids/user-guide/latest/qualification/quickstart.html)
  * Images: **a.** [rapids.ai & EMR](https://gallery.ecr.aws/emr-on-eks/spark/emr-7.0.0-spark-rapids), **b.** [EMR](https://gallery.ecr.aws/emr-on-eks?page=1)
  * [Amazon EMR & Dockerfile](https://github.com/awslabs/data-on-eks/blob/main/ai-ml/emr-spark-rapids/examples/xgboost/Dockerfile)
  * [cutting cost](https://developer.nvidia.com/blog/accelerated-data-analytics-faster-time-series-analysis-with-rapids-cudf/)
* [Getting Started: Amazon EMR, Python, Spark](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-gs.html#emr-getting-started-plan-and-configure)
* [Configure Docker for use with Amazon EMR clusters](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-plan-docker.html)
* [EMR & Custom Images](https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/docker-custom-images-steps.html)
* [EMR Pricing](https://aws.amazon.com/emr/pricing/)

<br>

ECS
* [Amazon ECS task definitions for GPU workloads](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/ecs-gpu.html)
* [Run Amazon ECS or Fargate tasks with Step Functions](https://docs.aws.amazon.com/step-functions/latest/dg/connect-ecs.html)

<br>

BATCH
* [Use a GPU workload AMI](https://docs.aws.amazon.com/batch/latest/userguide/batch-gpu-ami.html)


<br>

Engineering
* [Dockerfile](https://docs.docker.com/reference/dockerfile/)
* [requirements.txt](https://pip.pypa.io/en/stable/reference/requirements-file-format/)

<br>
<br>

<br>
<br>

<br>
<br>

<br>
<br>
