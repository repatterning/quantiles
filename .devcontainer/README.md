<br>

The requirements.txt file includes 

* dask[complete]

and 

* --extra-index-url=https://pypi.nvidia.com
  * cudf-cu12==25.4.*
  * dask-cudf-cu12==25.4.*

for GitHub Actions code analysis purposes.  These packages are included in the base image

> nvcr.io/nvidia/rapidsai/base:25.04-cuda12.8-py3.12

by default.  Hence, during the Dockerfile building steps each Docker file filters out the above packages.

<br>
<br>

<br>
<br>

<br>
<br>

<br>
<br>
