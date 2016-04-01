# Spark3DImageReconstruction
Implement 3D image reconstruction based on Maximum Likelihood Estimation Method (MLEM) algorithm using [Apache Spark](http://spark.apache.org/)

## Software Summary

- Implemented iterative MLEM algorithm for 3D image reconstruction of PET (positron emission tomography), SPECT (single photon emission computed tomography), and x-ray CT (computed tomography)
- The algorithm was written in the Apache Spark framework (version 1.2.1) using GraphX library
- Built the scala application via SBT

Software & System Setup
--------------------
- Apache Spark 1.2.1: spark-1.2.1-bin-hadoop2.4 pre-bulid binary
- Tested on a 32-core server system with 256GB RAM
- Store the scala code in [projectname]/src/main/scala/[code]
- Compile scala application via SBT
```
sbt package
```
- Submit the scala application via Spark (see [sparkjob.sh](sparkjob.sh))

Presentation
--------------------
- This work was presented at [Fully3D Image Reconstruction conference 2015](http://www.fully3d.org/)
- See [here](Fully3D2015/SparkMLEMPoster.pdf) for detailed poster presentation
