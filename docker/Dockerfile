# ==============================================================================
# STAGE 1: Builder
# Use the 'devel' image to get NVCC, GCC, CMake, and headers.
# ==============================================================================
FROM nvidia/cuda:12.8.1-devel-oraclelinux9 AS builder

USER root

# Define installation paths
ENV HOME=/home/jovyan \
    CONDA_DIR=/opt/conda \
    PATH=/opt/conda/bin:$PATH \
    # We install ISCE3 here, separate from the source code
    ISCE3_INSTALL_DIR=/opt/isce3

# 1. Install Build Dependencies
RUN set -ex \
    && yum update -y \
    && yum install -y curl bzip2 zip unzip yum-utils which file make \
       autoconf openssh-clients git gcc-c++ \
    && yum clean all \
    && rm -rf /var/cache/yum

# 2. Install Mamba
RUN curl -sSL https://github.com/conda-forge/miniforge/releases/download/24.7.1-0/Mambaforge-24.7.1-0-Linux-x86_64.sh -o miniconda.sh \
    && bash miniconda.sh -b -p /opt/conda \
    && rm miniconda.sh \
    && mamba init bash

# 3. Create Conda Environment
# We copy the spec file first to leverage Docker caching
COPY . $HOME/alos-to-insar
WORKDIR $HOME

RUN set -ex \
    && mamba install -y -c conda-forge nb_conda_kernels git cmake make \
    # Create the environment "isce3_src"
    && mamba create -y --file $HOME/alos-to-insar/docker/spec-file.txt -n isce3_src || \
       (echo "Warning: Assuming spec-file location from context, adjust if needed" && \
        mamba create -y -n isce3_src -c conda-forge ipykernel papermill) \
    # Install additional deps if not in spec file
    && mamba install -n isce3_src -c conda-forge --yes ipykernel papermill \
    # CLEANUP: Remove cached tarballs to save space in the copy
    && mamba clean --all -f -y

# 4. Build ISCE3 with NVCC
# Activate conda env for the build
SHELL ["mamba", "run", "-n", "isce3_src", "/bin/bash", "-c"]

ENV CUDAHOSTCXX=x86_64-conda-linux-gnu-g++ \
    CC=x86_64-conda-linux-gnu-gcc \
    CXX=x86_64-conda-linux-gnu-g++

RUN set -ex \
    && git clone --depth 1 --branch v0.25.7 https://github.com/isce-framework/isce3.git $HOME/isce3_src \
    && mkdir -p $HOME/isce3_src/build \
    && cd $HOME/isce3_src/build \
    # IMPORTANT: We install to /opt/isce3 (ISCE3_INSTALL_DIR)
    && cmake -DCMAKE_INSTALL_PREFIX="${ISCE3_INSTALL_DIR}" -DCMAKE_BUILD_TYPE=Release .. \
    && make -j 4 install

# 5. Install Notebook Wrapper
RUN set -ex \
    && git clone https://github.com/hysds/notebook_pge_wrapper.git $HOME/notebook_pge_wrapper \
    && cd $HOME/notebook_pge_wrapper \
    && pip install . \
    && python -m ipykernel install --user --name isce3_src --display-name "isce3_src"


# ==============================================================================
# STAGE 2: Final Runtime
# Use 'runtime' image. Contains CUDA shared libraries but NO compiler (nvcc).
# ==============================================================================
FROM nvidia/cuda:12.8.1-runtime-oraclelinux9

USER root

ENV HOME=/home/jovyan \
    CONDA_DIR=/opt/conda \
    PATH=/opt/conda/bin:$PATH \
    ISCE3_INSTALL_DIR=/opt/isce3 \
    NOTEBOOK_PGE_DIR=/home/jovyan/alos-to-insar/notebook_pges

# 1. Setup User and Minimal Runtime Dependencies
# We only install what is needed to RUN the code (git, curl, etc), not build it.
RUN set -ex \
    && useradd -m -s /bin/bash -N -u 1006 -r -d /home/jovyan jovyan \
    && echo "jovyan ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers \
    && yum update -y \
    && yum install -y curl sudo less unzip git openssh-clients \
    && yum clean all \
    && rm -rf /var/cache/yum

# 2. COPY Artifacts from Builder
# Copy the completely built Conda environment
COPY --from=builder /opt/conda /opt/conda
# Copy the compiled ISCE3 binaries/libs (Clean of source code and .o files)
COPY --from=builder /opt/isce3 /opt/isce3
# Copy the Jupyter kernels
COPY --from=builder /home/jovyan/.local /home/jovyan/.local

# 3. Copy Application Code
COPY . $HOME/alos-to-insar

# 4. Final Permissions and Env Setup
RUN chown -R jovyan:users $HOME \
    && chmod 777 $HOME/alos-to-insar \
    && echo ". /opt/conda/etc/profile.d/conda.sh" >> $HOME/.bash_profile \
    && echo "conda activate isce3_src" >> $HOME/.bash_profile \
    && mkdir -p $HOME/.cache && chmod 777 $HOME/.cache

# Point Python and Linker to the custom install location
ENV PYTHONPATH="${NOTEBOOK_PGE_DIR}:${ISCE3_INSTALL_DIR}/packages:${PYTHONPATH}" \
    LD_LIBRARY_PATH="${ISCE3_INSTALL_DIR}/lib64:${LD_LIBRARY_PATH}"

WORKDIR $HOME
USER jovyan
CMD ["/bin/bash", "--login"]