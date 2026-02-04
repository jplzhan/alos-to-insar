# ==============================================================================
# STAGE 1: Builder (Compiles ISCE3, Setups Envs)
# ==============================================================================
FROM nvidia/cuda:12.8.1-devel-oraclelinux9 AS builder

USER root

ENV HOME=/home/jovyan \
    CONDA_DIR=/opt/conda \
    PATH=/opt/conda/bin:$PATH \
    ISCE3_INSTALL_DIR=/opt/isce3

# 1. Install System Build Dependencies
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

WORKDIR $HOME

# 3. Clone ISCE3 (Source for spec-file and build)
#RUN git clone --depth 1 --branch v0.25.7 https://github.com/isce-framework/isce3.git
RUN mkdir isce3 && cd isce3 && \
    git init && \
    git remote add origin https://github.com/isce-framework/isce3.git && \
    git fetch --depth 1 origin fddc4eacd02655af77f026943bd300416c960441 && \
    git checkout FETCH_HEAD



# 4. Create Environments & Install Dependencies (IN BOTH ENVS)
RUN set -ex \
    # A. Install dependencies into BASE
    && mamba install -n base -y -c conda-forge \
       nb_conda_kernels \
       ipykernel \
       papermill \
       git cmake make \
    # B. Create ISCE3_SRC environment
    && mamba create -y --file $HOME/isce3/tools/imagesets/oracle8conda/dev/spec-file.txt -n isce3_src \
    # C. Install dependencies into ISCE3_SRC
    && mamba install -n isce3_src -c conda-forge --yes \
       ipykernel \
       papermill \
       gxx_linux-64 \
       gcc_linux-64 \
       cmake \
       make \
    && mamba clean --all -f -y

# 5. Build ISCE3
# We use the compilers we just installed in isce3_src
SHELL ["mamba", "run", "-n", "isce3_src", "/bin/bash", "-c"]

ENV CUDAHOSTCXX=x86_64-conda-linux-gnu-g++ \
    CC=x86_64-conda-linux-gnu-gcc \
    CXX=x86_64-conda-linux-gnu-g++

RUN set -ex \
    && mkdir -p $HOME/isce3/build \
    && cd $HOME/isce3/build \
    && cmake -DCMAKE_INSTALL_PREFIX="${ISCE3_INSTALL_DIR}" -DCMAKE_BUILD_TYPE=Release .. \
    && make -j 4 install

# 6. Install Notebook Wrapper (IN BOTH ENVS)
# We use a single RUN command to handle both installs to save layers
RUN set -ex \
    && git clone https://github.com/hysds/notebook_pge_wrapper.git $HOME/notebook_pge_wrapper \
    && cd $HOME/notebook_pge_wrapper \
    # Install in BASE
    && mamba run -n base pip install . \
    # Install in ISCE3_SRC
    && mamba run -n isce3_src pip install . \
    # Register kernel for isce3_src
    && mamba run -n isce3_src python -m ipykernel install --user --name isce3_src --display-name "isce3_src"


# ==============================================================================
# STAGE 2: Final Runtime
# ==============================================================================
FROM nvidia/cuda:12.8.1-runtime-oraclelinux9

USER root

ENV HOME=/home/jovyan \
    CONDA_DIR=/opt/conda \
    PATH=/opt/conda/bin:$PATH \
    ISCE3_INSTALL_DIR=/opt/isce3 \
    NOTEBOOK_PGE_DIR=/home/jovyan/alos-to-insar/notebook_pges

# 1. Setup User and Runtime Deps
RUN set -ex \
    && useradd -m -s /bin/bash -N -u 1006 -r -d /home/jovyan jovyan \
    && echo "jovyan ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers \
    && yum update -y \
    && yum install -y curl sudo less unzip git openssh-clients \
    && yum clean all \
    && rm -rf /var/cache/yum

# 2. COPY Artifacts from Builder
# This copies BOTH environments (base and isce3_src) because they live inside /opt/conda
COPY --from=builder /opt/conda /opt/conda
COPY --from=builder /opt/isce3 /opt/isce3
COPY --from=builder /home/jovyan/.local /home/jovyan/.local

# 3. Copy Application Code
COPY . $HOME/alos-to-insar

# 4. Final Permissions and Env
RUN chown -R jovyan:users $HOME \
    && chmod -R 777 $HOME \
    && echo ". /opt/conda/etc/profile.d/conda.sh" >> $HOME/.bash_profile \
    && echo "conda activate isce3_src" >> $HOME/.bash_profile \
    && mkdir -p $HOME/.cache && chmod 777 $HOME/.cache

ENV PYTHONPATH="${NOTEBOOK_PGE_DIR}:${ISCE3_INSTALL_DIR}/packages:${PYTHONPATH}" \
    LD_LIBRARY_PATH="${ISCE3_INSTALL_DIR}/lib64:${LD_LIBRARY_PATH}"

WORKDIR $HOME
USER jovyan
CMD ["/bin/bash", "--login"]