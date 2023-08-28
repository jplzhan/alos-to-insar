FROM nvidia/cuda:11.5.2-devel-centos7

USER root

ENV HOME=/home/jovyan \
    CONDA_DIR=/opt/conda \
    PATH=/opt/conda/bin:$PATH

RUN set -ex \
    && curl -sSL https://github.com/conda-forge/miniforge/releases/latest/download/Mambaforge-Linux-x86_64.sh -o miniconda.sh \
    && bash miniconda.sh -b -p /opt/conda \
    && rm miniconda.sh \
    && touch /opt/conda/conda-meta/pinned \
    && mamba update -c conda-forge conda \
    && mamba install -y git \
    && mamba config --set show_channel_urls True \
    && mamba init bash

# copy your repo into the docker container
################################################
COPY . $HOME/alos-to-insar
################################################

WORKDIR $HOME
    
SHELL ["mamba", "run", "-n", "base", "/bin/bash", "-c"]

RUN mamba env create -f $HOME/alos-to-insar/environment.yml -p isce3_src

SHELL ["mamba", "run", "-n", "isce3_src", "/bin/bash", "-c"]

RUN set -ex \
    && cd $HOME \
    && mamba install -y -c conda-forge nb_conda_kernels \
    && git clone https://github.com/hysds/notebook_pge_wrapper.git \
    && cd notebook_pge_wrapper \
    && pip install -e .

ENV CUDAHOSTCXX=x86_64-conda-linux-gnu-g++ \
    CC=x86_64-conda-linux-gnu-gcc \
    CXX=x86_64-conda-linux-gnu-g++

RUN set -ex \
    && cd $HOME \
    && git clone https://github.com/isce-framework/isce3.git \
    && mkdir $HOME/isce3_build

RUN set -ex \
    && cd $HOME/isce3_build \
    && cmake -DCMAKE_INSTALL_PREFIX="/home/jovyan/isce3_build" $HOME/isce3/ \
    && make -j 4 install

ENV PYTHONPATH="/home/jovyan/isce3_build/packages:${PYTHONPATH}"
ENV LD_LIBRARY_PATH="/home/jovyan/isce3_build/lib64:${LD_LIBRARY_PATH}"

CMD ["/bin/bash", "--login"]
