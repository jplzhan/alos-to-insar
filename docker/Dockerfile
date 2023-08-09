FROM nvidia/cuda:11.5.2-devel-centos7

USER root

ENV HOME=/home/ops \
    CONDA_DIR=/opt/conda \
    PATH=/opt/conda/bin:$PATH

RUN set -ex \
    # miniconda setup
    && curl -sSL https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -o miniconda.sh \
    && bash miniconda.sh -b -p /opt/conda \
    && rm miniconda.sh \
    && touch /opt/conda/conda-meta/pinned \
    && conda update -n base -c conda-forge conda \
    && conda install -y git \
    && conda config --set show_channel_urls True

# copy your repo into the docker container
################################################
COPY . $HOME/alos-to-insar
################################################

WORKDIR $HOME

RUN set -ex \
    && conda env update -n base -f $HOME/alos-to-insar/environment.yml \
    && conda init bash

SHELL ["conda", "run", "-n", "base", "/bin/bash", "-c"]

RUN set -ex \
    && cd $HOME \
    && conda install -y -c conda-forge nb_conda_kernels \
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
    && cmake -DCMAKE_INSTALL_PREFIX="/home/ops/isce3_build" $HOME/isce3/ \
    && make -j 4 install

ENV PYTHONPATH="/home/ops/isce3_build/packages:${PYTHONPATH}"
ENV LD_LIBRARY_PATH="/home/ops/isce3_build/lib64:${LD_LIBRARY_PATH}"

CMD ["/bin/bash", "--login"]
