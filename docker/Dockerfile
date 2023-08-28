FROM nvidia/cuda:11.5.2-devel-centos7

USER root

ENV HOME=/home/jovyan \
    CONDA_DIR=/opt/conda \
    PATH=/opt/conda/bin:$PATH

RUN set -ex \
    # miniconda setup
    && yum install -y wget \
    && yum update -y \
    && yum install -y curl sudo less bzip2 zip unzip centos-release-scl-rh yum-utils which file make configure \
                      autoconf ImageMagick openssh-clients httpd \
    && yum install -y environment-modules emacs.x86_64 make.x86_64 \
    && yum clean all \
    && echo 'skip_missing_names_on_install=False' >> /etc/yum.conf \
    && rm -rf /var/cache/yum

RUN set -ex \
    && curl -sSL https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -o miniconda.sh \
    && bash miniconda.sh -b -p /opt/conda \
    && rm miniconda.sh \
    && touch /opt/conda/conda-meta/pinned \
    && conda update -n base -c conda-forge conda \
    && conda install -y git \
    && conda config --set show_channel_urls True \
    && conda install -y -c conda-forge mamba \
    && conda init bash

# copy your repo into the docker container
################################################
COPY . $HOME/alos-to-insar
################################################

WORKDIR $HOME
    
SHELL ["conda", "run", "-n", "base", "/bin/bash", "-c"]

RUN mamba env create -f $HOME/alos-to-insar/environment.yml -p isce3_src

SHELL ["conda", "run", "-n", "isce3_src", "/bin/bash", "-c"]

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
    && cmake -DCMAKE_INSTALL_PREFIX="/home/jovyan/isce3_build" $HOME/isce3/ \
    && make -j 4 install

ENV PYTHONPATH="/home/jovyan/isce3_build/packages:${PYTHONPATH}"
ENV LD_LIBRARY_PATH="/home/jovyan/isce3_build/lib64:${LD_LIBRARY_PATH}"

CMD ["/bin/bash", "--login"]
