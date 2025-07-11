FROM nvidia/cuda:11.5.2-devel-centos7

USER root

ENV HOME=/home/jovyan \
    CONDA_DIR=/opt/conda \
    PATH=/opt/conda/bin:$PATH \
    ISCE3_BUILD_DIR=/home/jovyan/isce3/build \
    NOTEBOOK_PGE_DIR=/home/jovyan/alos-to-insar/notebook_pges

RUN set -ex \
    && useradd -m -s /bin/bash -N -u 1006 -r -d /home/jovyan jovyan \
    && echo "jovyan ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers \
    && yum clean all \
    && sed -i s/mirror.centos.org/vault.centos.org/g /etc/yum.repos.d/*.repo \
    && sed -i s/^\#.*baseurl=http/baseurl=http/g /etc/yum.repos.d/*.repo \
    && sed -i s/^mirrorlist=http/\#mirrorlist=http/g /etc/yum.repos.d/*.repo \
    && yum update -y \
    && yum install -y curl sudo less bzip2 zip unzip centos-release-scl-rh yum-utils which file make configure \
                      autoconf ImageMagick openssh-clients httpd environment-modules emacs.x86_64 \
    && yum clean all \
    && sed -i s/mirror.centos.org/vault.centos.org/g /etc/yum.repos.d/*.repo \
    && sed -i s/^\#.*baseurl=http/baseurl=http/g /etc/yum.repos.d/*.repo \
    && sed -i s/^mirrorlist=http/\#mirrorlist=http/g /etc/yum.repos.d/*.repo \
    && yum install -y make.x86_64 \
    && echo 'skip_missing_names_on_install=False' >> /etc/yum.conf \
    && rm -rf /var/cache/yum \
    && curl -sSL https://github.com/conda-forge/miniforge/releases/download/24.7.1-0/Mambaforge-24.7.1-0-Linux-x86_64.sh -o miniconda.sh \
    && bash miniconda.sh -b -p /opt/conda \
    && rm miniconda.sh \
    && touch /opt/conda/conda-meta/pinned \
    && mamba install -c conda-forge conda=23.10 \
    && mamba install -y git \
    && mamba config --set show_channel_urls True \
    && mamba init bash

# copy your repo into the docker container
################################################
COPY . $HOME/alos-to-insar
################################################

WORKDIR $HOME
    
SHELL ["mamba", "run", "-n", "base", "/bin/bash", "-c"]

RUN set -ex \
    && cd $HOME \
    && mamba install -y -c conda-forge nb_conda_kernels \
    && git clone https://github.com/hysds/notebook_pge_wrapper.git \
    && cd notebook_pge_wrapper \
    && pip install -e . \
    && mamba env create -f $HOME/alos-to-insar/environment.yml -n isce3_src \
    && mamba run -n isce3_src python -m ipykernel install --user --name isce3_src --display-name "isce3_src"

SHELL ["mamba", "run", "-n", "isce3_src", "/bin/bash", "-c"]

ENV CUDAHOSTCXX=x86_64-conda-linux-gnu-g++ \
    CC=x86_64-conda-linux-gnu-gcc \
    CXX=x86_64-conda-linux-gnu-g++

RUN set -ex \
    && cd $HOME \
    && git clone https://github.com/isce-framework/isce3.git \
    && mkdir $HOME/isce3/build \
    && cd $HOME/isce3/build \
    && git checkout v0.24.5 \
    && cmake -DCMAKE_INSTALL_PREFIX="${ISCE3_BUILD_DIR}" $HOME/isce3/ \
    && make -j 4 install

ENV PYTHONPATH="${NOTEBOOK_PGE_DIR}:${ISCE3_BUILD_DIR}/packages:${PYTHONPATH}" \
    LD_LIBRARY_PATH="${ISCE3_BUILD_DIR}/lib64:${LD_LIBRARY_PATH}"

# Fix permissions isues with loading up the bash profile and home profile, .cache folder, and everything else
RUN chmod 777 /home/jovyan/.bash_profile
RUN chmod 777 /home/jovyan/.cache
RUN chmod -R 777 /home/jovyan

CMD ["/bin/bash", "--login"]
