#!/bin/bash -e

if [ ! -e dist/ami/build_ami.sh ]; then
    echo "run build_ami.sh in top of scylla dir"
    exit 1
fi

print_usage() {
    echo "build_ami.sh --localrpm --repo [URL] --target [distribution]"
    echo "  --localrpm  deploy locally built rpms"
    echo "  --repo  repository for both install and update, specify .repo/.list file URL"
    echo "  --repo-for-install  repository for install, specify .repo/.list file URL"
    echo "  --repo-for-update  repository for update, specify .repo/.list file URL"
    echo "  --target    specify target distribution"
    exit 1
}
LOCALRPM=0
TARGET=centos
while [ $# -gt 0 ]; do
    case "$1" in
        "--localrpm")
            LOCALRPM=1
            shift 1
            ;;
        "--repo")
            REPO=$2
            shift 2
            ;;
        "--repo-for-install")
            REPO_FOR_INSTALL=$2
            shift 2
            ;;
        "--repo-for-update")
            REPO_FOR_UPDATE=$2
            shift 2
            ;;
        "--target")
            TARGET="$2"
            shift 2
            ;;
        *)
            print_usage
            ;;
    esac
done

is_redhat_variant() {
    [ -f /etc/redhat-release ]
}
is_debian_variant() {
    [ -f /etc/debian_version ]
}

pkg_install() {
    if is_redhat_variant; then
        sudo yum install -y $1
    elif is_debian_variant; then
        sudo apt-get install -y $1
    else
        echo "Requires to install following command: $1"
        exit 1
    fi
}

case "$TARGET" in
    "centos")
        AMI=ami-ae7bfdb8
        REGION=us-east-1
        SSH_USERNAME=centos
        ;;
    "trusty")
        AMI=ami-ff427095
        REGION=us-east-1
        SSH_USERNAME=ubuntu
        ;;
    "xenial")
        AMI=ami-da05a4a0
        REGION=us-east-1
        SSH_USERNAME=ubuntu
        ;;
    *)
        echo "build_ami.sh does not supported this distribution."
        exit 1
        ;;
esac

if [ $LOCALRPM -eq 1 ]; then
    sudo rm -rf build/*
    REPO=`./scripts/scylla_current_repo --target $TARGET`
    INSTALL_ARGS="$INSTALL_ARGS --localrpm --repo $REPO"
    if [ ! -f /usr/bin/git ]; then
        pkg_install git
    fi

    if [ "$TARGET" = "centos" ]; then
        if [ ! -f dist/ami/files/scylla-enterprise.x86_64.rpm ] || [ ! -f dist/ami/files/scylla-enterprise-kernel-conf.x86_64.rpm ] || [ ! -f dist/ami/files/scylla-enterprise-conf.x86_64.rpm ] || [ ! -f dist/ami/files/scylla-enterprise-server.x86_64.rpm ] || [ ! -f dist/ami/files/scylla-enterprise-debuginfo.x86_64.rpm ]; then
            dist/redhat/build_rpm.sh --dist --target epel-7-x86_64
            cp build/rpms/scylla-enterprise-`cat build/SCYLLA-VERSION-FILE`-`cat build/SCYLLA-RELEASE-FILE`.*.x86_64.rpm dist/ami/files/scylla-enterprise.x86_64.rpm
            cp build/rpms/scylla-enterprise-kernel-conf-`cat build/SCYLLA-VERSION-FILE`-`cat build/SCYLLA-RELEASE-FILE`.*.x86_64.rpm dist/ami/files/scylla-enterprise-kernel-conf.x86_64.rpm
            cp build/rpms/scylla-enterprise-conf-`cat build/SCYLLA-VERSION-FILE`-`cat build/SCYLLA-RELEASE-FILE`.*.x86_64.rpm dist/ami/files/scylla-enterprise-conf.x86_64.rpm
            cp build/rpms/scylla-enterprise-server-`cat build/SCYLLA-VERSION-FILE`-`cat build/SCYLLA-RELEASE-FILE`.*.x86_64.rpm dist/ami/files/scylla-enterprise-server.x86_64.rpm
            cp build/rpms/scylla-enterprise-debuginfo-`cat build/SCYLLA-VERSION-FILE`-`cat build/SCYLLA-RELEASE-FILE`.*.x86_64.rpm dist/ami/files/scylla-enterprise-debuginfo.x86_64.rpm
        fi
        if [ ! -f dist/ami/files/scylla-enterprise-jmx.noarch.rpm ]; then
            cd build
            git clone --depth 1 https://github.com/scylladb/scylla-enterprise-jmx.git
            cd scylla-jmx
            dist/redhat/build_rpm.sh --target epel-7-x86_64
            cd ../..
            cp build/scylla-enterprise-jmx/build/rpms/scylla-enterprise-jmx-`cat build/scylla-enterprise-jmx/build/SCYLLA-VERSION-FILE`-`cat build/scylla-enterprise-jmx/build/SCYLLA-RELEASE-FILE`.*.noarch.rpm dist/ami/files/scylla-enterprise-jmx.noarch.rpm
        fi
        if [ ! -f dist/ami/files/scylla-enterprise-tools.noarch.rpm ] || [ ! -f dist/ami/files/scylla-enterprise-tools-core.noarch.rpm ]; then
            cd build
            git clone --depth 1 https://github.com/scylladb/scylla-enterprise-tools-java.git
            cd scylla-enterprise-tools-java
            dist/redhat/build_rpm.sh --target epel-7-x86_64
            cd ../..
            cp build/scylla-enterprise-tools-java/build/rpms/scylla-enterprise-tools-`cat build/scylla-enterprise-tools-java/build/SCYLLA-VERSION-FILE`-`cat build/scylla-enterprise-tools-java/build/SCYLLA-RELEASE-FILE`.*.noarch.rpm dist/ami/files/scylla-enterprise-tools.noarch.rpm
            cp build/scylla-enterprise-tools-java/build/rpms/scylla-enterprise-tools-core-`cat build/scylla-enterprise-tools-java/build/SCYLLA-VERSION-FILE`-`cat build/scylla-enterprise-tools-java/build/SCYLLA-RELEASE-FILE`.*.noarch.rpm dist/ami/files/scylla-enterprise-tools-core.noarch.rpm
        fi
    else
        if [ ! -f dist/ami/files/scylla-server_amd64.deb ]; then
            ./scripts/git-archive-all --force-submodules --prefix scylla build/scylla.tar
            tar -C build/ -xvpf build/scylla.tar
            cd build/scylla
            dist/debian/build_deb.sh --dist --target $TARGET
            cd ../..
            cp build/scylla/build/debs/scylla_`cat build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/'`-`cat build/SCYLLA-RELEASE-FILE`-0ubuntu1~${TARGET}_amd64.deb dist/ami/files/scylla_amd64.deb
            cp build/scylla/build/debs/scylla-kernel-conf_`cat build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/'`-`cat build/SCYLLA-RELEASE-FILE`-0ubuntu1~${TARGET}_amd64.deb dist/ami/files/scylla-kernel-conf_amd64.deb
            cp build/scylla/build/debs/scylla-conf_`cat build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/'`-`cat build/SCYLLA-RELEASE-FILE`-0ubuntu1~${TARGET}_amd64.deb dist/ami/files/scylla-conf_amd64.deb
            cp build/scylla/build/debs/scylla-server_`cat build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/'`-`cat build/SCYLLA-RELEASE-FILE`-0ubuntu1~${TARGET}_amd64.deb dist/ami/files/scylla-server_amd64.deb
            cp build/scylla/build/debs/scylla-server-dbg_`cat build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/'`-`cat build/SCYLLA-RELEASE-FILE`-0ubuntu1~${TARGET}_amd64.deb dist/ami/files/scylla-server-dbg_amd64.deb
||||||| merged common ancestors
        sudo apt-get install -y git
        if [ ! -f dist/ami/files/scylla-server_amd64.deb ]; then
            if [ ! -f ../scylla-server_`cat build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/'`-`cat build/SCYLLA-RELEASE-FILE`-ubuntu1_amd64.deb ]; then
                echo "Build .deb before running build_ami.sh"
                exit 1
            fi
            cp ../scylla_`cat build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/'`-`cat build/SCYLLA-RELEASE-FILE`-ubuntu1_amd64.deb dist/ami/files/scylla_amd64.deb
            cp ../scylla-kernel-conf_`cat build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/'`-`cat build/SCYLLA-RELEASE-FILE`-ubuntu1_amd64.deb dist/ami/files/scylla-kernel-conf_amd64.deb
            cp ../scylla-conf_`cat build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/'`-`cat build/SCYLLA-RELEASE-FILE`-ubuntu1_amd64.deb dist/ami/files/scylla-conf_amd64.deb
            cp ../scylla-server_`cat build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/'`-`cat build/SCYLLA-RELEASE-FILE`-ubuntu1_amd64.deb dist/ami/files/scylla-server_amd64.deb
            cp ../scylla-server-dbg_`cat build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/'`-`cat build/SCYLLA-RELEASE-FILE`-ubuntu1_amd64.deb dist/am        fi
        if [ ! -f dist/ami/files/scylla-enterprise-jmx_all.deb ]; then
            cd build
            git clone --depth 1 https://github.com/scylladb/scylla-enterprise-jmx.git
            cd scylla-enterprise-jmx
            dist/debian/build_deb.sh --target $TARGET
            cd ../..
            cp build/scylla-enterprise-jmx/build/debs/scylla-jmx_`cat build/scylla-enterprise-jmx/build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/'`-`cat build/scylla-jmx/build/SCYLLA-RELEASE-FILE`-0ubuntu1~${TARGET}_all.deb dist/ami/files/scylla-enterprise-jmx_all.deb
        fi
        if [ ! -f dist/ami/files/scylla-enterprise-tools_all.deb ]; then
            cd build
            git clone --depth 1 https://github.com/scylladb/scylla-enterprise-tools-java.git
            cd scylla-enterprise-tools-java
            dist/debian/build_deb.sh --target $TARGET
            cd ../..
            cp build/scylla-enterprise-tools-java/build/debs/scylla-tools_`cat build/scylla-enterprise-tools-java/build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/'`-`cat build/scylla-enterprise-tools-java/build/SCYLLA-RELEASE-FILE`-0ubuntu1~${TARGET}_all.deb dist/ami/files/scylla-enterprise-tools_all.deb
        fi
    fi
fi

cd dist/ami

if [ ! -f variables.json ]; then
    echo "create variables.json before start building AMI"
    echo "see wiki page: https://github.com/scylladb/scylla/wiki/Building-CentOS-AMI"
    exit 1
fi

if [ ! -d packer ]; then
    EXPECTED="ed697ace39f8bb7bf6ccd78e21b2075f53c0f23cdfb5276c380a053a7b906853  packer_1.0.0_linux_amd64.zip"
    wget -nv https://releases.hashicorp.com/packer/1.0.0/packer_1.0.0_linux_amd64.zip -O packer_1.0.0_linux_amd64.zip
    CSUM=`sha256sum packer_1.0.0_linux_amd64.zip`
    if [ "$CSUM" != "$EXPECTED" ]; then
        echo "Error while downloading packer. Checksum doesn't match! ($CSUM)"
        exit 1
    fi
    mkdir packer
    cd packer
    unzip -x ../packer_1.0.0_linux_amd64.zip
    cd -
fi

packer/packer build -var-file=variables.json -var install_args="$INSTALL_ARGS" -var region="$REGION" -var source_ami="$AMI" -var ssh_username="$SSH_USERNAME" scylla.json
