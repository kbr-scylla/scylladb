#!/bin/bash -e

if [ ! -e dist/ami/build_ami.sh ]; then
    echo "run build_ami.sh in top of scylla dir"
    exit 1
fi

print_usage() {
    echo "build_ami.sh --localrpm --repo [URL]"
    echo "  --localrpm  deploy locally built rpms"
    echo "  --repo  repository for both install and update, specify .repo/.list file URL"
    echo "  --repo-for-install  repository for install, specify .repo/.list file URL"
    echo "  --repo-for-update  repository for update, specify .repo/.list file URL"
    exit 1
}
LOCALRPM=0
REPO=
REPO_FOR_INSTALL=
REPO_FOR_UPDATE=
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
        *)
            print_usage
            ;;
    esac
done

if [ $LOCALRPM ]; then
    INSTALL_ARGS="$INSTALL_ARGS --localrpm"
    if [ -z "$REPO" ] && [ -z "$REPO_FOR_INSTALL" ] && [ -z "$REPO_FOR_UPDATE" ]; then
        REPO=`./scripts/scylla_current_repo`
    fi
fi

if [ -n "$REPO" ]; then
    INSTALL_ARGS="$INSTALL_ARGS --repo $REPO"
fi

if [ -n "$REPO_FOR_INSTALL" ]; then
    INSTALL_ARGS="$INSTALL_ARGS --repo-for-install $REPO_FOR_INSTALL"
fi

if [ -n "$REPO_FOR_UPDATE" ]; then
    INSTALL_ARGS="$INSTALL_ARGS --repo-for-update $REPO_FOR_UPDATE"
fi

. /etc/os-release
case "$ID" in
    "centos")
        AMI=ami-46bf8a51
        REGION=us-east-1
        SSH_USERNAME=centos
        ;;
    "ubuntu")
        AMI=ami-ff427095
        REGION=us-east-1
        SSH_USERNAME=ubuntu
        ;;
    *)
        echo "build_ami.sh does not supported this distribution."
        exit 1
        ;;
esac


if [ $LOCALRPM -eq 1 ]; then
    if [ "$ID" = "centos" ]; then
        sudo rm -rf build/*
        sudo yum -y install git
        if [ ! -f dist/ami/files/scylla-enterprise.x86_64.rpm ] || [ ! -f dist/ami/files/scylla-enterprise-kernel-conf.x86_64.rpm ] || [ ! -f dist/ami/files/scylla-enterprise-conf.x86_64.rpm ] || [ ! -f dist/ami/files/scylla-enterprise-server.x86_64.rpm ] || [ ! -f dist/ami/files/scylla-enterprise-debuginfo.x86_64.rpm ]; then
            dist/redhat/build_rpm.sh
            cp build/rpms/scylla-enterprise-`cat build/SCYLLA-VERSION-FILE`-`cat build/SCYLLA-RELEASE-FILE`.*.x86_64.rpm dist/ami/files/scylla-enterprise.x86_64.rpm
            cp build/rpms/scylla-enterprise-kernel-conf-`cat build/SCYLLA-VERSION-FILE`-`cat build/SCYLLA-RELEASE-FILE`.*.x86_64.rpm dist/ami/files/scylla-enterprise-kernel-conf.x86_64.rpm
            cp build/rpms/scylla-enterprise-conf-`cat build/SCYLLA-VERSION-FILE`-`cat build/SCYLLA-RELEASE-FILE`.*.x86_64.rpm dist/ami/files/scylla-enterprise-conf.x86_64.rpm
            cp build/rpms/scylla-enterprise-server-`cat build/SCYLLA-VERSION-FILE`-`cat build/SCYLLA-RELEASE-FILE`.*.x86_64.rpm dist/ami/files/scylla-enterprise-server.x86_64.rpm
            cp build/rpms/scylla-enterprise-debuginfo-`cat build/SCYLLA-VERSION-FILE`-`cat build/SCYLLA-RELEASE-FILE`.*.x86_64.rpm dist/ami/files/scylla-enterprise-debuginfo.x86_64.rpm
        fi
        if [ ! -f dist/ami/files/scylla-enterprise-jmx.noarch.rpm ]; then
            cd build
            git clone --depth 1 git@github.com:scylladb/scylla-enterprise-jmx.git
            cd scylla-enterprise-jmx
            sh -x -e dist/redhat/build_rpm.sh $*
            cd ../..
            cp build/scylla-enterprise-jmx/build/rpms/scylla-enterprise-jmx-`cat build/scylla-enterprise-jmx/build/SCYLLA-VERSION-FILE`-`cat build/scylla-enterprise-jmx/build/SCYLLA-RELEASE-FILE`.*.noarch.rpm dist/ami/files/scylla-enterprise-jmx.noarch.rpm
        fi
        if [ ! -f dist/ami/files/scylla-enterprise-tools.noarch.rpm ] || [ ! -f dist/ami/files/scylla-enterprise-tools-core.noarch.rpm ]; then
            cd build
            git clone --depth 1 git@github.com:scylladb/scylla-enterprise-tools-java.git
            cd scylla-enterprise-tools-java
            sh -x -e dist/redhat/build_rpm.sh
            cd ../..
            cp build/scylla-enterprise-tools-java/build/rpms/scylla-enterprise-tools-`cat build/scylla-enterprise-tools-java/build/SCYLLA-VERSION-FILE`-`cat build/scylla-enterprise-tools-java/build/SCYLLA-RELEASE-FILE`.*.noarch.rpm dist/ami/files/scylla-enterprise-tools.noarch.rpm
            cp build/scylla-enterprise-tools-java/build/rpms/scylla-enterprise-tools-core-`cat build/scylla-enterprise-tools-java/build/SCYLLA-VERSION-FILE`-`cat build/scylla-enterprise-tools-java/build/SCYLLA-RELEASE-FILE`.*.noarch.rpm dist/ami/files/scylla-enterprise-tools-core.noarch.rpm
        fi
    else
        sudo apt-get install -y git
        if [ ! -f dist/ami/files/scylla-enterprise-server_amd64.deb ]; then
            if [ ! -f ../scylla-enterprise-server_`cat build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/'`-`cat build/SCYLLA-RELEASE-FILE`-ubuntu1_amd64.deb ]; then
                echo "Build .deb before running build_ami.sh"
                exit 1
            fi
            cp ../scylla-enterprise_`cat build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/'`-`cat build/SCYLLA-RELEASE-FILE`-ubuntu1_amd64.deb dist/ami/files/scylla-enterprise_amd64.deb
            cp ../scylla-enterprise-kernel-conf_`cat build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/'`-`cat build/SCYLLA-RELEASE-FILE`-ubuntu1_amd64.deb dist/ami/files/scylla-enterprise-kernel-conf_amd64.deb
            cp ../scylla-enterprise-conf_`cat build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/'`-`cat build/SCYLLA-RELEASE-FILE`-ubuntu1_amd64.deb dist/ami/files/scylla-enterprise-conf_amd64.deb
            cp ../scylla-enterprise-server_`cat build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/'`-`cat build/SCYLLA-RELEASE-FILE`-ubuntu1_amd64.deb dist/ami/files/scylla-enterprise-server_amd64.deb
            cp ../scylla-enterprise-server-dbg_`cat build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/'`-`cat build/SCYLLA-RELEASE-FILE`-ubuntu1_amd64.deb dist/ami/files/scylla-enterprise-server-dbg_amd64.deb
        fi
        if [ ! -f dist/ami/files/scylla-enterprise-jmx_all.deb ]; then
            cd build
            git clone --depth 1 git@github.com:scylladb/scylla-enterprise-jmx.git
            cd scylla-enterprise-jmx
            sh -x -e dist/debian/build_deb.sh $*
            cd ../..
            cp build/scylla-enterprise-jmx_`cat build/scylla-enterprise-jmx/build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/'`-`cat build/scylla-enterprise-jmx/build/SCYLLA-RELEASE-FILE`-ubuntu1_all.deb dist/ami/files/scylla-enterprise-jmx_all.deb
        fi
        if [ ! -f dist/ami/files/scylla-enterprise-tools_all.deb ]; then
            cd build
            git clone --depth 1 git@github.com:scylladb/scylla-enterprise-tools-java.git
            cd scylla-enterprise-tools-java
            sh -x -e dist/debian/build_deb.sh $*
            cd ../..
            cp build/scylla-enterprise-tools_`cat build/scylla-enterprise-tools-java/build/SCYLLA-VERSION-FILE | sed 's/\.rc/~rc/'`-`cat build/scylla-enterprise-tools-java/build/SCYLLA-RELEASE-FILE`-ubuntu0_all.deb dist/ami/files/scylla-enterprise-tools_all.deb
        fi
    fi
fi

cd dist/ami

if [ ! -f variables.json ]; then
    echo "create variables.json before start building AMI"
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
