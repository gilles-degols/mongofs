%define debug_package %{nil}
%global _python_bytecompile_errors_terminate_build 0
%define __python /usr/bin/python3.6

%{!?_release: %define _version 1.4.1 }
%{!?_release: %define _release 0 }

# Following line is needed if you compile
%{!?_src: %define _src %{_version}-%{_release} }

Name:   mongofs
Version:    %{_version}
Release:    %{_release}
Summary:    Mount MongoDB as local storage
Group:  Gilles Degols
License:    MIT
Requires:   fuse >= 2.9.2-10
Requires:   fuse-libs >= 2.9.2-10
Requires:   python34 >= 3.4.9-1.el7
Requires(pre): shadow-utils >= 4.1.5.1-24
BuildRequires:  dos2unix >= 6.0.3-7
BuildRequires:  python34 >= 3.4.9-1.el7, python34-pip >= 8.1.2-6.el7
BuildRequires:  python-virtualenv
Source:    %{_src}.tar.gz
BuildRoot:  %(mktemp -ud %{_tmppath}/%{name}-%{version}-%{release}-XXXXXX)

BuildArch:	noarch
AutoReqProv: 	no

%description
Mount MongoDB as local storage

%prep
%setup -q

%build

%pre
getent group mongofs >/dev/null || groupadd -r mongofs
getent passwd mongofs >/dev/null || useradd -r -g mongofs -d / -s /sbin/nologin -c "MongoFS User" mongofs

%install
rm -rf ${RPM_BUILD_ROOT}
install -d -m 0755 ${RPM_BUILD_ROOT}/usr/lib/mongofs
install -d -m 0755 ${RPM_BUILD_ROOT}/usr/bin
install -d -m 0755 ${RPM_BUILD_ROOT}/usr/sbin
cp -r src/ ${RPM_BUILD_ROOT}/usr/lib/mongofs

# Security to avoid creating an rpm with invalid end-of-line
find ${RPM_BUILD_ROOT}/usr/lib/mongofs/src -type f -print0 | xargs -0 dos2unix

install -D -m 0644 conf/mongofs.json ${RPM_BUILD_ROOT}/etc/mongofs/mongofs.json
install -D -m 0755 run ${RPM_BUILD_ROOT}/usr/lib/mongofs/run
/usr/bin/ln -s /usr/lib/mongofs/run ${RPM_BUILD_ROOT}/usr/bin/mongofs-mount
/usr/bin/ln -s /usr/lib/mongofs/run ${RPM_BUILD_ROOT}/usr/sbin/mount.mongofs

%define VPATH ${RPM_BUILD_ROOT}/usr/lib/mongofs/environment
%define REQUIREMENTS_PATH requirements.txt

# If you upgrade from pip 8.1.2 to 19.0.3, you will get a nice bug below, which only happens in a spec file for whatever
# reason. So we assume that we have the virtualenv installed (should always be the case)
# /usr/bin/python3.6 -m pip install virtualenv

/usr/bin/python3.6 -m virtualenv --python=python3.6 %{VPATH}/virtualenv
/usr/bin/python3.6 -m virtualenv --python=python3.6 --relocatable --distribute %{VPATH}/virtualenv

source %{VPATH}/virtualenv/bin/activate
%{VPATH}/virtualenv/bin/pip3.6 install --upgrade pip
%{VPATH}/virtualenv/bin/pip3.6 install setuptools
%{VPATH}/virtualenv/bin/pip3.6 install -r %{REQUIREMENTS_PATH}
deactivate

rm -rf %{VPATH}/virtualenv/local

find %{VPATH}/virtualenv/ -name __pycache__ -type d -prune -exec rm -rf {} +
sed -i "s|%{VPATH}/virtualenv/bin/python|/../bin/python|g" %{VPATH}/virtualenv/bin/*;
find %{VPATH}/virtualenv/lib/python3.6/site-packages/ -type f -exec sed -i "s|%{VPATH}/virtualenv/|/../|g" {} \;

find %{VPATH}/virtualenv -name '*.py[co]' -delete
sed -i "s|%{VPATH}|../..|g" %{VPATH}/virtualenv/bin/*

%clean
rm -rf ${RPM_BUILD_ROOT}

%files
%defattr(-,root,root)
/usr/lib/mongofs
/usr/bin/mongofs-mount
/usr/sbin/mount.mongofs
%config /etc/mongofs/mongofs.json

%changelog
* Fri Apr 26 2019 Gilles Degols - 1.4.1-0
- Don't forget to force S_IFDIR on root

* Wed Apr 24 2019 Gilles Degols - 1.4.0-0
- Add default root mode option
- Fix python dependencies

* Tue Apr 09 2019 Gilles Degols - 1.3.0-0
- Fix working directory of run script
- Allow mongofs to be mounted as any other file system

* Mon Apr 08 2019 Gilles Degols - 1.2.3-0
- Fix the lock system of files between nodes

* Sat Jan 19 2019 Gilles Degols - 1.2.2-0
- Proper RPM packaging and fix unit tests

* Tue Dec 11 2018 Gilles Degols - 1.2.1-0
- Better handle root users and groups with the same name

* Mon Dec 3 2018 Gilles Degols - 1.2.0-0
- Better handling of access rights and remove obsolete entries in the cache after file deletion

* Sun Jun 10 2018 Gilles Degols - 1.1.0-0
- Fix handling of big files
- Increase write/read speed by a factor 10

* Fri Jun 01 2018 Gilles Degols - 1.0.0-0
- First release with almost every functionality from FUSE / fusepy
