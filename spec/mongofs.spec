%define debug_package %{nil}
%global _python_bytecompile_errors_terminate_build 0

Name:   mongofs
Version:    1.2.0
Release:    0
Summary:    Mount MongoDB as local storage
Group:  Gilles Degols
License:    MIT
Requires:   fuse >= 2.9.2-10
Requires:   fuse-libs >= 2.9.2-10
Requires(pre): shadow-utils >= 4.1.5.1-24
BuildRequires:  dos2unix >= 6.0.3-7
BuildRequires:  python34 >= 3.4.9-1.el7, python34-pip >= 8.12-6.el7
Source0:    mongofs-1.2.0.tar.gz
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
install -d -m 0755 $RPM_BUILD_ROOT/usr/lib/mongofs
install -d -m 0755 $RPM_BUILD_ROOT/usr/bin
cp -r src/ $RPM_BUILD_ROOT/usr/lib/mongofs

# Security to avoid creating an rpm with invalid end-of-line
find $RPM_BUILD_ROOT/usr/lib/mongofs/src -type f -print0 | xargs -0 dos2unix

install -D -m 0644 conf/mongofs.json $RPM_BUILD_ROOT/etc/mongofs/mongofs.json
install -D -m 0644 run $RPM_BUILD_ROOT/usr/lib/mongofs/run
/usr/bin/ln -s /usr/lib/mongofs/run $RPM_BUILD_ROOT/usr/bin/mongofs-mount
chmod +x $RPM_BUILD_ROOT/usr/lib/mongofs/run

/usr/bin/python3 -m venv $RPM_BUILD_ROOT/usr/lib/mongofs/environment
source $RPM_BUILD_ROOT/usr/lib/mongofs/environment/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install -r requirements.txt
deactivate

%clean
rm -rf $RPM_BUILD_ROOT

%files
%defattr(-,root,root)
/usr/lib/mongofs
/usr/bin/mongofs-mount
%config /etc/mongofs/mongofs.json

%changelog
* Mon Dec 3 2018 Gilles Degols - 1.2.0-0
- Better handling of access rights and remove obsolete entries in the cache after file deletion

* Sun Jun 10 2018 Gilles Degols - 1.1.0-0
- Fix handling of big files
- Increase write/read speed by a factor 10

* Fri Jun 01 2018 Gilles Degols - 1.0.0-0
- First release with almost every functionality from FUSE / fusepy
