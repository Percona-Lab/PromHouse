%define debug_package %{nil}

Name:    %{_name}
Version: %{_version}
Release: %{_release}.el7
Summary: %{name}
License: MIT
URL:     https://liqiang.io/docs/rpms/promhouse

Source0: %{name}-%{version}.linux-%{_arch}
Source1: %{name}.service
Source2: %{name}.sysconfig


%description
%{name}


%clean
rm -rf %{buildroot}%{_sysconfdir}/%{name}

%install
install -d -m 755 %{buildroot}%{_bindir}
install -c -m 755 %{name} %{buildroot}%{_bindir}/%{name}

install -d -m 755 %{buildroot}%{_unitdir}
install -c -m 644 %{SOURCE1} %{buildroot}%{_unitdir}/%{name}.service

install -d -m 755 %{buildroot}%{_sysconfdir}/sysconfig
install -c -m 644 %{SOURCE2} %{buildroot}%{_sysconfdir}/sysconfig/%{name}.env

%preun
if [ $1 -eq 0 ]; then
  # uninstall
  /bin/systemctl disable %{name}.service
  /bin/systemctl stop %{name}.service
fi

%files
%defattr(-,root,root,-)
%{_bindir}/%{name}
%{_unitdir}/%{name}.service
%config(noreplace) %{_sysconfdir}/sysconfig/%{name}.env

%changelog
