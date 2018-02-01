### tap-oracle

Tap for Oracle


you must enable supplmental logging before turning on logminer
SQL> begin
  2  rdsadmin.rdsadmin_util.alter_supplemental_logging(
  3    p_action => 'ADD');
  4  end;
  5  /


begin
    rdsadmin.rdsadmin_util.set_configuration(
        name  => 'archivelog retention hours',
        value => '24');
end;

QL> exec rdsadmin.rdsadmin_util.show_configuration;
NAME:tracefile retention
VALUE:10080
DESCRIPTION:tracefile expiration specifies the duration in minutes before
tracefiles in bdump are automatically deleted.
NAME:archivelog retention hours
VALUE:24
DESCRIPTION:ArchiveLog expiration specifies the duration in hours before
archive/redo log files are automatically deleted.
/