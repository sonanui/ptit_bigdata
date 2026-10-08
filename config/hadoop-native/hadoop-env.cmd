@echo off
@rem Moi truong cho Hadoop native tren Windows. JAVA_HOME do scripts\native\hadoop-env.ps1 dat (JDK 21).
@rem File .cmd phai la ASCII va xuong dong CRLF (cmd.exe). @NATIVE_ROOT_WIN@ duoc setup-native.ps1 thay bang thu muc that.
set HADOOP_LOG_DIR=@NATIVE_ROOT_WIN@\logs
set HADOOP_PID_DIR=@NATIVE_ROOT_WIN@\pid
set HADOOP_IDENT_STRING=%USERNAME%
set HADOOP_CLIENT_OPTS=-Xmx1g %HADOOP_CLIENT_OPTS%
set HADOOP_NAMENODE_OPTS=-Xmx512m %HADOOP_NAMENODE_OPTS%
set HADOOP_DATANODE_OPTS=-Xmx512m %HADOOP_DATANODE_OPTS%
