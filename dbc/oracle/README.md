### Guide

You need to prepare something before actually can use this library.

First go to oracle instant client download page [http://www.oracle.com/technetwork/database/features/instant-client/index-097480.html][1], click the link which name is operating system you used.

Then download 3 files from opened link.

* instant client basic
* instant client sqlplus
* instant client sdk

After that follow tutorial below.

---

### Guide for OSX

Create new folder `/Application/oracle`, put these three files on it, and extract it as one folder.
Then **basic**, **sqlplus**, and **sdk**.

Then run this commands.bbbb

```bash
cd /Application/oracle
unzip instantclient-basic-macos.x64-11.2.0.4.0.zip
unzip instantclient-sqlplus-macos.x64-11.2.0.4.0.zip
unzip instantclient-sdk-macos.x64-11.2.0.4.0.zip
```

Go to the extracted folder then create symbolic link for some libs.

```bash
cd instantclient_11_2
ln -s libclntsh.dylib.11.1 libclntsh.dylib
ln -s libocci.dylib.11.1 libocci.dylib
```

Export the path as variable `ORACLE_HOME` and on `$PATH`. You can put it on `~/.profile`.

```bash
export ORACLE_HOME=/Applications/oracle/instantclient_11_2
export PATH=$PATH:$ORACLE_HOME
```

Now create new file called `tnsnames.ora`, put it on  `$ORACLE_HOME/network/admin`. Create the folder first, if it does not exist. Fill the file with your tns configuration.

```bash
MY=(DESCRIPTION=
        (ADDRESS_LIST=
            (ADDRESS=
                (PROTOCOL=TCP)
                (HOST=192.168.0.1)
                (PORT=1521)
            )
        )
        (CONNECT_DATA=
            (SID=ORCL)
            (SERVER=DEDICATED)
            (SERVICE_NAME=orcl.my.local)
        )
    )
```

Try to run sqlplus command, it should work.

```bash
source ~/.profile
sqlplus scott/tiger@MY
```

Now we need to prepare some things to make *go-oci8* worked.

Set variable `DYLD_LIBRARY_PATH` with value is `$ORACLE_HOME`. This variable required by *go-oci8*.

```bash
export DYLD_LIBRARY_PATH=$ORACLE_HOME:$DYLD_LIBRARY_PATH
```

Create new file `oci8.pc`, place it at `$ORACLE_HOME/pkg`. Create the folder if it does not exists. Then fill the file with these codes.

```bash
orainc=/Applications/oracle/instantclient_11_2/sdk/include
oralib=/Applications/oracle/instantclient_11_2

Name: oci8
Description: Oracle Instant Client
Version: 12.1
Cflags: -I${orainc}
Libs: -L${oralib} -lclntsh
```

The `orainc` should refer to `$ORACLE_HOME/sdk/include`, and the `oralib` should refer to `$ORACLE_HOME` itself.

After that, export the `pkg` folder.

```bash
export PKG_CONFIG_PATH=$ORACLE_HOME/pkg
```

Now try it sample code of *go-oci8*.

```bash
source ~/.profile
go get github.com/mattn/go-oci8
cd github.com/mattn/go-oci8/_example
go run oracle.go
```


  [1]: http://www.oracle.com/technetwork/database/features/instant-client/index-097480.html