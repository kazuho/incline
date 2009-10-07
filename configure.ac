AC_INIT(src/incline_def.cc)
AC_CONFIG_AUX_DIR(config)
AM_CONFIG_HEADER(src/incline_config.h)
AM_INIT_AUTOMAKE("incline", 0.05)

AC_PROG_CC
AM_PROG_CC_C_O
AC_PROG_CXX
AC_PROG_CPP
AC_PROG_GCC_TRADITIONAL
AC_PROG_LIBTOOL
LIBTOOL="$LIBTOOL --preserve-dup-deps"
AC_SUBST(LIBTOOL)

AC_ARG_WITH(mysql-config,
	[  --with-mysql-config=mysql-config
                          path to mysql_config [[automatic]]],
	[MYSQL_CONFIG="$withval"])
if test "$MYSQL_CONFIG" = "yes" -o -z "$MYSQL_CONFIG"
then
	MYSQL_CONFIG_PATH=`which mysql_config` 2> /dev/null
	if test "$MYSQL_CONFIG_PATH"
	then
		MYSQL_CONFIG="$MYSQL_CONFIG_PATH"
	else
		if "$MYSQL_CONFIG" = "yes"
		then
			AC_MSG_ERROR([could not find mysql_config])
		fi
		MYSQL_CONFIG="no"
	fi
fi
if test "$MYSQL_CONFIG" != "no"
then
	MYSQL_CFLAGS=`$MYSQL_CONFIG --include`
	CPPFLAGS="$CPPFLAGS $MYSQL_CFLAGS"
	MYSQL_LDFLAGS=`$MYSQL_CONFIG --libs_r`
	LDFLAGS="$MYSQL_LDFLAGS $LDFLAGS"
	WITH_MYSQL=1
fi
if test "$WITH_MYSQL"
then
	AC_DEFINE([WITH_MYSQL],1,[create mysql driver])
fi
AM_CONDITIONAL(WITH_MYSQL, test "$WITH_MYSQL")

AC_ARG_WITH(pgsql,
	[  --with-pgsql=dir        postgresql directory [[automatic]]],
	[PGSQL_DIR="$withval"])
if test "$PGSQL_DIR" = "yes" -o -z "$PGSQL_DIR"
then
	AC_CHECK_HEADERS([libpq-fe.h], [FOUND_PGSQL=1])
	if test "$FOUND_PGSQL"
	then
		LDFLAGS="$LDFLAGS -lpq"
		WITH_PGSQL=1
		PGSQL_DIR="no"
	elif test -d /usr/local/pgsql
	then
		PGSQL_DIR=/usr/local/pgsql
	elif test "$PGSQL_DIR" = "yes"
	then
		AC_MSG_ERROR([could not find pgsql])
	else
		PGSQL_DIR="no"
	fi
fi
if test "$PGSQL_DIR" != "no"
then
	if test ! -d "$PGSQL_DIR"
	then
		AC_MSG_ERROR([could not find pgsql at given location])
	fi
	CPPFLAGS="$CPPFLAGS -I$PGSQL_DIR/include"
	LDFLAGS="$LDFLAGS -L$PGSQL_DIR/lib -lpq"
	WITH_PGSQL=1
fi
if test "$WITH_PGSQL"
then
	AC_DEFINE([WITH_PGSQL],1,[create pgsql driver])
fi
AM_CONDITIONAL(WITH_PGSQL, test "$WITH_PGSQL")

if test "$WITH_MYSQL" = "" -a "$WITH_PGSQL" = ""
then
	AC_MSG_ERROR([no datatabase driver found, please set them manually using --with-XXX])
fi

CFLAGS="$CFLAGS -Wall -g -O"
CXXFLAGS="$CXXFLAGS -Wall -g -O"

AC_OUTPUT(Makefile src/Makefile)

cp Makefile Makefile.orig && perl META.yml.pl --fix-makefile < Makefile.orig > Makefile
if test "$?" -ne "0"
then
	AC_MSG_ERROR([failed to add metainfo to Makefile])
fi
